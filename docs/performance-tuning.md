# BigQuery Remote Functions on Cloud Run — measured behaviour

Everything below was measured on real infrastructure (project `<PROJECT_ID>`,
`us-central1`, August 2026) using [`fpe/scripts/sweep.py`](../fpe/scripts/sweep.py).
Raw records are JSONL under `fpe/scripts/sweep_raw_*.jsonl`; regenerate any
table with `python fpe/scripts/analyze.py <file>`.

**Workload.** FF3-1 format-preserving encryption in pure Python — ~77 µs/row on
one core, i.e. genuinely CPU-bound. That is deliberate: a cheap workload would
hide every effect below in network noise.

**Two measurement sources.** BigQuery job statistics give what the user
experiences. The service's own structured logs give what actually arrived —
batch sizes, worker processes, in-flight concurrency. Most of the surprising
results below are only visible from the second.

---

## Headline findings

1. **`max_batching_rows` is capped by a ~256 KiB request-body budget**, not by
   a row count. Narrow SSN rows cap at 11,905/request; wider email rows at
   6,025. Ask for 1,000,000 and you get whatever fits in 256 KiB. Cloud Run
   sizing has no effect on it whatsoever.
2. **Short-circuit evaluation costs ~180x.** A remote function inside `CASE`/`IF`
   drops to one HTTP request *per row*. This is exactly the shape entitlement-driven
   authorized views naturally take.
3. **The obvious fix for that doesn't work.** Hoisting the call into a subquery
   is inlined straight back by the optimizer. Measured identical to the bug.
4. **The 15 MB response ceiling is real and precise** — 14.3 MB fine, 17.9 MB fails.
5. **We could not reproduce the documented 10-concurrent-query limit** as a
   failure; 16 concurrent long-running queries all succeeded.
6. **Threads are worse than useless for CPU-bound Python here.** At identical
   `containerConcurrency`, 8 gthread threads in one process ran at 308 µs/row
   against 82 µs/row for 4 sync processes — the GIL serialises the work and the
   threads add contention.
7. **~94% of end-to-end time is compute, not network.** The transit floor is
   403,000 rows/s; FF3-1 runs at 26,000.

---

## 1. Batching: `max_batching_rows` is a request, not an instruction

`max_batching_rows` is documented as a cap BigQuery *may* use, with no stated
default. The service logs the real batch sizes, so this reads the answer off
the wire.

| `max_batching_rows` | Actual median rows/request | HTTP requests (400k rows) | Elapsed |
| --- | --- | --- | --- |
| 10,000 | 10,000 | 40 | 13.03s |
| 50,000 | **11,905** | 32 | 9.90s |
| 100,000 | **11,905** | 32 | 7.13s |
| 250,000 | **11,905** | 34 | 6.42s |
| 500,000 | **11,905** | 34 | 6.87s |
| 1,000,000 | **11,905** | 31 | 8.21s |
| *(omitted — BigQuery chooses)* | **11,905** | 34 | 7.09s |

On this column BigQuery caps a request at 11,905 rows regardless of what you
ask for, and its automatic choice is the same number.

### What actually sets the cap

11,905 is not a magic row count. Running `max_batching_rows = 1,000,000`
against four columns of different widths, across three very different Cloud
Run shapes:

| Cloud Run config | `ssn` (11 ch) | `dob` (10 ch) | `name` (~11 ch) | `email` (~30 ch) |
| --- | --- | --- | --- | --- |
| 1 vCPU, concurrency 1, 1 worker | 11,905 | 10,913 | 11,474 | 6,025 |
| 4 vCPU, concurrency 8, 4 workers | 11,905 | 10,913 | 11,474 | 6,025 |
| 8 vCPU, **concurrency 80**, 8 workers | 11,905 | 10,913 | 11,474 | 6,025 |

Two clean results:

- **Cloud Run sizing changes nothing.** Identical caps at 1 vCPU/concurrency 1
  and 8 vCPU/concurrency 80. That is expected once you see the mechanism:
  BigQuery decides the batch when it builds the HTTP request, before it has
  contacted your service, and it has no visibility into `containerConcurrency`
  at all. Raising instance size or concurrency cannot move this number.
- **Row width sets it.** Each row is serialised into the `calls` array as
  `["<value>","<element>"],`. Multiply the cap by those bytes:

  | Column | Cap | Bytes/row on the wire | Cap x bytes |
  | --- | --- | --- | --- |
  | `ssn` | 11,905 | 22.0 | 261,910 |
  | `dob` | 10,913 | 24.0 | 261,912 |
  | `name` | 11,474 | 22.9 | 262,755 |
  | `email` | 6,025 | 43.5 | 262,088 |

  Constant at ~262,000 bytes. **256 KiB is 262,144**, leaving ~232 bytes of
  JSON envelope (`requestId`, `caller`, `sessionUser`, `userDefinedContext`).
  `dob` matches exactly because it is fixed-width; the variable-width columns
  scatter by a few hundred bytes, which is just the mean-length estimate.

So the rule is **~256 KiB of request body**, and you can predict your own cap:

```
rows_per_request ≈ 261,900 / (len(value) + len(data_element) + 6)
```

Note this is observed behaviour, not documented contract — but it held across
every configuration tested.

### Not the 5 MB limit

The documented "maximum input size 5 MB" is a *per-row* limit — "the maximum
total size of all input arguments from a **single row**". It bounds how large
one row's arguments may be, not how many rows fit in a request. At ~22 bytes
per row we are six orders of magnitude away from it, and dividing 5 MB by the
observed cap yields 420–830 bytes/row, which matches nothing on the wire.

### Consequences

- Benchmarks comparing `b50000` against `b100000` are comparing identical
  configurations. This retroactively explains the Protegrity results in
  [`protegrity/README.md`](../protegrity/README.md), where batch sizes from
  10,000 to 100,000 all produced ~50,000 rows/s.
- **The cap is per HTTP request, not per query or per project.** Each query
  slices its own rows independently, so N concurrent queries each get full-size
  batches. Nothing is shared across queries.
- Wide columns cost you batching efficiency automatically. Passing a long
  `data_element` name, or extra arguments, directly shrinks your batch.
- The cap keeps you clear of the 15 MB *response* limit for ordinary row
  widths — you have to inflate replies deliberately to breach it (see §2).

---

## 2. Documented limits, probed until they broke

Source: [BigQuery quotas — remote functions](https://docs.cloud.google.com/bigquery/quotas#remote_function_limits).

| Limit | Documented | Measured |
| --- | --- | --- |
| HTTP response size (Cloud Run / gen2) | 15 MB | **Confirmed, precisely** |
| Max HTTP invocation retry attempts | 20 | **Confirmed** (~99 invocations over 5 partitions) |
| Concurrent queries with remote functions | 10 / project | **Not reproduced** |
| Max input size (all args, one row) | 5 MB | Not reachable with realistic rows |
| HTTP invocation time limit | 20 min | Not reached (our timeout is 900s) |

### Response size ceiling

Row count can't get you there — the 11,905 cap means ~11.9 MB at 1 KB/row. So
this widens each *reply* instead, holding rows at the cap:

| Reply width | Est. response | Result |
| --- | --- | --- |
| 1,000 B | 11.9 MB | OK |
| 1,200 B | 14.3 MB | OK |
| 1,500 B | 17.9 MB | **FAILED** |
| 2,000 B | 23.8 MB | **FAILED** |
| 3,000 B | 35.7 MB | **FAILED** (surfaced as a 500 from the container) |

The ceiling sits exactly where documented. Note the failure mode at 3,000 B
differs — the container itself fell over before BigQuery rejected the body, so
you cannot rely on a clean error here.

### Retry behaviour

| Endpoint returns | Query result | Elapsed | Endpoint invocations |
| --- | --- | --- | --- |
| always 503 | FAILED | 115.4s | **99** |
| 50% 503 | FAILED | 113.5s | 43 |
| always 400 | FAILED | **1.1s** | 5 |

503 is retried hard — 99 invocations for what should be 5 partitions, and two
minutes burned before giving up. 400 fails immediately. **Return 400 for
deterministic errors**; the default Flask behaviour of surfacing uncaught
exceptions as 500 will cost you 20 retries per partition. The FPE service does
this deliberately ([`main.py`](../fpe/service/main.py)).

Note the 50%-failure case also failed. Retries did not rescue it, because a
deterministic per-batch failure re-fails on every retry — retry helps with
transient faults, not with an endpoint that reliably rejects certain data.

### The 10-concurrent-query limit — not reproduced

Firing N simultaneous queries, each ~18s over 1M rows:

| Concurrent queries | Succeeded | Wall | Aggregate rows/s |
| --- | --- | --- | --- |
| 1 | 1/1 | 18.4s | 54,202 |
| 2 | 2/2 | 25.9s | 77,352 |
| 4 | 4/4 | 55.3s | 72,352 |
| 8 | 8/8 | 102.1s | 78,366 |
| 10 | 10/10 | 128.1s | 78,058 |
| 12 | **12/12** | 129.2s | 92,882 |
| 16 | **16/16** | 164.4s | 97,326 |

No `Exceeded rate limits: too many concurrent queries with remote functions`
error at any point. Possible explanations we did not distinguish: this project
may already carry a raised quota; the limit may be enforced by queueing rather
than rejection; or it may apply under conditions we didn't reproduce. **Treat
10 as the documented contract and design against it** — the absence of an error
here is not a guarantee.

What we *did* hit is a throughput ceiling around **78,000–97,000 rows/s**
aggregate, imposed by the Cloud Run service (4 vCPU, maxScale 4), not by
BigQuery. Beyond 8 concurrent queries, wall time grows roughly linearly while
aggregate throughput is flat: the queries are queueing on the backend.

**The limit is on concurrency, not rate.** Ten concurrent queries at 0.3s each
is ~33 queries/s; ten at 30s each is ~0.33/s. Shortening queries buys far more
headroom than the number "10" suggests.

---

## 3. The batching cliff

BigQuery disables batching when a remote function sits inside a short-circuiting
expression. The documented wording is: *"If evaluation is short-circuited (e.g.
conditional expressions, `MERGE ... WHEN [NOT] MATCHED`), batching is disabled
and the `calls` field has exactly one element."*

### What "`calls` has exactly one element" means

`calls` is the JSON array in the request body BigQuery POSTs to your service.
**Each element of that array is one row's arguments**, so the length of `calls`
*is* the batch size:

```jsonc
{
  "requestId": "124ab1c",
  "userDefinedContext": { "mode": "fpe_decrypt" },
  "calls": [                        // 3 elements = 3 rows in this one request
    ["123-45-6789", "ssn"],
    ["987-65-4321", "ssn"],
    ["555-11-2222", "ssn"]
  ]
}
```

Your service returns a `replies` array of the same length and order. This is
what `max_batching_rows` caps and what the ~256 KiB budget in §1 constrains —
how many elements BigQuery is willing to put in `calls`.

"Exactly one element" therefore means every request degenerates to:

```jsonc
{ "calls": [ ["123-45-6789", "ssn"] ] }
```

One row per request — **one full HTTP round trip per row**, each paying TCP/TLS,
HTTP framing, JSON parsing and the ~232-byte envelope in order to transform a
single 11-character string.

BigQuery does this to keep the semantics of `CASE`/`IF` honest: those constructs
promise the function is never evaluated for a row failing the condition, and
BigQuery preserves that by walking the conditional row by row rather than
gathering qualifying rows and batching them. Correctness is preserved; batching
is the casualty.

The `Rows/request` column below is the measured median length of `calls`: the
service reads it at [`main.py:206`](../fpe/service/main.py#L206) and logs
`len(calls)` per request at [`main.py:244`](../fpe/service/main.py#L244). These
are counted, not inferred from timings.

### The four shapes

All four do the same conceptual work: decrypt `ssn` for the half of the rows
where `MOD(id, 2) = 0`. They differ only in *where the conditional sits*
relative to the function call. Source:
[`sweep.py:365`](../fpe/scripts/sweep.py#L365).

**1. `plain` — unconditional baseline** ([`sweep.py:380`](../fpe/scripts/sweep.py#L380))

No conditional at all. Establishes what full batching looks like, and is the
control the other three are measured against.

```sql
SELECT SUM(LENGTH(fpe_decrypt_b5000(ssn, 'ssn')))
FROM (SELECT id, ssn FROM pii_tokenized LIMIT 20000)
```

**2. `inside_case` — the anti-pattern** ([`sweep.py:382`](../fpe/scripts/sweep.py#L382))

The function is an arm of a `CASE`. This is the shape an entitlement-driven
authorized view naturally takes, and the one to avoid.

```sql
SELECT SUM(LENGTH(
  CASE WHEN MOD(id, 2) = 0 THEN fpe_decrypt_b5000(ssn, 'ssn')
       ELSE ssn END))
FROM (SELECT id, ssn FROM pii_tokenized LIMIT 20000)
```

**3. `hoisted_subquery` — the fix that isn't** ([`sweep.py:390`](../fpe/scripts/sweep.py#L390))

Computes the decryption unconditionally in an inner `SELECT`, then chooses in
the outer one. The reasoning is sound and the result is wrong — see below.

```sql
SELECT SUM(LENGTH(IF(MOD(id, 2) = 0, dec, ssn)))
FROM (SELECT id, ssn, fpe_decrypt_b5000(ssn, 'ssn') AS dec
      FROM (SELECT id, ssn FROM pii_tokenized LIMIT 20000))
```

**4. `filter_then_apply` — the fix that works** ([`sweep.py:397`](../fpe/scripts/sweep.py#L397))

The condition becomes a `WHERE` predicate, so no conditional expression wraps
the call. Note this evaluates *half* the rows, like shape 2 — it is doing the
same amount of cryptographic work, just batched.

```sql
SELECT SUM(LENGTH(fpe_decrypt_b5000(ssn, 'ssn')))
FROM (SELECT id, ssn FROM pii_tokenized LIMIT 20000)
WHERE MOD(id, 2) = 0
```

### Results — 20,000 rows

| Query shape | Elapsed | HTTP requests | Rows/request |
| --- | --- | --- | --- |
| 1. `plain` — unconditional | **1.59s** | 4 | 5,000 |
| 2. `inside_case` — inside `CASE` | 240.32s | 9,963 | **1** |
| 3. `hoisted_subquery` — hoisted into subquery | 256.61s | 9,963 | **1** |
| 4. `filter_then_apply` — filter first | **1.36s** | 2 | 4,982 |

**~180x** between shapes 2 and 4, for identical output.

The 9,963 HTTP requests in shapes 2 and 3 are ~half of 20,000 — confirming that
only the entitled rows were evaluated, one row per request. The cost is not
extra cryptography; it is 9,963 round trips instead of 2.

**Why shape 3 fails.** Hoisting into a subquery *looks* like it forces
unconditional evaluation, and the numbers are identical to the bug it was meant
to fix. BigQuery's optimizer inlines the trivial subquery back into the `IF`,
restoring short-circuit semantics and single-row batches. It is kept in the
benchmark precisely because it is such a plausible trap — if you "fix" a slow
view this way and don't re-measure, you will believe you solved it.

The reliable rule: **no conditional expression may wrap the call.** Push the
condition into a `WHERE` clause or a join predicate (shape 4), or, when you must
return the unentitled rows too, use `UNION ALL` of unconditional branches
(§6, pattern B).

---

## 4. Where you put the call

Three pairs of queries. Within each pair both members return **exactly the same
answer**; the only difference is whether the reducing operation (`LIMIT`,
aggregation, `WHERE`) runs before or after the remote function. Source:
[`sweep.py:782`](../fpe/scripts/sweep.py#L782). 400,000 rows.

Naming: `AFTER_detok` means the reduction happens *after* detokenization, so the
function sees every row — the wasteful ordering. `BEFORE_detok` reduces first.

**Pair 1 — `LIMIT`** ([`sweep.py:795`](../fpe/scripts/sweep.py#L795)). Return 100
decrypted SSNs.

```sql
-- limit_AFTER_detok: decrypt everything, then take 100
SELECT ssn FROM (SELECT fpe_decrypt_b5000(ssn,'ssn') AS ssn
                 FROM (SELECT ssn FROM pii_tokenized LIMIT 400000)) LIMIT 100

-- limit_BEFORE_detok: take 100, then decrypt those
SELECT fpe_decrypt_b5000(ssn,'ssn') AS ssn
FROM (SELECT ssn FROM pii_tokenized LIMIT 100)
```

**Pair 2 — aggregation** ([`sweep.py:806`](../fpe/scripts/sweep.py#L806)). Count
distinct SSNs. Equivalent because tokenization is deterministic and injective:
distinct tokens map one-to-one onto distinct plaintexts.

```sql
-- aggregate_AFTER_detok: decrypt every row, then count distinct
SELECT COUNT(DISTINCT fpe_decrypt_b5000(ssn,'ssn'))
FROM (SELECT ssn FROM pii_tokenized LIMIT 400000)

-- aggregate_BEFORE_detok: count distinct tokens; no decryption needed at all
SELECT COUNT(DISTINCT ssn) FROM (SELECT ssn FROM pii_tokenized LIMIT 400000)
```

**Pair 3 — `WHERE`** ([`sweep.py:814`](../fpe/scripts/sweep.py#L814)). Decrypt
only rows matching a non-sensitive predicate (`MOD(id,1000)=0`, ~400 rows).

```sql
-- filter_AFTER_detok: filter written outside the projection
SELECT SUM(LENGTH(ssn)) FROM (
  SELECT fpe_decrypt_b5000(ssn,'ssn') AS ssn, id
  FROM (SELECT id, ssn FROM pii_tokenized LIMIT 400000)) WHERE MOD(id,1000)=0

-- filter_BEFORE_detok: filter written below the function
SELECT SUM(LENGTH(fpe_decrypt_b5000(ssn,'ssn')))
FROM (SELECT id, ssn FROM pii_tokenized LIMIT 400000) WHERE MOD(id,1000)=0
```

| Pair | Shape | Elapsed | Rows sent to service | HTTP requests |
| --- | --- | --- | --- | --- |
| 1 — `LIMIT` | `limit_AFTER_detok` | 4.63s | 150,000 | 30 |
| 1 — `LIMIT` | `limit_BEFORE_detok` | **0.97s** | **100** | 1 |
| 2 — aggregation | `aggregate_AFTER_detok` | 7.56s | 400,000 | 80 |
| 2 — aggregation | `aggregate_BEFORE_detok` | **0.34s** | **0** | **0** |
| 3 — `WHERE` | `filter_AFTER_detok` | 0.55s | 397 | 1 |
| 3 — `WHERE` | `filter_BEFORE_detok` | 0.57s | 397 | 1 |

Three different behaviours, and the differences matter:

- **Pair 1 — `LIMIT` is only partially pushed.** 150,000 rows crossed the wire
  to return 100. BigQuery stopped early rather than decrypting all 400,000, but
  still did 1,500x more work than needed. Put the `LIMIT` in a subquery below
  the function.
- **Pair 2 — aggregation is not pushed at all.** All 400,000 rows were
  decrypted to produce a single number. And `COUNT(DISTINCT token)` equals
  `COUNT(DISTINCT plaintext)` because tokenization is deterministic and
  injective, so the correct answer needs **zero** decryptions. Analytics over
  tokenized columns (counts, group-bys, joins, distincts) should generally not
  touch the remote function at all.
- **Pair 3 — `WHERE` placement is irrelevant.** Both shapes sent the same 397
  rows in one request: BigQuery pushes simple predicates through the remote
  function by itself. No need to contort the SQL for this case.

---

## 5. Search: tokenize the term, don't detokenize the column

FPE is deterministic, so finding a known plaintext never requires decrypting
the table. All three shapes below find the same row in a 1M-row table. Source:
[`sweep.py:547`](../fpe/scripts/sweep.py#L547).

**1. `detokenize_column` — the anti-pattern** ([`sweep.py:569`](../fpe/scripts/sweep.py#L569))

Decrypt every stored value, compare each against the plaintext you want. The
predicate is opaque to the planner, so it cannot prune anything.

```sql
SELECT COUNT(*) FROM pii_tokenized
WHERE fpe_decrypt_b5000(ssn, 'ssn') = '123-45-6789'
```

**2. `tokenize_term` — the pattern** ([`sweep.py:577`](../fpe/scripts/sweep.py#L577))

Encrypt the one search term, compare against stored ciphertext natively.

```sql
DECLARE tok STRING;
SET tok = (SELECT `proj.ds`.fpe_encrypt('123-45-6789', 'ssn'));
SELECT COUNT(*) FROM pii_tokenized WHERE ssn = tok;
```

**3. `precomputed_token` — the floor** ([`sweep.py:584`](../fpe/scripts/sweep.py#L584))

The same scan with the token already known and inlined as a literal, so no
remote function is involved at all. Included to show how much of shape 2's time
is the scan itself rather than the round trip.

```sql
SELECT COUNT(*) FROM pii_tokenized WHERE ssn = '<literal token>'
```

| Approach | Elapsed | HTTP requests | Rows through function |
| --- | --- | --- | --- |
| 1. `detokenize_column` | 20.72s | 192 | 960,000 |
| 2. `tokenize_term` | **2.38s** | **1** | **1** |
| 3. `precomputed_token` (no UDF) | 0.77s | 0 | 0 |

Shape 3 shows the scan alone costs 0.77s, so the single remote call in shape 2
adds ~1.6s — most of which is the extra scripting statement, not the transit.

Bind the token with `DECLARE`/`SET` rather than inlining the call in `WHERE`:
BigQuery treats remote functions as non-deterministic and does not guarantee
constant-folding, so an inline call risks per-row evaluation — which would
collapse shape 2 back into shape 1.

**Constraints.** Equality and `IN` only — FPE is not order-preserving, so
ranges, `LIKE`, and plaintext ordering are impossible on ciphertext. It requires
deterministic tokenization, which leaks equality and permits frequency analysis;
that is a real security trade-off, not a free win. And normalization must match
exactly at write and search time.

---

## 6. Access control: authorized views + entitlement table

The pattern in [`fpe/sql/access_control_patterns.sql`](../fpe/sql/access_control_patterns.sql):
data arrives already tokenized, BigQuery never holds plaintext at rest, and
row/column access is enforced by authorized views joined to an entitlement
table. Detokenization happens through the remote function only for entitled data.

The natural way to write it is the slowest possible way.

| Pattern | What it is | Semantics | Elapsed | HTTP requests | Rows/request |
| --- | --- | --- | --- | --- | --- |
| **A** [`v_ssn_case`](../fpe/sql/access_control_patterns.sql#L52) | entitlement in a `CASE` arm | all rows, masked column | 29.44s | 975 | 1 |
| **B** [`v_ssn_union`](../fpe/sql/access_control_patterns.sql#L71) | `UNION ALL` of entitled + masked branches | *identical to A* | **1.40s** | 1 | 987 |
| **C** [`v_ssn_rowfilter`](../fpe/sql/access_control_patterns.sql#L101) | entitlement as a join predicate | fewer rows | 1.37s | 1 | 987 |
| **D** [`v_row_and_column`](../fpe/sql/access_control_patterns.sql#L129) | per-column CTE + `LEFT JOIN` back | all rows, 3 masked columns | **1.67s** | 74 | 24 |
| **D-naive** | three nested `CASE` expressions | *identical to D* | 52.47s | 1,953 | 1 |
| **E** [`v_name_dedup`](../fpe/sql/access_control_patterns.sql#L185) | decode `DISTINCT` tokens, join back | *identical to E-naive* | **2.05s** | **1** | **63** |
| **E-naive** [`v_name_naive`](../fpe/sql/access_control_patterns.sql#L203) | decode every row | — | 12.73s | 101 | 500,409 rows total |

Equivalence is asserted by the benchmark, not claimed in prose — A vs B, D vs
D-naive, and E vs E-naive all returned **0 mismatches**.

### A vs B — they are not the same query

An important correction if you're reading this as guidance: **A and C are not
interchangeable.** `CASE` masking returns every row with the column masked
(column-level control). A row filter returns fewer rows (row-level control).
Only **B** is result-equivalent to A: a `UNION ALL` of two unconditional
branches, one detokenizing the entitled rows, one emitting the mask.

### D — row *and* column control that scales

`UNION ALL` per masked column means 2^N branches for N independently-governed
columns. Three columns is eight branches, each rescanning the base table.

Pattern D scales linearly instead: decode each column in its own CTE gated by
that column's grant, then `LEFT JOIN` the decoded values back by key. The
`COALESCE` that applies the mask operates on a materialized join column, not on
a remote function call, so nothing short-circuits.

```sql
WITH visible AS (              -- ROW level: one entitlement join
  SELECT t.id, t.ssn, t.email, t.name
  FROM v_tokenized_branched t
  JOIN entitlements e ON e.user_email = SESSION_USER() AND e.branch_id = t.branch_id
),
grants AS (                    -- COLUMN level: collapse to one scalar row
  SELECT LOGICAL_OR(can_see_ssn) AS ssn, LOGICAL_OR(can_see_email) AS email
  FROM entitlements WHERE user_email = SESSION_USER()
),
ssn_dec AS (
  SELECT v.id, fpe_decrypt_b5000(v.ssn, 'ssn') AS val
  FROM visible v WHERE (SELECT ssn FROM grants)     -- gate, not a CASE
)
SELECT v.id, COALESCE(s.val, '***-**-****') AS ssn
FROM visible v LEFT JOIN ssn_dec s USING (id);
```

A user without a column grant makes that CTE scan zero rows, so the function is
never invoked for it — in the measured run `email` was ungranted and cost nothing.

### E — decode distinct values, not rows

Determinism means a column with C distinct values across R rows needs C
decryptions, not R. For `name` (63 distinct tokens in the entitled half of 1M
rows) that is **63 rows through the service instead of 500,409** — a 7,900x
reduction, 6.2x faster wall-clock. Only worth it when C ≪ R; for near-unique
columns like `ssn` the `DISTINCT` and join cost more than they save.

### Hardening

- **Put the routines in a dataset users cannot query.** Anyone who can call
  `fpe_decrypt` directly bypasses every view above. Expose only the views.
  Verify authorized-view and routine-dataset authorization explicitly.
- **Every query through these views counts against the 10-concurrent-query
  limit.** Scope detokenizing views to a small entitled population.
- **Detokenize last** (§4) — most analytics never need it.

---

## 7. Cloud Run tuning

400,000 rows, `fpe_decrypt`, median of 2 iterations. Full tables in
[`results/perf-tables.md`](results/perf-tables.md).

### `containerConcurrency` vs the worker model — all at 4 vCPU

**`containerConcurrency` is an admission limit, not a parallelism guarantee.**
It controls how many requests Cloud Run sends to an instance at once. What
decides how many *execute* simultaneously is the application's worker model.

| concurrency | workers | threads | class | Rows/s | µs/row (svc) | Peak in-flight |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | 1 | 1 | sync | 6,882 | 82 | 1 |
| 1 | 4 | 1 | sync | 9,518 | 94 | 1 |
| 8 | **1** | 1 | sync | 9,423 | 82 | 1 |
| 16 | 1 | 1 | sync | 15,895 | 58 | 1 |
| 4 | 4 | 1 | sync | 18,886 | 122 | 1 |
| **8** | **4** | 1 | sync | **25,728** | 110 | 1 |
| 16 | 8 | 1 | sync | 25,858 | 111 | 1 |
| 16 | **16** | 1 | sync | 22,501 | 140 | 1 |
| 8 | 1 | **8** | gthread | **7,911** | **308** | **6** |
| 8 | 2 | 4 | gthread | 13,582 | 118 | 4 |

Three things fall out:

- **Admission without workers buys little.** `c8-w1` (9,423 rps) is barely
  above `c1-w1` (6,882). Raising `containerConcurrency` alone lets requests in
  and then queues them behind a single process.
- **Threads are the trap.** The `gthread` row admits 6 concurrent requests into
  one process — `Peak in-flight = 6`, so the concurrency is real — and delivers
  the *worst* throughput of any 8-concurrency config at **308 µs/row versus 82**.
  The GIL serialises the work and the threads add contention on top. Two
  processes × 4 threads recovers exactly two processes' worth (13,582 ≈ 2 ×
  6,882), confirming only process count matters.
- **Oversubscription costs.** 16 workers on 4 vCPU (22,501) is slower than 8
  (25,858) and 4 (25,728).

**Set workers = vCPU for CPU-bound work.** Threads only help when the work
releases the GIL — the `io` mode below, not `fpe_*`.

### Vertical vs horizontal scaling

| vCPU (workers = vCPU) | Rows/s | | maxScale | Rows/s | Instances observed |
| --- | --- | --- | --- | --- | --- |
| 1 | 10,104 | | 1 | 21,442 | 1 |
| 2 | 10,702 | | 2 | 27,058 | 2 |
| 4 | 20,656 | | 4 | 48,957 | 4 |
| 8 | 31,120 | | 8 | **67,898** | 5 |

Horizontal scaling is the stronger lever: **3.2× from maxScale 1→8**, versus
3.1× for an 8× increase in vCPU. Adding instances also sidesteps the
oversubscription ceiling, since each instance gets its own vCPU allocation.

The 2 vCPU point (10,702) is anomalous — statistically indistinguishable from
1 vCPU. With 2 iterations per config the run-to-run variance is wide enough to
swallow a 2× effect, so read the shape of these curves, not individual points.

### Workload decomposition

| Mode | Rows/s | µs/row (svc) | What it isolates |
| --- | --- | --- | --- |
| `noop` | **403,075** | 0 | Pure BigQuery↔Cloud Run transit floor |
| `hmac` | 214,505 | 7 | Cheap deterministic tokenization |
| `io` | 175,679 | 10 | One bulk downstream call per batch |
| `fpe_decrypt` | 26,002 | 118 | Real FF3-1 |
| `cpu` | 16,469 | 128 | 100 sha256 rounds/row |

The transit floor is ~403,000 rows/s. FF3-1 runs at 26,000 — **so ~94% of
end-to-end time is compute, not network.** Any tuning effort belongs in the
service, not the wire. Note this inverts the assumption behind the Protegrity
benchmark's noop-vs-FPE framing, where the remote vendor API made transit
dominant.

### CPU throttling

| `cpu-throttling` | Rows/s |
| --- | --- |
| `false` (always allocated) | 16,738 |
| `true` (request-scoped) | 23,497 |

No benefit from always-allocated CPU, and the difference here runs the *wrong*
way. Both numbers sit inside the variance seen elsewhere in this sweep, so the
honest reading is **no measurable effect for this workload** — which makes
sense: throttling only withholds CPU *between* requests, and a saturated batch
pipeline is never between requests for long. Leave it at the cheaper default.

### Batch size

| `max_batching_rows` | Rows/s | Actual rows/request |
| --- | --- | --- |
| 100 | 21,401 | 100 |
| 500 | 14,268 | 500 |
| 1,000 | 29,068 | 1,000 |
| 2,500 | **32,093** | 2,500 |
| 5,000 | 29,108 | 5,000 |
| 10,000 | 31,416 | 10,000 |
| 25,000 | 28,007 | **11,905** |
| 50,000 | 28,655 | **11,905** |

Throughput is flat from ~1,000 rows upward — within noise across a 50× range of
requested batch sizes. Only the smallest batches (100) pay a visible
per-request penalty, and 500 is an outlier consistent with the variance noted
above. Combined with the 11,905 cap from §1: **batch size is close to a
non-knob** above ~1,000. Tune workers and instances instead.

---

## Practical checklist

Ordered by the size of the effect measured here.

1. **Never put a remote function inside `CASE`/`IF`/`MERGE ... WHEN`** (~180x).
   Use `UNION ALL` of unconditional branches, or filter first. Hoisting into a
   subquery does *not* work.
2. **Decode distinct values, not rows**, for low-cardinality columns (7,900x
   fewer rows through the service on a 63-distinct-value column).
3. **Search by tokenizing the term**, bound via `DECLARE`/`SET` (~9x, and
   960,000 → 1 row through the function).
4. **Scale horizontally.** `maxScale` 1→8 gave 3.2x; it beats vCPU per unit of
   effort and avoids worker oversubscription.
5. **Set gunicorn workers = vCPU** for CPU-bound work; `containerConcurrency` a
   small multiple of that. Threads buy nothing without I/O — measured 3.3x
   *worse* per row than processes at the same concurrency.
6. **Detokenize after `LIMIT` and after aggregation** (5–22x). Don't bother
   restructuring `WHERE` — BigQuery already pushes it down.
7. **Return 400, not 500**, for deterministic errors, or pay up to 20 retries
   per partition.
8. **Design for 10 concurrent remote-function queries** even though we couldn't
   reproduce the limit, and request an increase early if you need more.
9. **Don't tune `max_batching_rows`** above ~1,000. It is capped by a ~256 KiB
   request-body budget (11,905 rows for narrow columns, 6,025 for email-width),
   and throughput is flat well below that. Cloud Run sizing cannot raise it.

## Reproducing

```bash
python fpe/scripts/sweep.py --list                 # plan only, deploys nothing
python fpe/scripts/sweep.py --phase all            # infrastructure sweep
python fpe/scripts/sweep.py --phase limits         # documented limits
python fpe/scripts/sweep.py --phase access_control # authorized-view shapes
python fpe/scripts/analyze.py fpe/scripts/sweep_raw_*.jsonl
```

## Caveats

- 2 iterations per configuration in the infrastructure sweep. Run-to-run
  variance is wide enough to swallow ~2x effects (see the 2 vCPU and
  `cpu-throttling` rows). Trends are sound; individual points are not.
- Single project, single region, one workload shape (CPU-bound, ~77 µs/row,
  narrow STRING columns). A memory-bound or I/O-bound service would invert the
  worker-model conclusions.
- The ~256 KiB request-body budget was consistent across four column widths
  and three Cloud Run shapes, but is undocumented — treat it as observed
  behaviour rather than contract.
