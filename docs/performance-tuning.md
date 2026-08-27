# BigQuery Remote Functions on Cloud Run — measured behaviour

Everything below was measured on real infrastructure (project `<PROJECT_ID>`,
`us-central1`, August 2026) using [`fpe/scripts/sweep.py`](../fpe/scripts/sweep.py).
Raw records are JSONL under [`fpe/results/`](../fpe/results/); regenerate any
table with `python fpe/scripts/analyze.py fpe/results/<file>`.

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
   sizing has no effect on it whatsoever. It is a batching *target*, though —
   a single row wider than that is still sent, alone, up to a hard 5 MiB.
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
8. **"Workers = vCPU" under-provisioned this service by 1.29x.** 16 gunicorn
   workers on 4 vCPU beat 4, with non-overlapping run ranges. The rule assumes
   pure-Python CPU burn; AES-in-C and socket I/O take processes off-core, and
   extra processes fill the gaps. Measure it rather than applying the rule.
9. **`containerConcurrency` barely matters above a floor.** Everything from 2 to
   80 was inside the run-to-run noise. Only `= 1` was clearly bad, by starving
   workers.

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
- **Row width sets it.** Each row is serialised into the `calls` array as one
  JSON array of that row's argument *values* — for this repo's two-argument
  functions, `["<value>","<data_element>"],`. `data_element` is the second
  *argument value*, not the column name: the `dob` column is passed `'digits'`
  ([`sweep.py:483`](../fpe/scripts/sweep.py#L483)), which is why its row costs
  24.0 bytes and not 21.0. Column names never cross the wire. Multiply the cap
  by those bytes:

  | Column | Data element | Cap | Bytes/row on the wire | Cap x bytes |
  | --- | --- | --- | --- | --- |
  | `ssn` | `'ssn'` | 11,905 | 11 + 3 + 8 = 22.0 | 261,910 |
  | `dob` | `'digits'` | 10,913 | 10 + 6 + 8 = 24.0 | 261,912 |
  | `name` | `'name'` | 11,474 | ~10.9 + 4 + 8 = 22.9 | 262,755 |
  | `email` | `'email'` | 6,025 | ~30.5 + 5 + 8 = 43.5 | 262,088 |

  Constant at ~262,000 bytes. **256 KiB is 262,144**, leaving ~232 bytes of
  JSON envelope (`requestId`, `caller`, `sessionUser`, `userDefinedContext`).
  `ssn` and `dob` are fixed-width and land exactly; the variable-width columns
  scatter by a few hundred bytes, which is just the mean-length estimate.

  The 8 bytes are the JSON punctuation around a *two-argument* element —
  `[`, `"`, `"`, `,`, `"`, `"`, `]`, `,`: two brackets, four quotes, one comma
  between the arguments, one separating this element from the next. It is not a
  universal constant. Writing `n_args` for the number of quoted string
  arguments, the punctuation costs `3 × n_args + 2` bytes: 5 for one argument,
  8 for two, 11 for three.

So the rule is **~256 KiB of request body**, and you can predict your own cap:

```
rows_per_request ≈ 261,900 / (sum of argument lengths + 3 × n_args + 2)

  n_args = number of quoted string arguments the function takes
```

That predicts 11,905 / 10,913 / 11,436 / 6,021 for the four columns above,
against 11,905 / 10,913 / 11,474 / 6,025 measured — within 0.3% on all four.
(Numeric arguments lose their two quotes; `BYTES` are base64, so ~4/3 of raw
length.) Note this is observed behaviour, not documented contract — but it held
across every configuration tested.

### How arity, batch size and latency connect

Argument count is a design decision, and it propagates through a chain:

```
arity + argument lengths
  -> bytes per row on the wire
  -> rows per request (batch size), since the ~256 KiB budget is fixed
  -> number of HTTP requests needed for a given table
  -> total latency
```

Every link is real, but **the last one is weak**: total latency is dominated by
per-row processing, which arity does not change. Quantified at the end of this
section.

Nothing about remote functions is two-argument. The signature is whatever your
`CREATE FUNCTION` declares, and that is a three-way contract nothing validates:

1. **The DDL declares the signature.** This repo hardcodes
   `(val STRING, data_element STRING)` for every generated FPE function
   ([`generate_remote_functions.py:84`](../fpe/scripts/generate_remote_functions.py#L84))
   and for the Protegrity functions
   ([`create_remote_functions.sql`](../protegrity/sql/create_remote_functions.sql)),
   but `pii_noop(val STRING)` in that same file takes one — 5 bytes of overhead
   per row, not 8.
2. **BigQuery serialises exactly those arguments, in declared order, values
   only.** No names, no types, no schema exchange.
3. **The service unpacks positionally** — `call[0]`, `call[1]`
   ([`main.py:218`](../fpe/service/main.py#L218)).

If 1 and 3 disagree you find out at runtime: a surplus declared argument is
silently ignored, a missing one raises `IndexError`, and Flask surfaces that as
a 500 — 20 retries per partition (§2).

**Per-row constants do not belong in arguments.** `user_defined_context` is
fixed at `CREATE FUNCTION` time and sent **once per request** in the envelope,
so it costs nothing per row. That is already how `mode` is passed here.
`data_element` could go the same way:

| Design | Per-row wire form | Bytes/row (`ssn`) | Cap | Functions to maintain |
| --- | --- | --- | --- | --- |
| Two arguments (this repo) | `["123-45-6789","ssn"],` | 22 | 11,905 | 1, any data element |
| One argument + context | `["123-45-6789"],` | 16 | **16,368** | one per data element |

+37% batch on `ssn`, +22% on `email`, and the service already supports it:
`default_de` reads `data_element` from the context and the per-row argument is
optional ([`main.py:209`](../fpe/service/main.py#L209),
[`main.py:221`](../fpe/service/main.py#L221)). Only the DDL template is fixed at
two arguments.

We kept two arguments: one function serves every data element, versus N
functions to regenerate whenever a new column is governed.

**And the +37% buys far less than it looks like.** Follow the chain to its end.
Dropping the argument raises batch size from 11,905 to 16,368, which cuts the
request count for a 1M-row table from 84 to 62 — a 26% reduction in *requests*.
But the rows, and therefore the FF3-1 work, are unchanged. All you save is the
per-request fixed cost on the 22 requests you eliminated.

The batch sweep in §7 measures how much that is worth: throughput was flat from
1,000 rows/request (29,068 rows/s) to 50,000 (28,655 rows/s) — indistinguishable
across a 50x range. The 11,905 → 16,368 move sits entirely inside that flat
region, so it is worth approximately nothing here.

Arity matters when per-request overhead is a meaningful share of the total —
tiny result sets, a very cheap per-row transform, or an unusually wide argument
list. For a workload where per-row processing dominates, choose your signature
for maintainability and let the batch size fall where it does.

Fitting that fixed cost out of §7's batch-size table — 100 rows/request at
21,401 rows/s against 2,500 at 32,093, over 4 workers — gives **~6.5 ms per
request**, against 118 µs/row of compute. So on a 1M-row `ssn` query the
one-argument form issues 62 requests instead of 84 and saves ~143 ms out of
~118 s of work: **0.12%**. The break-even is where per-row work equals the
amortised fixed cost, `6,458 µs ÷ 16,368 rows ≈ 0.4 µs/row`. Against §7's
workload decomposition `fpe_decrypt` sits 300x above that line and spends 0.3%
of each request on overhead, `hmac` (7 µs/row) spends 5%, and only a
`noop`-class function spends enough for argument width to matter. Optimise it
when you are per-request-bound rather than compute-bound, or when the endpoint
bills per invocation.

(That 6.5 ms is fitted from two points in a table the Caveats section warns is
noisy — an order of magnitude, not a constant. It would have to be wrong by 100x
to change the conclusion.)

One constraint if you are tempted to fold arguments together: remote functions
do not support `ARRAY`, `STRUCT`, `INTERVAL` or `GEOGRAPHY`, so you cannot pass
a struct. `JSON` is allowed.

Two things this cap is *not*. It is not the documented 5 MB per-row input
limit — that governs a different regime, and both are measured in
[§2](#per-row-input-size--and-why-it-does-not-conflict-with-1). And it is not
a hard ceiling on request size: a single row wider than the budget is still
sent, alone.

### Consequences

- Benchmarks comparing `b50000` against `b100000` are comparing identical
  configurations. This retroactively explains the Protegrity results in
  [`protegrity/README.md`](../protegrity/README.md), where batch sizes from
  10,000 to 100,000 all produced ~50,000 rows/s.
- **The cap is per HTTP request, not per query or per project.** Each query
  slices its own rows independently, so N concurrent queries each get full-size
  batches. Nothing is shared across queries.
- Wide columns cost you batching efficiency automatically, and so does every
  extra argument you declare — see the arity trade-off above.
- The cap keeps you clear of the 15 MB *response* limit for ordinary row
  widths — you have to inflate replies deliberately to breach it (see §2).

---

## 2. Documented limits, probed until they broke

Source: [BigQuery quotas — remote functions](https://docs.cloud.google.com/bigquery/quotas#remote_function_limits).

Ordered as the subsections below, request side first.

| Limit | Documented | Measured |
| --- | --- | --- |
| Max input size (all args, one row) | 5 MB | **Confirmed** — 5 MiB exactly (5,242,880 B); 6 MB fails |
| HTTP response size (Cloud Run / gen2) | 15 MB | **Confirmed** — 14.3 MB passes, 17.9 MB fails |
| Max HTTP invocation retry attempts | 20 | **Confirmed** (~99 invocations over 5 partitions) |
| Concurrent queries with remote functions | 10 / project | **Not reproduced** |
| HTTP invocation time limit | 20 min | Not reached (our timeout is 900s) |

### Per-row input size — and why it does not conflict with §1

This limit is *per row* — "the maximum total size of all input arguments from
a **single row**" — and is not what produces the ~256 KiB batch budget in §1:
dividing 5 MB by the caps measured there yields 420–830 bytes/row, which matches
nothing on the wire.

That raises an obvious objection. If BigQuery never sends more than ~256 KiB,
this limit could never be reached and would be dead letter. It isn't, because
**the 256 KiB budget is a batching target, not a hard cap on request size.**
BigQuery packs rows until the next one would overflow the budget — but it always
sends at least one row, even when that row alone exceeds it.

Measured, using `hmac` (whose reply is 16 chars regardless of input, so the
response ceiling stays out of the way):

| Bytes per row | Rows per request | Result |
| --- | --- | --- |
| 50,000 | 6, 2, 1 | OK — several rows fit |
| 200,000 | 2 | OK |
| 300,000 | **1** | OK — request exceeds the 256 KiB budget |
| 1,000,000 | **1** | OK |
| 5,000,000 | **1** | OK |
| 6,000,000 | **1** | **FAILED** |

The failure is explicit and gives the real number:

```
The maximum total size of all input parameters is 5242880 bytes.
```

**5,242,880 is exactly 5 MiB**, not decimal 5 MB.

So the two limits never compete — they govern different regimes:

| Row width | What binds | Effect |
| --- | --- | --- |
| < ~256 KiB | the batching budget | many rows per request; this is the normal case |
| > ~256 KiB | nothing until 5 MiB | one row per request, request as large as the row |
| > 5 MiB | the input limit | query fails |

The 5 MiB ceiling is therefore the one that matters for *wide* columns — free
text, JSON documents, base64 `BYTES` — not for the narrow PII fields this repo
benchmarks. A Protegrity-style discovery pass over a document column would live
squarely in that second regime, at one row per request with no batching at all.

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

## Best practices for remote function UDFs

Sections 1 and 2 are how BigQuery behaves: fixed, not yours to change, and worth
knowing so you stop tuning things that cannot move. The four below are the
opposite — query shapes you fully control, where the same answer can cost 180x
more or less depending on how you write the SQL.

| § | Practice | Measured effect |
| --- | --- | --- |
| 3 | Never let the call sit inside `CASE`/`IF` | **~180x** |
| 4 | Reduce (`LIMIT`, aggregate) before you detokenize | **5–22x** |
| 5 | Search by tokenizing the term, not detokenizing the column | **~9x** |
| 6 | Shape authorized views so batching survives entitlement logic | **~33x** |

They share one mechanism. A remote function is only fast when BigQuery can send
many rows per HTTP request, and each of these is a way that property gets
silently destroyed — by a conditional, by evaluating rows you were going to
throw away, by filtering on a decrypted column, or by an access-control
expression. None of them raise an error. The query is simply slow.

### 3. The batching cliff

BigQuery disables batching when a remote function sits inside a short-circuiting
expression. The documented wording is: *"If evaluation is short-circuited (e.g.
conditional expressions, `MERGE ... WHEN [NOT] MATCHED`), batching is disabled
and the `calls` field has exactly one element."*

#### What "`calls` has exactly one element" means

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

#### The four shapes

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

#### Results — 20,000 rows

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

### 4. Where you put the call

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

### 5. Search: tokenize the term, don't detokenize the column

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

**3. `precomputed_token` — caller already holds the token** ([`sweep.py:584`](../fpe/scripts/sweep.py#L584))

The same scan with the token supplied as a literal, so no remote function is
involved at all.

```sql
SELECT COUNT(*) FROM pii_tokenized WHERE ssn = '<literal token>'
```

This is both the measurement floor — it isolates how much of shape 2 is the scan
rather than the round trip — and a realistic pattern in its own right:

- **The token is already the identifier.** When data arrives pre-tokenized from
  an upstream PEP and that system hands downstream consumers the token rather
  than the plaintext, every lookup is naturally token-based and BigQuery never
  tokenizes anything.
- **Application-side caching.** Resolve a subject once, keep the token, and
  every subsequent query — dashboard refresh, drill-down, pagination — is a
  literal comparison. You pay tokenization once per subject, not once per query.
- **BI tools that cannot script.** Shape 2 requires a `DECLARE`/`SET`
  multi-statement script, which most dashboarding layers cannot emit. They can
  usually pass a token as a filter parameter, making shape 3 the only fast path
  actually available.
- **It escapes the concurrency limit.** The per-project cap in §2 counts
  *queries containing remote functions*. Shape 2 contains one; shape 3 contains
  none, so it does not count at all. For a high-concurrency interactive
  workload that is the difference between contending for 10 slots and having no
  remote-function ceiling.

The trade-off: the caller must hold the token, so either it has key access or a
trusted system supplied it. Distributing tokens is normally fine — but under
deterministic tokenization a token is a stable pseudonymous identifier that
permits correlation across datasets and over time, so treat "non-sensitive" as
a threat-model decision rather than an assumption. Cached tokens also go stale
across key rotation.

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

### 6. Access control: authorized views + entitlement table

The pattern in [`fpe/sql/access_control_patterns.sql`](../fpe/sql/access_control_patterns.sql):
data arrives already tokenized, BigQuery never holds plaintext at rest, and
row/column access is enforced by authorized views joined to an entitlement
table. Detokenization happens through the remote function only for entitled data.

The natural way to write it is the slowest possible way.

Grouped by scenario, naive shape first in each:

| Scenario | Pattern | What it is | Elapsed | Speed-up | HTTP requests | Rows to service |
| --- | --- | --- | --- | --- | --- | --- |
| **1.** Mask one column<br>*(all rows returned)* | **A** — naive<br>[`v_ssn_case`](../fpe/sql/access_control_patterns.sql#L71) | entitlement inside a `CASE` arm | 25.78s | — | 987 | 987 |
| | **B** — fix<br>[`v_ssn_union`](../fpe/sql/access_control_patterns.sql#L89) | `UNION ALL` of two unconditional branches | **0.79s** | **33x** | **1** | 987 |
| **2.** Mask three columns<br>*(all rows returned)* | **D-naive** — naive<br>[`v_row_and_column_naive`](../fpe/sql/access_control_patterns.sql#L125) | one `CASE` per governed column | 50.74s | — | 1,974 | 1,974 |
| | **D** — fix<br>[`v_row_and_column`](../fpe/sql/access_control_patterns.sql#L158) | per-column CTE + `LEFT JOIN` back | **1.59s** | **32x** | 76 | 1,885 |
| **3.** Low-cardinality column<br>*(all entitled rows)* | **E-naive** — naive<br>[`v_name_naive`](../fpe/sql/access_control_patterns.sql#L211) | decode every row | 13.46s | — | 101 | 500,409 |
| | **E** — fix<br>[`v_name_dedup`](../fpe/sql/access_control_patterns.sql#L231) | decode `DISTINCT` tokens, join back | **0.95s** | **14x** | **1** | **63** |
| **4.** Row-level only<br>*(fewer rows returned)* | **C**<br>[`v_ssn_rowfilter`](../fpe/sql/access_control_patterns.sql#L260) | entitlement as a join predicate | 0.96s | n/a | 1 | 987 |
| **5.** Point lookup by plaintext<br>*(one record)* | **F-naive** — naive<br>[`v_lookup_naive`](../fpe/sql/access_control_patterns.sql#L296) | filter on the decrypted column | 20.52s | — | 200 | 1,000,000 |
| | **F** — fix<br>[`v_lookup_by_token`](../fpe/sql/access_control_patterns.sql#L324) | tokenize term, filter on ciphertext | **1.79s** | **11x** | **1** | **1** |
| | **F** — caller holds token | same view, token supplied by caller | **0.42s** | **49x** | **0** | **0** |

Within each scenario the two shapes return **identical results** — the benchmark
asserts it rather than claiming it, and A vs B, D vs D-naive and E vs E-naive
all returned **0 mismatches**. Scenario 4 is listed separately because it is
*not* result-equivalent to the others: it filters rows away instead of masking a
column, so it has no naive counterpart to beat.

Do not compare elapsed times *across* scenarios. Scenarios 1, 2 and 4 run over a
~1,950-row slice; scenario 3 runs over the full entitled half of the 1M-row
table. Only the within-scenario ratios are meaningful.

#### A vs C — masking and filtering are different semantics

An important distinction if you're reading this as guidance: **A and C are not
interchangeable.** `CASE` masking (A) returns every row with the column masked —
column-level control. A row filter (C) returns fewer rows — row-level control.
C is faster than A, but it is not a faster *A*; it answers a different question.

Only **B** is result-equivalent to A: a `UNION ALL` of two unconditional
branches, one detokenizing the entitled rows, one emitting the mask. That is why
the table pairs A with B, and lists C on its own as scenario 4.

#### D — row *and* column control that scales

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

#### E — decode distinct values, not rows

Determinism means a column with C distinct values across R rows needs C
decryptions, not R. For `name` (63 distinct tokens in the entitled half of 1M
rows) that is **63 rows through the service instead of 500,409** — a 7,900x
reduction, and 14x faster wall-clock. Only worth it when C ≪ R; for near-unique
columns like `ssn` the `DISTINCT` and join cost more than they save.

#### F — point lookup by plaintext, combining §5 and §6

The shape most user-facing traffic actually takes: *"show me this one person's
record, if I'm allowed to see it."* It needs the search pattern from §5 and the
access control from §6 at the same time, and they pull against each other — an
authorized view must detokenize to be useful, but a lookup must filter *before*
detokenizing or it decrypts the whole table.

The naive query looks entirely reasonable:

```sql
SELECT * FROM v_lookup_naive WHERE ssn = '123-45-6789';   -- 20.52s
```

The predicate references the view's *decrypted* column, so it cannot be
evaluated until decryption has happened. **1,000,000 rows crossed the wire** —
the entire table, not merely the entitled half — to return one record.

The resolution is one extra column: the view projects the raw token alongside
the decrypted value, so callers can filter on ciphertext.

```sql
CREATE VIEW v_lookup_by_token AS
SELECT t.id, t.ssn AS ssn_token,                    -- <- the enabling column
       fpe_decrypt_b5000(t.ssn, 'ssn') AS ssn
FROM v_tokenized_branched t
JOIN entitlements e ON e.user_email = SESSION_USER()
                   AND e.branch_id = t.branch_id AND e.can_see_ssn;

DECLARE tok STRING;
SET tok = (SELECT fpe_encrypt('123-45-6789', 'ssn'));
SELECT * FROM v_lookup_by_token WHERE ssn_token = tok;    -- 1.79s
```

**1 row through the service instead of 1,000,000**, 11x faster, with the
entitlement join still enforcing access. This works because of the §4 finding
that BigQuery pushes simple `WHERE` predicates through a remote function: the
predicate on `ssn_token` is applied before the decryption in the projection, so
only the matching row is ever decrypted.

And when the caller already holds the token (§5), the query contains no remote
function at all — **0.42s, zero invocations**, and it does not count against the
10-concurrent-query limit.

The trade-off is that the view now exposes ciphertext. That is not plaintext,
but under deterministic tokenization a token is a stable pseudonymous identifier
that permits correlation across datasets — so project it only where the caller
is already entitled to the row, which the join above guarantees.

#### Hardening

- **Put the routines in a dataset users cannot query.** Anyone who can call
  `fpe_decrypt` directly bypasses every view above. Expose only the views.
  Verify authorized-view and routine-dataset authorization explicitly.
- **Every query through these views counts against the 10-concurrent-query
  limit.** Scope detokenizing views to a small entitled population.
- **Detokenize last** (§4) — most analytics never need it.

---

## 7. Cloud Run tuning

400,000 rows, `fpe_decrypt`, median of 2 iterations. Full tables in
[`../fpe/results/perf-tables.md`](../fpe/results/perf-tables.md).

### `containerConcurrency` vs the worker model — all at 4 vCPU

First, whose knobs these are — because only one of the four columns below is a
Cloud Run setting at all:

| Column | Owned by | What it is |
| --- | --- | --- |
| `concurrency` | **Cloud Run** | `containerConcurrency`: how many requests the platform will send to one instance at once |
| `workers` | **gunicorn** | OS *processes* forked inside the container. Each is a separate Python interpreter with its own GIL |
| `threads` | **gunicorn** | Threads per worker process, sharing that process's GIL |
| `class` | **gunicorn** | Worker model: `sync` = one request per worker at a time (`threads` ignored); `gthread` = a thread pool per worker |

Cloud Run knows nothing about the last three. It sees a container listening on a
port; how that container handles concurrent requests is entirely your
application's business. In this repo they are set in
[`gunicorn.conf.py`](../fpe/service/gunicorn.conf.py) from the `FPE_WORKERS`,
`FPE_THREADS` and `FPE_WORKER_CLASS` env vars on the revision, so a sweep can
change the worker model without rebuilding the image.

The two sides multiply out like this:

```
Cloud Run admits           up to `concurrency` requests per instance
The app executes           workers x threads   (gthread)
                           workers             (sync — threads ignored)
Anything beyond that       queues in the socket backlog
```

**So `containerConcurrency` is an admission limit, not a parallelism
guarantee.** Set it above what the app can execute and the surplus simply waits
inside the container: latency rises, throughput does not. That is the `c8-w1`
row below — 8 admitted, 1 executing.

And for CPU-bound pure Python, only `workers` buys parallelism. Threads share a
GIL, so they interleave rather than run at once; they help only when the work
*releases* the GIL, as blocking I/O does. Hence `gthread` being the worst row in
the table despite genuinely holding 6 requests in flight.

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

Threads only help when the work releases the GIL — the `io` mode below, not
`fpe_*`.

Concurrency and workers move together in that table, so it cannot say what
either should be on its own. The next two subsections isolate them in turn —
concurrency first, because the worker result depends on it.

### So what should `containerConcurrency` be?

The table above cannot answer that: it varies concurrency *and* workers
together, so the two are confounded. This isolates it — cpu=4, workers=4, sync,
**maxScale=1** (with autoscaling on, a low concurrency just makes Cloud Run add
instances, which measures the autoscaler instead of the setting).

| `containerConcurrency` | Median rows/s | Min | Max | Spread across 2 runs |
| --- | --- | --- | --- | --- |
| **1** | **9,348** | 9,234 | 9,462 | 1.02x |
| 2 | 17,357 | 17,100 | 17,615 | 1.03x |
| 4 | 19,136 | 17,273 | 21,000 | 1.22x |
| 6 | 23,322 | 21,390 | 25,254 | 1.18x |
| 8 | 18,975 | 17,940 | 20,010 | 1.12x |
| 12 | 16,584 | 10,505 | 22,663 | **2.16x** |
| 16 | 16,500 | 11,092 | 21,908 | **1.98x** |
| 32 | 17,947 | 16,333 | 19,560 | 1.20x |
| 80 *(Cloud Run default)* | 22,463 | 18,838 | 26,089 | 1.38x |

**Read the last column before the first.** Two runs of the *same* configuration
differ by up to 2.16x, while the entire span of medians from concurrency 2 to 80
is 1.41x. The between-config variation is smaller than the within-config noise,
so this sweep cannot distinguish 2 from 80, and the apparent peak at 6 is not a
result — chasing it would be fitting noise.

One thing is clean, because its two runs agree to 1.02x and the gap is 2.5x:

> **`containerConcurrency: 1` starves the workers.** With 4 worker processes and
> an admission limit of 1, three processes idle permanently. Everything from 2
> upward is indistinguishable.

So the practical rule is a floor, not an optimum:

```
containerConcurrency >= workers        # or workers x threads for gthread
```

Set it at or a little above your worker count so no process starves, and spend
your tuning effort on `workers` (next subsection) and `maxScale`, which produced
3.7x and 3.2x respectively — effects far outside this noise band. Cloud Run's default of 80 is
fine here; there is no measured reason to lower it, and lowering it below your
worker count is the one way to make things actively worse.

This also argues for reading the previous table conservatively. The
worker-model conclusions there rest on gaps of 3x or more (6,882 → 25,728 rows/s,
82 → 308 µs/row), which survive this noise comfortably. Differences of 20–30%
between adjacent rows do not.

### So how many workers, then?

If concurrency should just sit at the default of 80, the obvious objection is
that 4 workers must then be far too few. Standard advice says workers track
vCPU, because processes contend for cores. Isolating it — cpu=4,
`containerConcurrency` 80 as concluded above, sync, maxScale=1, 3 runs each —
says otherwise:

| workers | vs 4 vCPU | Median rows/s | Range across 3 runs | Spread |
| --- | --- | --- | --- | --- |
| 1 | 0.25x | 10,543 | 6,351 – 10,996 | 1.73x |
| 2 | 0.5x | 12,689 | 11,982 – 14,499 | 1.21x |
| 4 | **1x** | 26,980 | 22,793 – 26,996 | 1.18x |
| 8 | 2x | 24,806 | 23,966 – 25,352 | 1.06x |
| **16** | **4x** | **34,737** | **34,182 – 39,487** | 1.16x |
| 32 | 8x | 29,172 | 27,980 – 31,107 | 1.11x |

**16 workers on 4 vCPU beat 4 workers by 1.29x, and the ranges do not overlap** —
22,793–26,996 against 34,182–39,487. Unlike the concurrency sweep just above,
this clears the noise band comfortably: it is signal. Throughput then falls back at 32, so there is a real peak
around 4x vCPU.

So "workers = vCPU" is wrong here, and the reason is that the work is not the
pure-Python CPU burn that rule assumes. FF3-1 runs AES through pycryptodome, a C
extension, and every request also parses JSON, allocates, and reads and writes a
socket. A process spends real time off-core, and extra processes fill those gaps.
The rule only holds for work that genuinely occupies a core end to end.

This does not contradict the earlier `c16-w16` row (22,501 rows/s, worse than
`c16-w8`). That ran at `containerConcurrency` 16, so the admission gate starved
16 workers. Feed the same 16 workers at concurrency 80 and they reach 34,737.
The two settings interact exactly as the floor rule predicts: concurrency must be
at least workers, or the extra processes never see traffic.

**Measure this for your own workload.** What sets the peak is how much of a
request's wall time is spent *not* holding a core — waiting on a socket, or
inside a C extension that has released the GIL. The more a process idles, the
more processes it takes to keep the cores busy:

| Workload | Time off-core | Useful slots |
| --- | --- | --- |
| Pure-Python CPU transform | ~none | ~1x vCPU |
| This service (AES in C, JSON, sockets) | substantial | **4x vCPU, measured** |
| Blocked on a remote API (e.g. Protegrity) | nearly all | many times vCPU |

One important caveat on that third row: it needs more concurrent *slots*, but
they should not be processes. A request waiting on a socket has released the
GIL, so threads serve just as well and cost a fraction of the memory — 32
gunicorn processes here needed 16 GiB, where 32 threads would need almost
nothing extra. Reach for `gthread` with a high thread count when the work is
I/O-bound, and for `sync` with more processes when it is CPU-bound, as here.
That is the same split as the `gthread` rows in the first table, read from the
other direction.

What generalises is the method, not the number 4: fix everything else, set
concurrency high, sweep workers, and check whether the run ranges overlap before
believing a difference.

### Vertical vs horizontal scaling

| vCPU (workers set = vCPU) | Rows/s | | maxScale | Rows/s | Instances observed |
| --- | --- | --- | --- | --- | --- |
| 1 | 10,104 | | 1 | 21,442 | 1 |
| 2 | 10,702 | | 2 | 27,058 | 2 |
| 4 | 20,656 | | 4 | 48,957 | 4 |
| 8 | 31,120 | | 8 | **67,898** | 5 |

Horizontal scaling is the stronger lever: **3.2× from maxScale 1→8**, versus
3.1× for an 8× increase in vCPU. Note the vCPU column held workers equal to
vCPU, which the worker sweep above shows under-provisions this service — so
those figures are a floor on what each instance size can do, not a ceiling. Adding instances also sidesteps the
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

### Worked example: sizing 300 concurrent 1M-row queries

Everything above is a measurement of one configuration at a time. This applies
those constants to a concrete ask — **300 concurrent queries, 1,000,000 rows
each, 10-byte values, through a one-argument function.** It is *derived from*
the tables above, not separately measured; treat the arithmetic as planning
guidance, not as a result.

**Requests.** One argument means 5 bytes of JSON punctuation per row (§1), so
15 bytes on the wire:

```
261,900 / (10 + 5) = 17,460 rows/request
1,000,000 / 17,460 ≈ 58 requests per query  →  ~17,400 requests in total
```

The two-argument form of the same function, with a 3-character data element,
costs 21 bytes/row → 12,471 rows/request → 81 requests per query, ~24,300 in
total. Dropping the argument removes **28% of the HTTP requests** — which, per
the batch-size table above, is worth approximately nothing here, because each
request is 2.06 s of FF3-1 either way (17,460 rows × 118 µs) and per-request
overhead is invisible against that.

**Concurrent requests — not 17,400.** Requests issued and requests in flight are
different numbers. §2's solo 1M-row query ran at 54,202 rows/s with 1.40 s of
service time per request, which by Little's law is ~6 requests in flight. So 300
such queries offer roughly **1,800 concurrent requests**, and at
`containerConcurrency 8` that needs ~225 instances to absorb without queueing.
Read ~6 as a floor: that query was already service-limited, so a faster backend
would draw more.

**Capacity.** 300 × 1M = 300,000,000 rows of FF3-1. The horizontal-scaling table
above gives ~21,000 rows/s for a single 4-vCPU instance and ~12,000–13,600
rows/s for each instance beyond the first (scaling efficiency ~60%):

| `maxInstances` (4 vCPU each) | Aggregate rows/s | Wall clock, all 300 queries |
| --- | --- | --- |
| 4 | 48,957 *(measured)* | ~1h 42m |
| 8 | 67,898 *(measured)* | ~1h 14m |
| 100 (Cloud Run default) | ~1.3M *(extrapolated)* | ~4 min |
| 250 | ~3.3M *(extrapolated)* | ~1.5 min |

Those wall-clock figures assume the work simply queues, and it does: BigQuery
holds ~6 requests in flight per query and issues the next only when one returns,
so all 300 queries make proportional progress and finish together rather than
some finishing early. The cost lands on per-request latency instead. At
`maxInstances` 4 the service completes 48,957 / 17,460 ≈ 2.8 requests/s, so
1,800 in flight means **~640 s per HTTP request**; at `maxInstances` 8, ~460 s.

Both sit under the service's 900 s Cloud Run request timeout
([`service.yaml.template:34`](../fpe/service/service.yaml.template#L34)) — but
with no margin, and that clock includes time queued waiting for an instance, not
just handler time. So the mean request survives and the tail does not. The
likelier failure is Cloud Run shedding queued requests as 429 before the timeout
is reached, at which point BigQuery's retry behaviour (§2: 99 invocations for 5
partitions) amplifies the load rather than shedding it.

Matching the solo latency of a single 1M-row query (18.4 s, §2) across all 300
would take ~15M rows/s — about 1,150 instances and 4,600 vCPU, well past a
default regional quota.

Do not read that as 46 vCPU across 100 instances. Cloud Run caps an instance at
8 vCPU, and the vertical table above shows CPU does not scale linearly anyway —
per-vCPU throughput *falls* from 10,104 rows/s at 1 vCPU to 3,890 at 8. One
hundred 8-vCPU instances is ~1.7M rows/s — against ~1.3M for a hundred 4-vCPU
instances in the table above, so doubling per-instance CPU bought 1.3x, not 2x.
That is ~3 minutes for the 300M rows, still 9x short of the target. Reaching 15M
rows/s with 8-vCPU instances would take ~880 of them and ~7,000 vCPU — more
total CPU than the 4-vCPU shape needs for the same work.

**What actually binds, in order.** BigQuery's 10-concurrent-remote-function-query
limit (§2) — 300 is 30x that, so this whole exercise presumes a quota increase.
Then the regional Cloud Run vCPU quota, which caps `maxInstances` at
`quota ÷ 4 vCPU`. Then, and only then, anything in the tables above. Note also
that overload here surfaces as latency rather than rejection until Cloud Run's
queue depth trips 429s — at which point BigQuery's retry behaviour (§2: 99
invocations for 5 partitions) amplifies the load rather than shedding it.

**What this does not change.** None of §7's tuning conclusions move: still
`workers = vCPU`, still no threads for CPU-bound work, still horizontal before
vertical. The scenario changes the sizing arithmetic, not the shape of the
service.

**And check whether you need any of it.** 300 concurrent queries is an ordinary
interactive workload. 300 concurrent queries each pushing 1,000,000 rows through
the remote function is the shape §3–§6 exist to eliminate, and the arithmetic
above is what it costs to brute-force past them. Before provisioning thousands
of vCPU:

- **Is a conditional wrapping the call?** Then it isn't 58 requests per query,
  it's ~1,000,000 — one per row (§3). Fix that before sizing anything.
- **Is the query aggregating?** `COUNT(DISTINCT token)` equals
  `COUNT(DISTINCT plaintext)` under deterministic tokenization: 400,000 rows
  became **zero** invocations (§4). Analytics over tokenized columns should not
  reach the function at all.
- **Is it a point lookup?** Tokenize the term instead of detokenizing the
  column: 1,000,000 rows through the function became **1** (§5, §6 pattern F).
  That is the shape most user-facing traffic actually takes, and if the caller
  already holds the token it contains no remote function at all — so it does not
  count against the concurrency limit either.
- **Is the column low-cardinality?** Decode `DISTINCT` tokens and join back:
  500,409 rows became **63** (§6 pattern E).
- **Is it really returning 1M rows to someone?** Then the `LIMIT` belongs below
  the function, not above it (§4 pair 1).

A workload that genuinely needs 300,000,000 plaintexts materialised at once — a
bulk export, a migration, a re-key — is real, and then the sizing above is the
answer. A dashboard is not that. The cheapest 15M rows/s is the one you never
have to serve.

---

## Does this apply to the Protegrity demo?

Mostly yes. The measurements were taken with the FPE service, but almost every
finding is a property of **BigQuery's remote function protocol**, not of what
the service does with a value once it arrives. Anything in this document that
concerns the BigQuery side transfers unchanged to
[`protegrity/`](../protegrity/) — or to any other remote function you write.

**Transfers unchanged** — these are BigQuery-side behaviours:

| Section | Why it transfers |
| --- | --- |
| §1 batch cap (~256 KiB) | BigQuery sizes the request before contacting any service |
| §2 limits (15 MB response, 20 retries, 10 concurrent queries) | Enforced by BigQuery |
| §3 short-circuit batching cliff | A query-planning behaviour |
| §4 call placement | Query planning again |
| §5 search by tokenizing the term | Needs only *deterministic* tokenization, which Protegrity FPE/tokenization data elements are by default |
| §6 access-control patterns | Pure SQL shapes; swap `fpe_decrypt` for `pii_detokenize_fpe_multi` |

**Does not transfer** — §7. But *which way* it fails to transfer depends
entirely on how Protegrity is deployed, and the two modes are opposites.

### Two Protegrity deployments, two profiles

| | Developer Edition (this repo) | Production PEP |
| --- | --- | --- |
| Where the crypto runs | Protegrity's hosted API | **In the container, locally** |
| Keys | never held locally | **fetched once from the ESA, cached in memory** |
| Per-row work | an HTTPS round trip | native-code tokenize/detokenize |
| Bound by | network + vendor rate limit | **CPU** |

This repo implements the first, and that is checkable rather than assumed: the
`appython` session caches only a JWT ([`protector.py:121`](../protegrity/service/appython/protector.py#L121))
and every protect/unprotect posts to `api.developer-edition.protegrity.com`
([`payload_builder.py:27`](../protegrity/service/appython/service/payload_builder.py#L27),
[`request_handler.py:32`](../protegrity/service/appython/service/request_handler.py#L32)).
Nothing is computed locally.

**A production deployment is the opposite**, and is what a real customer runs. A
PEP pulls policy and keys from the ESA once, caches them in memory, and performs
the transformation in-process. There is no per-row network call, so it is a
**CPU workload, like the FPE service** — not the I/O workload the Developer
Edition code implies.

### So is production Protegrity CPU-intensive?

CPU-bound, yes. *Intensive*, probably not — and the distinction decides the
tuning.

The PEP's core is native code, not Python. Our FF3-1 costs ~77 µs/row precisely
because it is pure Python; the same algorithm in C is typically one to two
orders of magnitude cheaper. The `modes` table in §7 is a calibrated ruler for
placing it:

| Per-row cost | Throughput | Regime |
| --- | --- | --- |
| `noop`, 0 µs | 403,000 rows/s | pure transit floor |
| `hmac`, 7 µs | 214,000 rows/s | cheap native-ish crypto |
| `fpe_decrypt`, 118 µs | 26,000 rows/s | pure-Python FF3-1 |

If a production PEP tokenizes in single-digit µs/row — plausible for native
code, though **we have not measured it and cannot** — it lands near the `hmac`
row. That regime is **transit-dominated, not compute-dominated**, which inverts
§7 again, in the opposite direction from the Developer Edition:

- **Worker count matters little.** There is not much CPU to parallelise. The 4x
  vCPU finding is an artefact of expensive Python crypto.
- **Batch size matters more.** Per-request overhead is a large share of a cheap
  request, so the flat 1,000–50,000 curve in §7 would not stay flat.
- **Instance count matters little**; the bottleneck moves to the wire and to
  BigQuery.

**How to settle it in an afternoon.** Deploy the real PEP service with
per-request logging like [`main.py`](../fpe/service/main.py) emits, run any of
the sweeps, and read µs/row off the logs. Compare against the three rows above:
near `hmac` means tune batching, near `fpe_decrypt` means tune workers. Every
BigQuery-side finding in §§1–6 applies either way.

### Developer Edition only

If you *are* running the Developer Edition code in this repo, then it is
I/O-bound and wants `gthread` — and it currently gets neither threads nor
processes. Its Dockerfile runs a bare `gunicorn main:app`
([`Dockerfile:13`](../protegrity/service/Dockerfile#L13)), so gunicorn defaults
to **one sync worker**, handling one request at a time, while the session cache
sits behind a `threading.Lock`
([`main.py:16-38`](../protegrity/service/main.py#L16)) that protects nothing.
Sync workers would also each hold their own session, multiplying login traffic
against a rate-limited API, where threads share one. None of this is measured —
the access needed to test it is gone.

---

## Practical checklist

Ordered by the size of the effect measured here.

1. **Never put a remote function inside `CASE`/`IF`/`MERGE ... WHEN`** (~180x).
   Use `UNION ALL` of unconditional branches, or filter first. Hoisting into a
   subquery does *not* work.
2. **Decode distinct values, not rows**, for low-cardinality columns (7,900x
   fewer rows through the service on a 63-distinct-value column, 14x faster).
3. **Search by tokenizing the term**, bound via `DECLARE`/`SET` (~9x, and
   960,000 → 1 row through the function). Through an authorized view, project
   the token column so lookups can filter on ciphertext (§6 pattern F: 11x,
   1,000,000 → 1 row). Better still, have the caller supply an already-known
   token: that query contains no remote function at all, so it escapes the
   10-concurrent-query limit entirely.
4. **Scale horizontally.** `maxScale` 1→8 gave 3.2x; it beats vCPU per unit of
   effort and avoids worker oversubscription.
5. **Sweep gunicorn workers; do not assume workers = vCPU.** That rule
   under-provisioned this service by 1.29x — 16 workers on 4 vCPU beat 4, with
   non-overlapping run ranges, because AES-in-C and socket I/O take processes
   off-core. Threads are still no substitute: 3.3x *worse* per row than
   processes. `containerConcurrency` only needs to be **>= workers** so nothing
   starves; above that it made no measurable difference, so leave it at the
   default.
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
python fpe/scripts/analyze.py fpe/results/sweep_raw_*.jsonl
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
