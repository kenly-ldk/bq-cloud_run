# Sizing Cloud Run for a BigQuery remote function

[`performance-tuning.md`](performance-tuning.md) §7 tunes Cloud Run for exactly
one workload: FF3-1 in pure Python, ~87 µs/row, entirely CPU-bound. Its
conclusions are correct there and **do not generalise** — the same service made
20x cheaper per row wants a different worker count, and made I/O-bound wants a
different worker *model* entirely.

This guide replaces "here is what worked for our workload" with: **measure two
numbers about your workload, look up the configuration.**

Everything below was measured with
[`fpe/scripts/sweep.py`](../fpe/scripts/sweep.py) against seven workload points
spanning 0.1 µs/row to 2,100 µs/row and CPU shares from 1.00 to 0.012. Raw
records are in [`fpe/results/`](../fpe/results/); the generated tables are
[`scaling-tables.md`](../fpe/results/scaling-tables.md).

---

## 1. The measurement recipe

Two numbers, one query.

**Step 1 — log them.** Your service needs to time each batch on two clocks. The
FPE service does this in [`main.py`](../fpe/service/main.py); it is six lines:

```python
t_work = time.perf_counter()
c_work = time.thread_time()      # NOT process_time: threads share a process
...do the work for this batch...
work_ms = (time.perf_counter() - t_work) * 1000
cpu_ms  = (time.thread_time()  - c_work) * 1000

print(json.dumps({
    "us_per_row":     work_ms * 1000 / n_rows,
    "cpu_us_per_row": cpu_ms  * 1000 / n_rows,
    "cpu_share":      cpu_ms / work_ms,
}))
```

`thread_time`, not `process_time`: under a threaded worker several requests
share one process, and `process_time` would charge each of them for all the
others' CPU.

**Step 2 — profile at one slot.** Deploy **one worker, `containerConcurrency:
1`**, and run one modest query. This part matters: as soon as requests overlap,
the thread is descheduled by its neighbours and `cpu_share` measures contention
instead of your workload.

```bash
python fpe/scripts/sweep.py --phase profile      # does exactly this
```

**Step 3 — read the two numbers off the logs.**

| Number | What it is | What it decides |
| --- | --- | --- |
| **µs/row** | wall-clock cost of a row | whether tuning is worth anything, and which scaling axis works |
| **CPU share** | fraction of that spent holding a core | `sync` vs `gthread`, and how many slots |

"Slots" below means **requests the container can execute at once** —
`workers` for `sync`, `workers x threads` for `gthread`. It is not
`containerConcurrency`, which is only an admission limit (§7).

The seven reference points, measured under exactly that recipe:

| Point | Workload | µs/row (wall) | µs/row (CPU) | CPU share | wait/service | Rows/s at 1 slot |
| --- | --- | --- | --- | --- | --- | --- |
| W1 | `noop` — transit floor | 0.1 | 0.1 | 1.00 | 0.0 | 200,200 |
| W2 | 5 µs synthetic CPU — **production PEP shape** | 4.3 | 4.3 | 1.00 | 0.0 | 105,876 |
| W3 | 30 µs synthetic CPU | 27.9 | 27.9 | 1.00 | 0.0 | 29,214 |
| W4 | **FF3-1, real** — §7's workload | 86.7 | 86.7 | 1.00 | 0.0 | 9,495 |
| W5 | 2 ms remote call per row — **Developer Edition shape** | 2,113.8 | 24.9 | **0.012** | 82.3 | 461 |
| W6 | 100 µs CPU + 1 ms wait | 1,201.2 | 119.6 | **0.100** | 9.0 | 811 |
| W7 | 50 µs CPU + 1 ms wait *(held out)* | 1,164.6 | 83.4 | **0.072** | 12.9 | 818 |

---

## 2. The decision table

4 vCPU per instance. `containerConcurrency` at Cloud Run's default of 80
throughout, except where slots exceed it.

| CPU share | µs/row | `worker_class` | workers | threads | slots | `containerConcurrency` | Scale first | Basis |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| **≥ 0.9** | < 1 | `sync` | 4 *(= vCPU)* | 1 | 4 | 80 | vertical | W1, measured |
| **≥ 0.9** | 1 – 10 | `sync` | **16** *(4x vCPU)* | 1 | 16 | 80 | **vertical** | W2, measured |
| **≥ 0.9** | 10 – 50 | `sync` | 4 – 16 | 1 | 4 – 16 | 80 | vertical | W3, measured *(4 already optimal)* |
| **≥ 0.9** | 50 – 200 | `sync` | **16** *(4x vCPU)* | 1 | 16 | 80 | **horizontal** | W4, §7 |
| **≥ 0.9** | > 200 | `sync` | 16 | 1 | 16 | 80 | horizontal | interpolated from W4 |
| **0.05 – 0.5** | any | `sync` | **32** *(8x vCPU)* | 1 | 32 | 80 | vertical | W6, tested only to 64 slots |
| **< 0.05** | any | **`gthread`** | 4 | **16 – 32** | **64 – 128** | 2x slots | vertical | W5, W7, measured |

Two things to notice before using it.

**The CPU-share column does more work than the µs/row column.** Every row with
share ≥ 0.9 wants `sync` and 4–16 slots. The row below 0.05 wants `gthread` and
64–128. µs/row only shifts things within a regime, and mostly decides *which
scaling axis works* rather than the worker model.

**The bottom row is the only one where threads are right** — and it is the row
the Developer Edition sits in.

---

## 3. Why — with the measurements

### 3.1 Threads vs processes is decided by CPU share, and by nothing else

At **identical slot counts**, so the only difference is processes versus
threads:

| Workload | CPU share | Slots | `sync` rows/s | `gthread` rows/s | Processes win by |
| --- | --- | --- | --- | --- | --- |
| W2 | 1.00 | 32 | **392,645** | 100,365 *(1x32)* | **3.9x** |
| W3 | 1.00 | 32 | **124,671** | 49,584 *(1x32)* | **2.5x** |
| W6 | 0.10 | 32 | **16,688** | 6,298 *(1x32)* | **2.6x** |
| W5 | 0.012 | 8 | 3,718 | 3,700 *(1x8)* | **1.00x — a tie** |
| W5 | 0.012 | 32 | 9,853 | 9,736 *(1x32)* | **1.01x — a tie** |

The GIL explains all five rows. A thread that is computing holds the GIL and
blocks its siblings, so threads cannot parallelise CPU work; a thread blocked on
a socket has released it, so threads parallelise waiting perfectly.

**The tie is the useful result, because the two sides do not cost the same.**
32 `sync` workers are 32 Python interpreters with `ff3` and `pycryptodome`
resident, and needed **16Gi**; 32 threads in one process ran in **2Gi**. Same
throughput, one eighth of the memory. For a wait-bound service, threads are
free capacity.

Note W6 at CPU share 0.10: still firmly a `sync` workload. The crossover is not
at "mostly waiting" — it is much closer to "almost entirely waiting". Somewhere
between share 0.10 and 0.012, and this study does not resolve where.

### 3.2 How many slots — the rule, and how far to trust it

The rule under test was `slots = cores x (1 + wait/service)`, against the
measured knee (the *fewest* slots statistically indistinguishable from the best
configuration):

| Workload | wait/service | Rule predicts | Measured knee | Rule is |
| --- | --- | --- | --- | --- |
| W1 | 0.0 | 4 | 4 | exact |
| W2 | 0.0 | 4 | **16** | 4x low |
| W3 | 0.0 | 4 | 4 | exact |
| W4 | 0.0 | 4 | **16** | 4x low |
| W6 | 9.0 | 40 | 32 | close |
| W7 | 12.9 | 56 | 96 | ~2x low |
| W5 | 82.3 | 333 | ≥ 64 *(not tested higher)* | untested |

**So the rule is the right shape and an unreliable point predictor.** It gets
the order of magnitude right — it correctly separates the 4-slot workloads from
the 100-slot ones — and it is wrong by up to 4x on individual points. Use it to
pick a bracket, then sweep within it. Do not use it to pick a number.

Where it fails on the pure-CPU points, the reason is that "wait" is not only
what you wrote. W2 has *no* sleep at all and still wants 16 slots on 4 cores,
because a request also parses JSON, allocates, and reads and writes a socket,
and the socket part is off-core. §7 reached the same conclusion for W4 from one
data point; this reproduces it at a per-row cost 20x cheaper.

### 3.3 The falsification test

W7 was held out of everything above, and a prediction was
[written down before it was run](plans/cloud-run-scaling-decision-guide.md).
Two rules were on trial:

- the **plan's rule**, `4 x (1 + 20) = 84` slots from W7's nominal parameters;
- a **capped variant** the Phase 1 data seemed to support: reconstructed peak
  concurrency never exceeded 32 in any of 39 cells, so slots beyond ~32 looked
  unfillable — predicting **32**.

Measured:

| Slots | Rows/s (median) | Range | Verdict |
| --- | --- | --- | --- |
| 8 | 5,371 | 5,149 – 5,452 | slower |
| 16 | 6,696 | 5,337 – 6,779 | slower |
| 32 | 10,387 | 7,792 – 17,012 | slower |
| 64 | 12,098 | 11,727 – 20,440 | slower |
| **96** | 12,847 | 12,660 – **22,827** | **overlaps the best** |
| **128** | **23,312** | 22,485 – 23,857 | **best** |
| 256 | 17,261 | 12,386 – 21,607 | slower |

- **The capped variant is falsified outright.** 32 slots is 2.2x slower than
  128, with disjoint ranges. Peak concurrency did stay pinned near 30 all the
  way to 256 slots — but throughput kept climbing anyway, so that ceiling was
  never what bound throughput. *Mean* concurrency, which rose 11.2 → 22.8,
  tracked it. A metric that saturates is not automatically the constraint.
- **The plan's rule survives, at the edge.** Its answer of 84 lands between 64
  (measurably slower) and 96 (indistinguishable from the best). The plan's
  criterion was that the prediction fall inside the measured optimum's range,
  and 96 — the nearest tested point, 14% above the prediction — does.

The honest caveat: the 32/64/96/256 cells have spreads of 1.7–2.2x, so this
region is poorly resolved and the true optimum lies somewhere in 96–200. The one
crisp finding is that 128 slots beat everything else with a tight 1.06x spread.

### 3.4 Vertical vs horizontal — §7's answer inverts

§7 found horizontal scaling the stronger lever (3.2x from `maxScale` 1→8,
against 3.1x for 8x the vCPU). **For cheap or wait-bound workloads horizontal
scaling does nothing at all:**

| Workload | Vertical, 1→8 vCPU | Horizontal, `maxScale` 1→8 | Instances Cloud Run actually ran |
| --- | --- | --- | --- |
| W2 (4.3 µs/row) | 152,290 → **581,732** = **3.8x** | 394,688 → 348,209 — every point overlaps | **1** |
| W5 (2,114 µs/row, share 0.012) | 4,722 → **12,832** = **2.7x** | 11,657 → 11,644 — every point overlaps | **1** |
| W4 (87 µs/row) | 3.1x *(§7)* | **3.2x** *(§7)* | up to 5 *(§7)* |

The cause is in the last column. Cloud Run adds instances when requests queue.
BigQuery offers on the order of 20–30 concurrent requests to one endpoint for a
single query; a 16-slot instance absorbs that without a queue forming, so the
autoscaler never fires and `maxScale` is inert. At W4's 87 µs/row each request
takes ~0.6 s, requests do queue, and the autoscaler engages.

> **Horizontal scaling is not a throughput knob. It is a queueing relief valve.**
> If one instance can absorb the concurrency your client offers, raising
> `maxScale` changes nothing — and it costs nothing either, so leave it at the
> default as insurance rather than tuning it.

### 3.5 Batch size matters again once rows are cheap

§7 measured `max_batching_rows` as "close to a non-knob" above ~1,000. That was
at 87 µs/row, where a 5,000-row request carries 0.4 s of compute and
per-request overhead is invisible. At W2's 4.3 µs/row the same request is 21 ms:

Both columns are normalised to their own best cell.

| `max_batching_rows` | W2 rows/s (4.3 µs/row) | vs best | W4 rows/s (§7, 87 µs/row) | vs best |
| --- | --- | --- | --- | --- |
| 100 | **56,742** | **0.18x** | 21,401 | 0.67x |
| 500 | 203,246 | 0.64x | 14,268 | 0.44x |
| 1,000 | 256,978 | 0.80x | 29,068 | 0.91x |
| 2,500 | 222,174 | 0.69x | **32,093** | 1.00x |
| 5,000 | 270,718 | 0.85x | 29,108 | 0.91x |
| 10,000 | 279,412 | 0.87x | 31,416 | 0.98x |
| 25,000 | 276,776 | 0.87x | 28,007 | 0.87x |
| 50,000 | **319,735** | 1.00x | 28,655 | 0.89x |

Two differences from §7, one bigger than the other.

**Small batches hurt 3.8x more.** Dropping to 100 rows/request costs **5.63x**
at W2 against **1.50x** at W4. At 87 µs/row a 100-row request still carries
8.7 ms of compute to hide the per-request overhead behind; at 4.3 µs/row it
carries 0.43 ms and the overhead is most of the request.

**And the top of the curve is no longer flat.** At W2, 50,000 rows/request beats
1,000 with **non-overlapping ranges** — 298,792–330,305 against 219,800–273,287,
a real 1.24x. §7 measured that same span as flat. The gain is modest and it
saturates against the ~11,905-row request cap of §1 well before 50,000 is
honoured, so:

> Do not tune `max_batching_rows` *upward* — the ~256 KiB request-body budget
> caps it near 11,905 rows anyway, and everything from 5,000 up is within noise
> of the best. Do make sure nothing has driven it *down*: at cheap per-row costs
> that hurts nearly four times as much as §7's numbers imply, and the batching
> cliff of §3 — which drops it to one row per request — is correspondingly
> worse.

### 3.6 A correction to §7's transit floor

§7 reports the BigQuery↔Cloud Run transit floor as 403,000 rows/s and concludes
"~94% of end-to-end time is compute, not network." The floor is higher:

| Measurement | Rows/query | Workers | Rows/s |
| --- | --- | --- | --- |
| §7 `modes` phase | 500,000 | 4 | 403,075 |
| This study, W1 | 15,311,200 | 4 | **959,108** |
| This study, W1 | 20,000,000 | 16 | 842,318 |

At 959,000 rows/s a 500,000-row query is half a second, so §7's figure was
mostly measuring BigQuery's fixed query overhead. The direction of §7's
conclusion holds — FF3-1 at 9,495 rows/s per slot is nowhere near the wire —
but the floor is ~2.4x further away than stated, which matters for a production
PEP at 4 µs/row running within a factor of 2 of it.

---

## 4. What this means for the two Protegrity deployments

§7's "Does this apply to the Protegrity demo?" section poses a question it could
not answer. It can now be answered, conditionally on the per-row cost.

**A production PEP** — native crypto, keys cached in memory — was estimated at
single-digit µs/row. That is W2. §7 speculated that regime would be
*transit-dominated*, so that "worker count matters little" and "batch size
matters more". Measured:

| §7's expectation for a production PEP | Measured at W2 |
| --- | --- |
| Worker count matters little | **Wrong.** 1 → 16 workers is 180,158 → 417,034 rows/s, a **2.3x** gain with disjoint ranges. Worker count matters as much as it does for FF3-1. |
| Batch size matters more | **Right**, in both directions: dropping to 100 rows/request costs 5.63x rather than FF3-1's 1.50x, and 50,000 rows/request is a real 1.24x over 1,000 where §7 measured that span as flat. |
| Instance count matters little | **Right**, and more strongly than expected: `maxScale` is completely inert and Cloud Run never leaves one instance. |

So a production PEP should be configured **exactly like the FF3-1 service** —
`sync`, ~4x vCPU workers, `containerConcurrency` 80 — and should scale
**vertically**, not horizontally. What changes is only where the throughput
lands: ~417,000 rows/s per 4-vCPU instance instead of ~35,000.

**The Developer Edition** in this repo posts every row to a remote HTTPS API. Its
CPU share will be near W5's 0.012, and it currently runs a bare
`gunicorn main:app` — **one sync worker, one slot**. That is the worst cell in
the entire study: W5 at one slot is 466 rows/s against 11,602 at 64, a **25x**
difference. The fix is one line in its Dockerfile:

```dockerfile
CMD ["gunicorn", "--worker-class", "gthread", "--workers", "4", "--threads", "16", \
     "--timeout", "600", "main:app"]
```

Threads rather than processes for two independent reasons: they are
throughput-equivalent here (§3.1) at one eighth of the memory, and sync workers
would each hold their own vendor session, multiplying login traffic against a
rate-limited API where threads share one.

---

## 5. Error bars

**Measured, with non-overlapping ranges:** the `sync`/`gthread` split by CPU
share (§3.1); W2's 2.3x from worker count; W5's 25x from slot count; W7's
128-slot optimum; both vertical arms; the batch-size penalty at 100 rows and the
1.24x gain from 1,000 to 50,000 rows at W2; the transit floor correction.

**Measured, but ranges overlap — read as "no measurable difference":** every
`maxScale` comparison; W3's entire worker sweep (4, 16 and 32 slots are
indistinguishable); W2 at 16 vs 32 slots; W6 at 32 vs 64 slots.

**Interpolated, not measured:**

- The `> 200 µs/row, share ≥ 0.9` row of the decision table. Nothing was
  measured above 87 µs/row at a CPU share of 1.0; it is extrapolated from W4.
- The share-0.05–0.5 row rests on W6 and W7 alone, and **W6 was never tested
  above 64 slots**, so its "32 slots" entry is the best of what was run, not a
  located optimum.
- Where the `sync`→`gthread` crossover sits. Somewhere between CPU share 0.10
  (W6, still wants processes) and 0.012 (W5, indifferent). Untested.
- W5's slot optimum. 64 was the highest tested; W7's behaviour up to 128
  suggests W5 has not plateaued either.

**Protocol.** 5 iterations per cell, min/median/max reported, and no difference
claimed while ranges overlap. Each phase re-ran its first configuration at the
end as a drift check; three of four showed no drift. The fourth — W5's vertical
arm — drifted 4.5% (4,722 → 4,934, ranges just disjoint), far below the 2.7x
effect that arm reports, so its conclusion stands but its lowest point is soft.

**Scope.** Single project, single region, one instance size for most of the
study (4 vCPU), narrow STRING columns, one argument, and synthetic CPU
(iterated SHA-256) standing in for real crypto. The `mixed` and `io_row` modes
model a *blocking* per-row call; a service using async I/O or connection pooling
would behave differently. Per-row costs were dialled through a calibrated
container curve (µs/row = 1.09 + 1.0879 x rounds), re-measured on Cloud Run
because the workstation's curve was **2.33x** off.

---

## 6. Reproducing

```bash
python fpe/scripts/sweep.py --phase calibrate_cpu   # container rounds -> µs/row
python fpe/scripts/sweep.py --phase profile         # the two numbers, per workload
python fpe/scripts/sweep.py --phase workload_matrix --iterations 5 --adaptive
python fpe/scripts/sweep.py --phase rule_check --workload W7 --slots 8,16,32,64,96,128,256
python fpe/scripts/sweep.py --phase scale_axis --workload W2 --workers 16 --worker-class sync
python fpe/scripts/sweep.py --phase batch_at_cheap --workload W2 --workers 16
python fpe/scripts/analyze.py fpe/results/sweep_raw_study_*.jsonl
```

`--adaptive` is not optional for the matrix: its cells differ in throughput by
over 2,000x, and no fixed row count gives all of them a measurable run. Each
cell is piloted and sized to ~25 s of work. Budget ~5 minutes per cell; a Cloud
Run deploy is ~75 s of it.
