# Plan: Cloud Run scaling behaviour as a function of workload characteristics

**Status:** not started. Written as a handoff for a fresh session.
**Goal:** replace "here is what worked for *our* workload" with "measure these
two numbers about your workload, look up the config".

---

## 1. Why this is needed

[`docs/performance-tuning.md`](../performance-tuning.md) §7 tunes Cloud Run for
exactly **one** workload point: FF3-1 in pure Python, ~77 µs/row, CPU-bound. Its
conclusions are correct there and demonstrably do not generalise:

- "workers = vCPU" was measured **wrong by 1.29x** for that workload (16 workers
  on 4 vCPU won), because AES-in-C and socket I/O take processes off-core.
- For a **production Protegrity PEP** — keys cached in memory, native-code
  crypto, likely single-digit µs/row — the workload is probably
  *transit-dominated*, which inverts §7 again in the opposite direction: batch
  size would start to matter and worker count stop.
- For the **Developer Edition** code in this repo, every row is a remote HTTPS
  call, so it is I/O-bound and wants `gthread`.

Three deployments of the same conceptual service, three different right answers.
The document currently tells a reader to "measure it for your own workload"
without telling them what to measure or how to map the answer to a config. This
plan closes that.

**Deliverable:** a decision guide — measure per-row cost and CPU share, look up
`worker_class` / `workers` / `threads` / `containerConcurrency` / `cpu` /
`maxScale`.

---

## 2. Orientation for a fresh session

Read first, in order:
1. [`README.md`](../../README.md) — repo layout, the two demos.
2. [`docs/performance-tuning.md`](../performance-tuning.md) §7 and the
   "Does this apply to the Protegrity demo?" section — the state of the art and
   its limits.
3. [`fpe/scripts/sweep.py`](../../fpe/scripts/sweep.py) — the harness. Phases in
   `PHASES`, one-off probes in `PROBES`.
4. [`fpe/service/main.py`](../../fpe/service/main.py) — the service and its modes.

### The instrument already exists

The FPE service is a **programmable workload generator**, which is the whole
reason this study is cheap. Existing modes and their measured per-row cost:

| Mode | Per-row cost | Models |
| --- | --- | --- |
| `noop` | ~0 µs (403,000 rows/s) | pure transit floor |
| `hmac` | ~7 µs (214,000 rows/s) | cheap native-ish crypto |
| `cpu` | `rounds` sha256 iterations — **dial** | arbitrary CPU cost |
| `fpe_decrypt` | ~118 µs (26,000 rows/s) | expensive pure-Python crypto |
| `io` | `sleep_ms` **once per batch** | one bulk downstream call |

Do not build a new service. Extend these.

### Environment

```bash
pyenv local bq-cloud-run-fpe                 # already created
source ~/.bashrc                             # provides the `gc` wrapper
gc admin--kenly-demo-1 <command>             # ALL gcloud/bq/python that touch GCP
```

- Project `kenly-demo-1`, region `us-central1`, dataset `fpe_perf_demo`.
- Config contract is `config/shared.env` + gitignored `config/shared.env.local`.
- The service is deployed at **`minScale=0`**, so it is cold. Warm it before any
  measurement or the first data point absorbs a cold start.
- **The GitHub repo is public.** Scrub `kenly-demo-1`, the project number, the
  `run.app` hostname and any email from every file before committing — see the
  scrub snippets in recent git history.

---

## 3. Methodology traps — all of these were hit the hard way

Read this section before designing anything. Each cost real time.

1. **Noise is larger than you expect.** At 2 iterations, two runs of the *same*
   config differed by up to **2.16x**. An entire concurrency sweep from 2 to 80
   spanned only 1.41x and was therefore uninterpretable. **Always report
   min/median/max and check whether ranges overlap before claiming a
   difference.** Use ≥5 iterations for anything subtle; 3 was barely enough to
   separate 16 workers from 4.
2. **Never vary two knobs at once.** The original `concurrency` phase moved
   concurrency and workers together and could answer neither question. The
   isolated re-runs (`concurrency_only`, `workers_only`) are the model to copy.
3. **`maxScale=1` when isolating per-instance settings.** Otherwise a low
   concurrency just makes Cloud Run add instances and you measure the autoscaler.
4. **`LOG_SETTLE_S = 20`.** Cloud Logging ingestion lag silently reported "0
   requests" for a real single-request run at 8s. A zero in a raw record means
   *not observed*, never *did not happen*.
5. **Warm before measuring** (see `minScale=0` above).
6. **`REPEAT()` has an output cap** below 5 MiB; build wide strings by
   concatenating 1 MB chunks.
7. **Memory scales with worker count.** Each gunicorn worker is a full
   interpreter with ff3/pycryptodome (~70 MB RSS). 32 workers needed 16Gi; too
   little memory means OOM kills, which measures the wrong thing.
8. **Deploys dominate wall-clock**, ~60–90s each. Budget by deploy count, not
   query count.

---

## 4. Work

### Phase 0 — extend the instrument (no GCP sweeps yet)

**0a. Calibrate `cpu` mode.** Build a `rounds` → µs/row curve so a target per-row
cost can be dialled. Do this locally first (`python -c` against
`fpe/service/main.py`'s `_cpu_burn`), then confirm one point on Cloud Run, since
container CPU differs from the workstation.
Deliverable: a `ROUNDS_FOR_US` lookup in `sweep.py`.

**0b. Add `io_row` mode** — sleep per *row* rather than per batch, modelling a
per-row remote call (the Developer Edition shape). Keep the existing per-batch
`io`; they model genuinely different architectures and both are needed.

**0c. Add `mixed` mode** — `cpu_us` of CPU **plus** `io_ms` of sleep per row, so
the CPU/I-O plane can be spanned rather than just its two axes. This is the mode
that makes the whole study possible; without it there is no way to sit a
workload at, say, 70% CPU / 30% wait.

**0d. Decide the statistical protocol** and write it into the harness: iteration
count, whether to interleave configs to cancel drift, and an automatic
"ranges overlap → report as indistinguishable" flag in `analyze.py`. Doing this
once here prevents re-litigating every result.

### Phase 1 — the workload × worker-model matrix (the core experiment)

Fixed: `cpu=4`, `containerConcurrency=80`, `maxScale=1`, `minScale=1`,
400k rows, batch 5000.

Workload points (via Phase 0 modes):

| Id | Workload | Represents |
| --- | --- | --- |
| W1 | `noop` | transit floor |
| W2 | `mixed` 5 µs CPU | **production Protegrity PEP** (native crypto) |
| W3 | `mixed` 30 µs CPU | moderate transform |
| W4 | `fpe_decrypt` ~118 µs | today's baseline |
| W5 | `io_row` 20 ms | **Developer Edition** / any per-row remote call |
| W6 | `mixed` 20 µs CPU + 20 ms I/O | realistic hybrid |

Worker models per workload: `sync` with workers ∈ {1, 2, 4, 8, 16, 32} and
`gthread` with (workers, threads) ∈ {(1,8), (1,32), (2,16), (4,16)}.

That is 6 × 10 = 60 deploys ≈ 2–3 hours if run unattended. **Trim before
running** — W1 and W4 are partly known, and the gthread arm can be skipped for
pure-CPU points once the pattern is clear. Prefer running W2 and W5 first: they
are the two that map onto real customer deployments.

**Record per cell:** rows/s (min/median/max), µs/row from service logs, peak
in-flight, worker processes observed.

### Phase 2 — derive and falsify a rule

Hypothesis to test: optimal concurrent slots ≈ `cores × (1 + wait/service)`.
Our one data point fits loosely — peak at 16 slots on 4 cores implies wait ≈ 3 ×
service, plausible for AES-in-C plus socket time.

- Fit the rule to Phase 1.
- **Then falsify it:** predict the optimum for a workload point *not* in the fit
  set, run only that config plus its neighbours, and check the prediction lands
  inside the measured optimum's range. A rule that was only ever fitted is not a
  finding.

### Phase 3 — the scaling axis

For each workload's best per-instance config from Phase 1:
- sweep `cpu` ∈ {1, 2, 4, 8} with workers rescaled by the Phase 2 rule;
- sweep `maxScale` ∈ {1, 2, 4, 8}.

Question to answer: **at which workload characteristics does horizontal beat
vertical?** §7 found horizontal stronger (3.2x vs 3.1x) for W4 only. Expect the
answer to flip for transit-dominated workloads, where neither helps much and
batch size takes over.

Also re-run the **batch-size** sweep at W2. §7 found batch size irrelevant above
1,000 rows, but that was measured where compute dominated. For a cheap workload
the per-request overhead share is much larger and the curve should not be flat —
if it is, that is itself worth knowing.

### Phase 4 — the deliverable

New section in `docs/performance-tuning.md` (or a sibling doc if it gets long),
containing:

1. **A measurement recipe.** "Deploy with per-request logging, run one query,
   read µs/row and the CPU share off the logs." Two numbers, nothing else.
2. **A decision table** keyed on those two numbers:

   | µs/row | CPU share | worker_class | workers | threads | concurrency | scale first |
   | --- | --- | --- | --- | --- | --- | --- |
   | … | … | … | … | … | … | … |

3. **The reasoning**, so a reader can extrapolate off the table's edges.
4. **Honest error bars** — say which cells are measured and which interpolated.

Then update the cross-references: §7's "measure this for your own workload"
should point at the guide, and the Protegrity section's open question about
where a production PEP lands should be answerable by it.

---

## 5. Budget and cost

- Phase 0: no GCP sweeps, ~1 hour.
- Phase 1: 60 deploys worst case, ~3 hours; trimmed ~30 deploys, ~1.5 hours.
- Phase 3: ~30 deploys, ~1.5 hours.
- Cloud Run cost is small (4 vCPU, minScale=1 during runs); BigQuery scans
  ~13 MB per query. The real cost is wall-clock.
- **Set `minScale=0` when finished**, as the current state does.

---

## 6. Definition of done

- [ ] A reader with an unmeasured workload can pick a Cloud Run config from the
      guide using two numbers they can obtain in one query.
- [ ] Every recommendation cites a measurement or is explicitly marked as
      interpolated.
- [ ] The Phase 2 rule survived a falsification attempt on a held-out point.
- [ ] No claimed difference rests on ranges that overlap.
- [ ] `docs/performance-tuning.md` §7 no longer implies its numbers generalise.
- [ ] Raw JSONL committed under `fpe/results/`, identifiers scrubbed.

## 7. Open questions worth resolving on the way

- Does `containerConcurrency` stay a no-op above the floor for *cheap*
  workloads? It was noise-dominated at W4; at W1/W2 per-request overhead is a
  bigger share and it may finally matter.
- Does Cloud Run's CPU throttling behave differently for bursty cheap requests?
  §7 found no effect at W4 and the two numbers ran the wrong way round.
- Is there a memory-per-worker ceiling worth documenting alongside the worker
  count recommendation?
