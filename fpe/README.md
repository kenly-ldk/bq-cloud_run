# FPE Demo — BigQuery Remote Function on Cloud Run, no vendor dependency

Format-preserving encryption (NIST SP 800-38G **FF3-1**) executed in-process by
a Flask service on Cloud Run, called from BigQuery through a remote function.
No external API, no licence, no credentials beyond your own GCP project.

It doubles as the measurement rig for
[`docs/performance-tuning.md`](../docs/performance-tuning.md): the same service
exposes synthetic workloads whose cost you control, which is what makes it
possible to separate network transit from compute from queueing.

## What it does to a value

Only characters in the data element's alphabet are encrypted. Everything else
is structural and stays exactly where it was, so ciphertext keeps the shape of
plaintext:

```text
ssn    123-45-6789               ->  516-91-2276               (dashes preserved)
email  john.smith.42@example.com ->  0u3w.ylwumnik@example.com (domain preserved)
name   John Smith                ->  Kpwx Adjqr                (space preserved)
```

FF3-1 requires `radix^len >= 1,000,000`, so values whose encryptable payload is
shorter than that floor are **passed through unchanged** and counted in
`PASSTHROUGH_COUNTER` (visible at `/stats`). Documented behaviour, not silent
corruption — but it does mean very short values are not protected.

`FPE_KEY` (32/48/64 hex) and `FPE_TWEAK` (14 hex) come from the environment.
The committed default is a demo key; put a real one in `shared.env.local`, or
better, Secret Manager.

## Modes

One service backs every remote function. The function's `user_defined_context`
picks the mode:

| Mode | Work per row | Purpose |
| --- | --- | --- |
| `fpe_encrypt` / `fpe_decrypt` | FF3-1, ~77 µs | The real demo workload; CPU-bound |
| `hmac` | HMAC-SHA256, ~2 µs | Cheap deterministic tokenization |
| `noop` | echo | Pure network-transit floor |
| `cpu` | N sha256 rounds | Synthetic CPU knob |
| `io` | sleep once per batch | Synthetic I/O knob (models a bulk downstream call) |
| `bloat` | pad reply to `width` | Probes the 15 MB response ceiling |
| `error` | fail `fail_pct` of batches | Probes BigQuery's retry behaviour |

Every request emits one structured JSON log line with pid, thread, batch size,
in-flight concurrency and timings. Cloud Monitoring tells you how many
*instances* ran; these logs tell you what happened *inside* each one.

## Tuning knobs

All are env vars on the Cloud Run revision, so a sweep changes behaviour by
redeploying the same image:

| Variable | Effect |
| --- | --- |
| `FPE_CPU`, `FPE_MEMORY` | Instance size |
| `FPE_CONCURRENCY` | `containerConcurrency` — requests *admitted* per instance |
| `FPE_WORKERS` | gunicorn processes — requests *executed in parallel* |
| `FPE_THREADS`, `FPE_WORKER_CLASS` | Thread model; only helps GIL-releasing work |
| `FPE_MIN_INSTANCES`, `FPE_MAX_INSTANCES` | Autoscaling bounds |
| `FPE_CPU_THROTTLING` | `false` = CPU always allocated |

The distinction between `FPE_CONCURRENCY` and `FPE_WORKERS` is the single most
consequential thing in this repo — see the study. Short version: concurrency is
an admission gate that only needs to exceed the worker count, while the worker
count is the real lever, and its best value was measured at 4x vCPU rather than
the textbook 1x.

## Setup

```bash
./scripts/provision.sh              # APIs, AR repo, dataset, connection, IAM
./scripts/build.sh                  # Cloud Build -> Artifact Registry
./scripts/deploy.sh                 # render Knative manifest -> Cloud Run
python scripts/generate_remote_functions.py --apply
./scripts/setup_data.sh             # tokenized table + roundtrip verification
./scripts/setup_access_control.sh   # entitlement table + authorized views
```

Local tests, no GCP needed:

```bash
python service/test_fpe_engine.py
```

## Access-control patterns

[`sql/access_control_patterns.sql`](sql/access_control_patterns.sql) implements
row- and column-level access control over already-tokenized data using
authorized views plus an entitlement lookup table — and shows why the obvious
way to write it is ~30x slower than the equivalent correct way. Benchmarked by
`sweep.py --phase access_control`, which also asserts the fast shapes return
byte-identical results to the naive ones.

## Benchmarking

```bash
python scripts/sweep.py --list                    # show the plan, deploy nothing
python scripts/sweep.py --phase batch             # max_batching_rows
python scripts/sweep.py --phase concurrency       # concurrency x worker model
python scripts/sweep.py --phase concurrency_only  # containerConcurrency isolated
python scripts/sweep.py --phase workers_only      # worker count isolated
python scripts/sweep.py --phase limits            # all documented BQ limits
python scripts/sweep.py --phase access_control    # authorized-view shapes
python scripts/analyze.py results/sweep_raw_*.jsonl --out results/tables.md
```

Each phase deploys revisions, runs queries, and records both BigQuery job stats
and the service's own logs. Raw output is JSONL, one record per iteration.
