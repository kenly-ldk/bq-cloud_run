#!/usr/bin/env python3
"""Concurrency and performance sweep for BigQuery remote functions on Cloud Run.

Each sweep step is: deploy a Cloud Run revision with one set of knobs, warm it,
then run BigQuery queries against remote functions with a given
max_batching_rows, and record what happened on both sides.

Two independent measurement sources, deliberately:

  BigQuery job stats  -> what the *user* experiences (elapsed, slot time)
  Cloud Run logs      -> what the *service* did (instances, worker processes,
                         real in-flight concurrency, actual batch sizes)

The second is what makes this more than a stopwatch. BigQuery's
`max_batching_rows` is an upper bound, not a promise; the logs show the batch
sizes actually sent. And containerConcurrency is an admission limit, not a
parallelism guarantee; `inflight` vs distinct pids shows whether requests were
genuinely running in parallel or just queued inside one worker.

Usage:
    python fpe/scripts/sweep.py --phase batch
    python fpe/scripts/sweep.py --phase concurrency
    python fpe/scripts/sweep.py --phase all --rows 500000
    python fpe/scripts/sweep.py --list

Statistical protocol — decided once (decision-guide plan, Phase 0d) so it does
not get re-litigated per result:

  * **5 iterations** for the scaling study. At 2, two runs of the *same* config
    differed by 2.16x, wider than most of the effects being looked for.
  * **min/median/max, and overlap before difference.** `analyze.py` will not
    call one config better than another while their [min, max] ranges overlap.
  * **Adaptive row count.** Cells in this study differ in throughput by over
    100x (one sync worker on a per-row-I/O workload versus 64 threads), so no
    single row count gives every cell a measurable run. Each cell is piloted
    and sized to about `--target-seconds` of work. Throughput is a rate, so
    differently-sized cells stay comparable; the fixed per-query overhead share
    does not, which is why every cell targets the same duration.
  * **No interleaving.** Interleaving configs to cancel drift would multiply
    the deploy count — the dominant cost at ~75 s each — by the iteration
    count. Instead each phase ends with a **drift sentinel**: the phase's first
    config is redeployed and re-run, and the result is flagged if its range no
    longer overlaps the original. That tests exactly the assumption
    interleaving would have protected, for one extra deploy per phase.
  * **One log fetch per cell, not per iteration.** Cloud Logging needs ~20 s to
    settle (LOG_SETTLE_S), which at 5 iterations cost more than the iterations
    did. Iterations run back to back; the window is split by timestamp after.
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import subprocess
import sys
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from config._loader import load, require  # noqa: E402

from calibration import CONTAINER, Curve  # noqa: E402
from workloads import WORKLOADS  # noqa: E402

from google.cloud import bigquery  # noqa: E402

#: CLI overrides that individual phase definitions read, populated in main().
#: Lets a phase be parameterised (which slot counts, which workload) without
#: threading arguments through the PHASES registry.
OPTS: dict = {}


# --------------------------------------------------------------------------
# Sweep definitions
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class Deployment:
    """One Cloud Run revision configuration."""

    label: str
    cpu: int = 2
    memory: str = "2Gi"
    concurrency: int = 8
    workers: int = 2
    threads: int = 1
    worker_class: str = "sync"
    min_instances: int = 1
    max_instances: int = 1
    cpu_throttling: str = "false"

    def env(self) -> dict[str, str]:
        return {
            "FPE_CPU": str(self.cpu),
            "FPE_MEMORY": self.memory,
            "FPE_CONCURRENCY": str(self.concurrency),
            "FPE_WORKERS": str(self.workers),
            "FPE_THREADS": str(self.threads),
            "FPE_WORKER_CLASS": self.worker_class,
            "FPE_MIN_INSTANCES": str(self.min_instances),
            "FPE_MAX_INSTANCES": str(self.max_instances),
            "FPE_CPU_THROTTLING": self.cpu_throttling,
            "REVISION_SUFFIX": self.label,
        }

    @property
    def slots(self) -> int:
        """Requests this container can have *executing* at once.

        Not `containerConcurrency`, which is only an admission limit. For
        `sync`, gunicorn runs one request per worker process; for `gthread`,
        `threads` requests per process. This is the quantity the Phase 2 rule
        predicts, so it belongs on the deployment rather than in each phase.
        """
        return self.workers * (self.threads if self.worker_class == "gthread" else 1)


@dataclass(frozen=True)
class Trial:
    """One BigQuery query configuration run against a deployment."""

    mode: str
    batch: int
    column: str = "ssn"
    data_element: str = "ssn"
    #: Explicit remote function name. The mode x batch grid is named
    #: `<mode>_b<batch>`, but the workload points carry their cost parameters in
    #: the name too (`mixed_r4s0_b5000`), so those must be named outright.
    function: str | None = None
    #: Workload id (W1..W7) for the record, when this trial realises one.
    workload: str | None = None
    #: Fixed row count. None means size the run adaptively to --target-seconds.
    rows: int | None = None

    @property
    def name(self) -> str:
        return self.function or f"{self.mode}_b{self.batch}"


# The baseline used by every phase that is not varying infrastructure.
# 4 vCPU / 4 sync workers. NOTE: this is a reference point, not the optimum --
# phase `workers_only` measured 16 workers on 4 vCPU as 1.29x faster. Kept as
# the baseline so earlier phases stay comparable.
BASELINE = Deployment(label="baseline", cpu=4, memory="2Gi", concurrency=8, workers=4)


def phase_batch() -> list[tuple[Deployment, list[Trial]]]:
    """Vary max_batching_rows only. Infrastructure held constant."""
    return [
        (
            BASELINE,
            [Trial(mode="fpe_decrypt", batch=b)
             for b in (100, 500, 1000, 2500, 5000, 10000, 25000, 50000)],
        )
    ]


def phase_concurrency() -> list[tuple[Deployment, list[Trial]]]:
    """The headline matrix: containerConcurrency x gunicorn worker model.

    All at cpu=4 so the CPU ceiling is identical and the only thing changing is
    how the service is allowed to use it.
    """
    trials = [Trial(mode="fpe_decrypt", batch=5000)]
    configs = [
        # concurrency=1: one request at a time per instance, whatever the workers
        Deployment(label="c1-w1", cpu=4, concurrency=1, workers=1),
        Deployment(label="c1-w4", cpu=4, concurrency=1, workers=4),
        # concurrency high, single worker: admission without parallelism
        Deployment(label="c8-w1", cpu=4, concurrency=8, workers=1),
        Deployment(label="c16-w1", cpu=4, concurrency=16, workers=1),
        # matched: concurrency and workers both scale
        Deployment(label="c4-w4", cpu=4, concurrency=4, workers=4),
        Deployment(label="c8-w4", cpu=4, concurrency=8, workers=4),
        Deployment(label="c16-w8", cpu=4, concurrency=16, workers=8),
        # oversubscribed: far more workers than CPU
        Deployment(label="c16-w16", cpu=4, concurrency=16, workers=16, memory="4Gi"),
        # threads instead of processes: the GIL test
        Deployment(label="c8-t8-gthread", cpu=4, concurrency=8, workers=1,
                   threads=8, worker_class="gthread"),
        Deployment(label="c8-w2t4-gthread", cpu=4, concurrency=8, workers=2,
                   threads=4, worker_class="gthread"),
    ]
    return [(c, trials) for c in configs]


def phase_concurrency_only() -> list[tuple[Deployment, list[Trial]]]:
    """Isolate containerConcurrency with everything else nailed down.

    The `concurrency` phase varies concurrency and workers together, so the two
    are confounded and it cannot answer "what should containerConcurrency be?".
    This holds cpu=4, workers=4 (matched to vCPU), sync, maxScale=1 and sweeps
    only the admission limit.

    maxScale=1 is essential: with autoscaling on, a low containerConcurrency
    just makes Cloud Run add instances, which measures the autoscaler rather
    than the setting.
    """
    trials = [Trial(mode="fpe_decrypt", batch=5000)]
    return [
        (Deployment(label=f"conconly{c}", cpu=4, memory="2Gi", workers=4,
                    concurrency=c, min_instances=1, max_instances=1), trials)
        # 80 is Cloud Run's own default, worth knowing where it lands.
        for c in (1, 2, 4, 6, 8, 12, 16, 32, 80)
    ]


def phase_workers_only() -> list[tuple[Deployment, list[Trial]]]:
    """Isolate the worker count at Cloud Run's default containerConcurrency.

    Answers the natural objection to the previous phase: if the platform admits
    80 requests, surely 4 workers is too few? For CPU-bound work the answer is
    no — workers are processes contending for cores, and containerConcurrency is
    only an admission gate. This fixes cpu=4, concurrency=80, sync, maxScale=1
    and sweeps workers across and well beyond the vCPU count.

    Memory scales with the worker count because each gunicorn worker is a full
    Python interpreter with ff3/pycryptodome loaded (~70 MB RSS); 32 workers in
    2Gi would be OOM-killed, which would measure the wrong thing.
    """
    trials = [Trial(mode="fpe_decrypt", batch=5000)]
    mem = {1: "2Gi", 2: "2Gi", 4: "2Gi", 8: "4Gi", 16: "8Gi", 32: "16Gi"}
    return [
        (Deployment(label=f"w{n}c80", cpu=4, memory=mem[n], workers=n,
                    concurrency=80, min_instances=1, max_instances=1), trials)
        for n in (1, 2, 4, 8, 16, 32)
    ]


def phase_cpu() -> list[tuple[Deployment, list[Trial]]]:
    """Vertical scaling: does throughput track vCPU when workers match?"""
    trials = [Trial(mode="fpe_decrypt", batch=5000)]
    configs = [
        Deployment(label=f"cpu{n}-w{n}", cpu=n, workers=n,
                   concurrency=2 * n, memory="2Gi" if n <= 4 else "4Gi")
        for n in (1, 2, 4, 8)
    ]
    return [(c, trials) for c in configs]


def phase_scale() -> list[tuple[Deployment, list[Trial]]]:
    """Horizontal scaling: let Cloud Run add instances."""
    trials = [Trial(mode="fpe_decrypt", batch=5000)]
    configs = [
        Deployment(label=f"max{n}", cpu=4, workers=4, concurrency=8,
                   min_instances=1, max_instances=n)
        for n in (1, 2, 4, 8)
    ]
    return [(c, trials) for c in configs]


def phase_modes() -> list[tuple[Deployment, list[Trial]]]:
    """Decompose the cost: transit floor vs cheap compute vs real FPE vs I/O."""
    return [
        (
            BASELINE,
            [
                Trial(mode="noop", batch=5000),
                Trial(mode="hmac", batch=5000),
                Trial(mode="fpe_decrypt", batch=5000),
                Trial(mode="cpu", batch=5000),
                Trial(mode="io", batch=5000),
            ],
        )
    ]


def phase_throttling() -> list[tuple[Deployment, list[Trial]]]:
    """CPU always-allocated vs request-scoped."""
    trials = [Trial(mode="fpe_decrypt", batch=5000)]
    return [
        (Deployment(label="throttle-off", cpu=4, workers=4, concurrency=8,
                    cpu_throttling="false"), trials),
        (Deployment(label="throttle-on", cpu=4, workers=4, concurrency=8,
                    cpu_throttling="true"), trials),
    ]


# --------------------------------------------------------------------------
# The scaling decision guide (docs/plans/cloud-run-scaling-decision-guide.md)
#
# Everything above measures one workload — FF3-1, ~118 µs/row, CPU-bound — and
# its conclusions demonstrably do not generalise. These phases sweep the
# *workload* as well as the config, so the output can be a lookup table rather
# than a war story.
# --------------------------------------------------------------------------

#: Memory per worker *process*. Each is a full interpreter with ff3 and
#: pycryptodome resident, ~70 MB RSS; 32 of them in 2Gi are OOM-killed, which
#: measures the wrong thing. Threads share the interpreter and cost almost
#: nothing by comparison, so this keys on processes only.
MEMORY_FOR_WORKERS = {1: "2Gi", 2: "2Gi", 4: "4Gi", 8: "4Gi", 16: "8Gi", 32: "16Gi"}

#: The worker models under test. `sync` buys parallelism with processes,
#: `gthread` with threads; which one wins is the question, and the answer is a
#: function of the workload, not of the service.
SYNC_ARM = [(n, 1, "sync") for n in (1, 2, 4, 8, 16, 32)]
GTHREAD_ARM = [(1, 8, "gthread"), (1, 32, "gthread"),
               (2, 16, "gthread"), (4, 16, "gthread")]

#: Worker models to run per workload. The full 6 x 10 grid is 60 deploys and
#: ~4.5 hours, and the plan says to trim, so:
#:   W2, W5   full coverage — they are the two that map onto real customer
#:            deployments (production PEP, Developer Edition).
#:   W4       sync arm already measured by `workers_only`; only the gthread arm
#:            is new, so only that is re-run.
#:   W1, W3   enough points to establish the shape, not to resolve an optimum.
#:   W6       full gthread arm, because a hybrid workload is where threads are
#:            expected to win and that claim needs the resolution.
MATRIX_COVERAGE: dict[str, list[tuple[int, int, str]]] = {
    "W1": [(1, 1, "sync"), (4, 1, "sync"), (16, 1, "sync"), (1, 32, "gthread")],
    "W2": SYNC_ARM + GTHREAD_ARM,
    "W3": [(1, 1, "sync"), (4, 1, "sync"), (16, 1, "sync"), (32, 1, "sync"),
           (1, 32, "gthread")],
    "W4": [(1, 32, "gthread"), (4, 16, "gthread")],
    "W5": SYNC_ARM + GTHREAD_ARM,
    "W6": [(1, 1, "sync"), (4, 1, "sync"), (16, 1, "sync"), (32, 1, "sync")]
          + GTHREAD_ARM,
}


def _model_label(workers: int, threads: int, worker_class: str) -> str:
    return f"s{workers}" if worker_class == "sync" else f"g{workers}x{threads}"


def _matrix_deployment(wid: str, workers: int, threads: int, worker_class: str,
                       cpu: int = 4, max_instances: int = 1) -> Deployment:
    """One cell's revision. containerConcurrency is held at Cloud Run's default.

    80 is above every slot count in the matrix (max 64), so it never starves a
    worker — the one thing §7 found containerConcurrency can actually do. It is
    held fixed precisely so it is not a second moving knob.
    """
    return Deployment(
        label=f"{wid.lower()}-{_model_label(workers, threads, worker_class)}",
        cpu=cpu,
        memory=MEMORY_FOR_WORKERS[workers],
        # Twice the slot count so admission never starves the workers (§7's
        # floor rule), but containerConcurrency is capped at 1000 by Cloud Run —
        # `gcloud run services replace` rejects anything above it outright.
        concurrency=min(1000, max(
            80, 2 * workers * (threads if worker_class == "gthread" else 1))),
        workers=workers,
        threads=threads,
        worker_class=worker_class,
        min_instances=1,
        max_instances=max_instances,
    )


def _workload_trial(wid: str, batch: int | None = None) -> Trial:
    w = WORKLOADS[wid]
    if batch is None or batch == w.batch:
        return Trial(mode=w.mode, batch=w.batch, function=w.function, workload=wid)
    return Trial(mode=w.mode, batch=batch,
                 function=f"{w.mode}{w.tag}_b{batch}", workload=wid)


def phase_calibrate_cpu() -> list[tuple[Deployment, list[Trial]]]:
    """Measure the container's `rounds` -> µs/row curve. Phase 0a.

    One worker at containerConcurrency 1, so nothing contends for a core and
    the number is the container's raw single-slot cost. That matters: the one
    pre-existing data point (`cpu` rounds=100 at 122-133 µs/row) was taken with
    4 workers on 4 vCPU and so conflates contention with speed.

    The result is read off the service logs, not off query wall clock, and
    `analyze.py` fits it. Paste the fitted Curve into calibration.CONTAINER.
    """
    from generate_remote_functions import CALIBRATION_BATCH, CALIBRATION_ROUNDS

    cfg = Deployment(label="calib", cpu=4, memory="2Gi", concurrency=1,
                     workers=1, threads=1, worker_class="sync",
                     min_instances=1, max_instances=1)
    return [(cfg, [Trial(mode="cpu", batch=CALIBRATION_BATCH,
                         function=f"cpu_r{r}_b{CALIBRATION_BATCH}",
                         rows=OPTS.get("calibration_rows", 50_000))
                   for r in CALIBRATION_ROUNDS])]


def phase_profile() -> list[tuple[Deployment, list[Trial]]]:
    """The measurement recipe itself, run against every workload point.

    This is what the decision guide asks a reader to do to their *own* service,
    made executable: deploy one worker at containerConcurrency 1, send one
    modest query, and read two numbers off the service logs —

        us_per_row      how expensive a row is
        cpu_share       how much of that is spent holding a core

    One slot and one admitted request matter. With several requests in flight
    the thread is descheduled by its neighbours and `cpu_share` measures
    contention rather than the workload. Small runs, because this is a
    profile, not a throughput measurement.
    """
    cfg = Deployment(label="profile", cpu=4, memory="2Gi", concurrency=1,
                     workers=1, threads=1, worker_class="sync",
                     min_instances=1, max_instances=1)
    # Enough rows to average over many batches, few enough that the per-row
    # I/O points still finish in seconds.
    rows = {"W1": 200_000, "W2": 200_000, "W3": 100_000, "W4": 50_000,
            "W5": 10_000, "W6": 20_000, "W7": 20_000}
    return [(cfg, [
        Trial(mode=WORKLOADS[w].mode, batch=WORKLOADS[w].batch,
              function=WORKLOADS[w].function, workload=w, rows=rows[w])
        for w in WORKLOADS
    ])]


def phase_workload_matrix() -> list[tuple[Deployment, list[Trial]]]:
    """Phase 1: workload x worker model, everything else nailed down.

    Fixed: cpu=4, containerConcurrency=80, maxScale=1, minScale=1, batch 5000.
    Varying: the workload's per-row cost, and how the container is allowed to
    execute requests concurrently. Nothing else.
    """
    only = OPTS.get("workloads") or list(MATRIX_COVERAGE)
    plan = []
    for wid in only:
        for workers, threads, worker_class in MATRIX_COVERAGE[wid]:
            plan.append((_matrix_deployment(wid, workers, threads, worker_class),
                         [_workload_trial(wid)]))
    return plan


def phase_rule_check() -> list[tuple[Deployment, list[Trial]]]:
    """Phase 2: falsify the fitted rule on the held-out workload W7.

    W7 is deliberately absent from MATRIX_COVERAGE. Fit `optimal slots =
    cores x (1 + wait/service)` on Phase 1, predict W7's optimum, then run only
    that config and its neighbours. If the measured optimum's range contains
    the prediction, the rule survived; if it does not, it was curve-fitting.

    Slot counts come from --slots so the prediction can be made *after* seeing
    Phase 1 rather than being baked in here.
    """
    wid = OPTS.get("workload", "W7")
    slots = OPTS.get("slots") or [16, 44, 84, 168]
    plan = []
    for n in slots:
        # Realised as threads on a small number of processes: W7 is
        # wait-dominated, so slots should be cheap, and 168 processes would need
        # more memory than a 4 vCPU instance can have.
        workers = 4 if n >= 16 else 1
        threads = max(1, n // workers)
        plan.append((_matrix_deployment(wid, workers, threads, "gthread"),
                     [_workload_trial(wid)]))
    return plan


def phase_scale_axis() -> list[tuple[Deployment, list[Trial]]]:
    """Phase 3: vertical (vCPU) and horizontal (maxScale) for a given workload.

    Run per workload with --workload and the Phase 1 winner in --workers /
    --threads / --worker-class. The vCPU arm rescales the worker count with the
    vCPU count, because holding workers fixed while vCPU changes measures
    neither: §7's vertical table did exactly that and produced a floor rather
    than a curve.
    """
    wid = OPTS.get("workload", "W2")
    workers = OPTS.get("workers", 4)
    threads = OPTS.get("threads", 1)
    worker_class = OPTS.get("worker_class", "sync")
    base_cpu = 4

    plan = []
    for cpu in (1, 2, 4, 8):
        scaled = max(1, round(workers * cpu / base_cpu))
        cfg = _matrix_deployment(wid, scaled, threads, worker_class, cpu=cpu)
        plan.append((
            Deployment(**{**asdict(cfg),
                          "label": f"{wid.lower()}-cpu{cpu}-"
                                   f"{_model_label(scaled, threads, worker_class)}"}),
            [_workload_trial(wid)],
        ))
    for max_instances in (1, 2, 4, 8):
        cfg = _matrix_deployment(wid, workers, threads, worker_class,
                                 max_instances=max_instances)
        plan.append((
            Deployment(**{**asdict(cfg),
                          "label": f"{wid.lower()}-max{max_instances}"}),
            [_workload_trial(wid)],
        ))
    return plan


def phase_batch_at_cheap() -> list[tuple[Deployment, list[Trial]]]:
    """Phase 3b: re-run the batch-size question at a *cheap* workload.

    §7 found batch size irrelevant above ~1,000 rows, but measured it where a
    request carried 118 µs/row x 5,000 rows = 0.6 s of compute, against which
    per-request overhead is invisible. At W2's 5 µs/row the same request is
    25 ms, so the overhead share is ~25x larger and the curve should not stay
    flat. If it does, that is itself the finding.

    One deployment, eight trials: batch size lives in the function definition,
    so this costs a single deploy.
    """
    wid = OPTS.get("workload", "W2")
    from generate_remote_functions import BATCH_SIZES

    cfg = _matrix_deployment(wid, OPTS.get("workers", 4), OPTS.get("threads", 1),
                             OPTS.get("worker_class", "sync"))
    return [(Deployment(**{**asdict(cfg), "label": f"{wid.lower()}-batch"}),
             [_workload_trial(wid, batch=b) for b in BATCH_SIZES])]


PHASES = {
    "batch": phase_batch,
    "concurrency": phase_concurrency,
    "concurrency_only": phase_concurrency_only,
    "workers_only": phase_workers_only,
    "cpu": phase_cpu,
    "scale": phase_scale,
    "modes": phase_modes,
    "throttling": phase_throttling,
    # The scaling decision guide
    "calibrate_cpu": phase_calibrate_cpu,
    "profile": phase_profile,
    "workload_matrix": phase_workload_matrix,
    "rule_check": phase_rule_check,
    "scale_axis": phase_scale_axis,
    "batch_at_cheap": phase_batch_at_cheap,
}

#: Phases in the scaling study. `--phase all` keeps its original meaning (the
#: §7 sweep) so the existing raw files stay reproducible; these are opted into.
STUDY_PHASES = ["calibrate_cpu", "profile", "workload_matrix", "rule_check",
                "scale_axis", "batch_at_cheap"]


# --------------------------------------------------------------------------
# Limit probes
#
# These test BigQuery's documented remote-function ceilings rather than
# throughput, so they run their own query patterns and treat failures as
# results rather than errors.
#
#   Concurrent queries containing remote functions .. 10 per project
#   Maximum input size (all args, single row) ....... 5 MB
#   HTTP response size (Cloud Run / gen2) ........... 15 MB
#   HTTP invocation time limit (Cloud Run / gen2) ... 20 minutes
#   Maximum HTTP invocation retry attempts .......... 20
#
# Source: https://docs.cloud.google.com/bigquery/quotas#remote_function_limits
# --------------------------------------------------------------------------

#: Deployment used for all limit probes — generous, so that any failure we see
#: is BigQuery's ceiling and not our own under-provisioning.
LIMIT_DEPLOYMENT = Deployment(
    label="limits", cpu=4, memory="4Gi", concurrency=16, workers=4, max_instances=4
)


def probe_concurrent_queries(client, ctx: dict) -> list[dict]:
    """Find the 10-concurrent-remote-function-query ceiling.

    Fires N BigQuery queries simultaneously and records how many succeed. The
    documented limit is 10 per project.

    The queries must be SLOW for this to test anything. An earlier version used
    20k-row queries finishing in ~1.5s; by the time the 16th was submitted the
    first had already retired, so nothing was ever really concurrent and all 16
    "passed". This uses the full row count so each query runs long enough that
    all N genuinely overlap.
    """
    from concurrent.futures import ThreadPoolExecutor

    records: list[dict] = []
    rows = ctx["rows"]
    sql = build_sql(ctx["project"], ctx["dataset"], ctx["table"],
                    Trial(mode="fpe_decrypt", batch=5000), rows)

    for n in (1, 2, 4, 8, 10, 12, 16):
        print(f"    firing {n} concurrent queries ({rows:,} rows each) ...")
        t0 = time.time()
        with ThreadPoolExecutor(max_workers=n) as pool:
            futures = [pool.submit(run_query, client, sql) for _ in range(n)]
            ok, failed, errors = 0, 0, []
            for f in futures:
                try:
                    f.result()
                    ok += 1
                except Exception as exc:  # noqa: BLE001
                    failed += 1
                    errors.append(str(exc)[:200])
        wall = time.time() - t0
        quota_hit = any("too many concurrent queries" in e.lower()
                        or "exceeded rate limits" in e.lower() for e in errors)
        rec = {
            "phase": "limits_queries", "config": LIMIT_DEPLOYMENT.label,
            "probe": "concurrent_queries", "concurrent_queries": n,
            "succeeded": ok, "failed": failed, "wall_s": round(wall, 2),
            "aggregate_rps": round(ok * rows / wall, 0) if wall else 0,
            "quota_error": quota_hit,
            "sample_error": errors[0] if errors else None,
        }
        records.append(rec)
        print(f"      -> {ok}/{n} ok in {wall:.1f}s"
              f"{'  [QUOTA ERROR]' if quota_hit else ''}"
              f"  aggregate {rec['aggregate_rps']:,.0f} rps")
        if failed:
            print(f"         {errors[0][:160]}")
        time.sleep(5)
    return records


#: Rows per HTTP request that BigQuery was measured to cap at, regardless of
#: max_batching_rows. Used to predict response size for the ceiling probe.
OBSERVED_ROW_CAP = 11905

#: Cloud Logging ingestion lag. 8s was too short: a single small batch would
#: land after the window and be reported as "0 requests".
LOG_SETTLE_S = 20


def probe_response_size(client, ctx: dict) -> list[dict]:
    """Cross the 15 MB HTTP response ceiling using `bloat` mode.

    Varying rows per batch cannot reach the ceiling: BigQuery caps a request at
    ~11,905 rows no matter what max_batching_rows says, so at 1 KB/row the
    response tops out near 11.9 MB and never crosses 15 MB. The only way there
    is to widen each *reply*, so this varies `width` and holds the row count at
    the cap.
    """
    records: list[dict] = []
    for width in (1000, 1200, 1500, 2000, 3000):
        est_mb = OBSERVED_ROW_CAP * width / 1_000_000
        fn = f"`{ctx['project']}.{ctx['dataset']}`.bloat_w{width}"
        sql = (f"SELECT SUM(LENGTH({fn}(ssn, 'ssn')))\n"
               f"FROM (SELECT ssn FROM `{ctx['project']}.{ctx['dataset']}."
               f"{ctx['table']}` LIMIT 50000)")
        try:
            stats = run_query(client, sql)
            ok, err = True, None
        except Exception as exc:  # noqa: BLE001
            stats, ok, err = {}, False, str(exc)[:300]
        rec = {
            "phase": "limits_response", "config": LIMIT_DEPLOYMENT.label,
            "probe": "response_size", "reply_width": width,
            "rows_per_request": OBSERVED_ROW_CAP,
            "est_response_mb": round(est_mb, 1), "succeeded": ok,
            "elapsed_s": stats.get("elapsed_s"), "error": err,
        }
        records.append(rec)
        print(f"    width={width:>5}B  ~{est_mb:>5.1f} MB response  "
              f"{'OK' if ok else 'FAILED'}")
        if err:
            print(f"      {err[:200]}")
        time.sleep(3)
    return records


def probe_actual_batching(client, ctx: dict) -> list[dict]:
    """Does BigQuery honour very large max_batching_rows, or cap it silently?

    `max_batching_rows` is documented as an upper bound BigQuery *may* use, with
    no stated default. The service logs the real batch sizes, so this reads the
    answer straight off the wire instead of guessing.
    """
    records: list[dict] = []
    trials = [Trial(mode="fpe_decrypt", batch=b)
              for b in (10000, 50000, 100000, 250000, 500000, 1000000)]
    # Plus the no-max_batching_rows function: BigQuery's own default choice.
    for trial in trials + [Trial(mode="fpe_decrypt_auto", batch=0)]:
        name = (f"fpe_decrypt_b{trial.batch}" if trial.batch
                else "fpe_decrypt_auto")
        fn = f"`{ctx['project']}.{ctx['dataset']}`.{name}"
        sql = (f"SELECT SUM(LENGTH({fn}(ssn, 'ssn')))\n"
               f"FROM (SELECT ssn FROM `{ctx['project']}.{ctx['dataset']}."
               f"{ctx['table']}` LIMIT {ctx['rows']})")
        t_start = datetime.now(timezone.utc) - timedelta(seconds=2)
        try:
            stats = run_query(client, sql)
            ok, err = True, None
        except Exception as exc:  # noqa: BLE001
            stats, ok, err = {}, False, str(exc)[:300]
        t_end = datetime.now(timezone.utc) + timedelta(seconds=2)
        time.sleep(LOG_SETTLE_S)
        logs = summarise_logs(fetch_logs(ctx["project"], ctx["service"],
                                         t_start, t_end)) if ok else {}
        rec = {
            "phase": "limits_batching", "config": LIMIT_DEPLOYMENT.label,
            "probe": "actual_batching", "requested_batch": trial.batch or "auto",
            "rows": ctx["rows"], "succeeded": ok, "error": err,
            **stats, **logs,
        }
        records.append(rec)
        print(f"    max_batching_rows={str(trial.batch or 'auto'):>8}  "
              f"{'OK' if ok else 'FAILED'}  "
              f"actual_median={logs.get('batch_rows_median', '?')}  "
              f"actual_max={logs.get('batch_rows_max', '?')}  "
              f"batches={logs.get('log_batches', '?')}")
        if err:
            print(f"      {err[:200]}")
    return records


def probe_short_circuit(client, ctx: dict) -> list[dict]:
    """The batching cliff.

    BigQuery disables batching when a remote function sits inside a
    short-circuiting expression (CASE/IF, MERGE ... WHEN MATCHED): `calls` then
    "has exactly one element". One HTTP request per row is catastrophic, and
    the fix — hoist the call out of the conditional — is invisible unless you
    know to look. The logs prove which shape actually batched.
    """
    project, dataset, table = ctx["project"], ctx["dataset"], ctx["table"]
    fn = f"`{project}.{dataset}`.fpe_decrypt_b5000"
    src = f"(SELECT id, ssn FROM `{project}.{dataset}.{table}` LIMIT {ctx['small_rows']})"

    variants = {
        # Baseline: function applied unconditionally -> full batching.
        "plain": f"SELECT SUM(LENGTH({fn}(ssn, 'ssn'))) FROM {src}",
        # Anti-pattern: inside CASE -> short-circuit -> batching disabled.
        "inside_case": (
            f"SELECT SUM(LENGTH(CASE WHEN MOD(id, 2) = 0 "
            f"THEN {fn}(ssn, 'ssn') ELSE ssn END)) FROM {src}"
        ),
        # Attempted fix that does NOT work: the optimizer inlines this trivial
        # subquery straight back into the IF, restoring short-circuit semantics
        # and single-row batches. Kept in the matrix precisely because it looks
        # like it should help.
        "hoisted_subquery": (
            f"SELECT SUM(LENGTH(IF(MOD(id, 2) = 0, dec, ssn))) FROM ("
            f"SELECT id, ssn, {fn}(ssn, 'ssn') AS dec FROM {src})"
        ),
        # Fix that does work: filter to the rows you actually need FIRST, then
        # apply the function unconditionally to all of them. No conditional
        # remains for the optimizer to short-circuit against.
        "filter_then_apply": (
            f"SELECT SUM(LENGTH({fn}(ssn, 'ssn'))) FROM {src} "
            f"WHERE MOD(id, 2) = 0"
        ),
    }

    records: list[dict] = []
    for name, sql in variants.items():
        t_start = datetime.now(timezone.utc) - timedelta(seconds=2)
        try:
            stats = run_query(client, sql)
            ok, err = True, None
        except Exception as exc:  # noqa: BLE001
            stats, ok, err = {}, False, str(exc)[:300]
        t_end = datetime.now(timezone.utc) + timedelta(seconds=2)
        time.sleep(LOG_SETTLE_S)
        logs = summarise_logs(fetch_logs(ctx["project"], ctx["service"],
                                         t_start, t_end)) if ok else {}
        rec = {
            "phase": "antipattern", "config": LIMIT_DEPLOYMENT.label,
            "probe": "short_circuit", "variant": name,
            "rows": ctx["small_rows"], "succeeded": ok, "error": err,
            **stats, **logs,
        }
        records.append(rec)
        print(f"    {name:<12} {'OK' if ok else 'FAILED'}  "
              f"{stats.get('elapsed_s', 0):.2f}s  "
              f"batches={logs.get('log_batches', '?')}  "
              f"median_batch_rows={logs.get('batch_rows_median', '?')}")
        if err:
            print(f"      {err[:200]}")
    return records


def probe_retries(client, ctx: dict) -> list[dict]:
    """Retry behaviour: 503 is retried (up to 20x), 400 is not."""
    project, dataset, table = ctx["project"], ctx["dataset"], ctx["table"]
    records: list[dict] = []
    for name, fn_name, expect in (
        ("always_503", "error_b1000", "fails after retries"),
        ("half_503", "error_half_b1000", "succeeds via retries"),
        ("always_400", "error_400_b1000", "fails fast, no retries"),
    ):
        fn = f"`{project}.{dataset}`.{fn_name}"
        sql = (f"SELECT COUNT({fn}(ssn, 'ssn')) FROM "
               f"(SELECT ssn FROM `{project}.{dataset}.{table}` LIMIT 5000)")
        t_start = datetime.now(timezone.utc) - timedelta(seconds=2)
        t0 = time.time()
        try:
            stats = run_query(client, sql)
            ok, err = True, None
        except Exception as exc:  # noqa: BLE001
            stats, ok, err = {"elapsed_s": time.time() - t0}, False, str(exc)[:300]
        t_end = datetime.now(timezone.utc) + timedelta(seconds=5)
        time.sleep(LOG_SETTLE_S)
        # Count how many times the endpoint was actually hit — that is the
        # retry count, and it is not visible from the BigQuery side at all.
        attempts = len(
            fetch_logs(project, ctx["service"], t_start, t_end,
                       events=("batch", "batch_error"))
        )
        rec = {
            "phase": "limits_retries", "config": LIMIT_DEPLOYMENT.label,
            "probe": "retries", "variant": name, "expectation": expect,
            "succeeded": ok, "elapsed_s": round(stats.get("elapsed_s", 0), 2),
            "endpoint_invocations": attempts, "error": err,
        }
        records.append(rec)
        print(f"    {name:<11} {'OK' if ok else 'FAILED':<7} "
              f"{rec['elapsed_s']:>7.2f}s  endpoint_hits={attempts}  ({expect})")
        if err:
            print(f"      {err[:200]}")
    return records


#: Cloud Run shapes for the batch-cap probe. Deliberately extreme in both
#: directions, including containerConcurrency at Cloud Run's own default of 80.
BATCH_CAP_DEPLOYMENTS = [
    Deployment(label="capsmall", cpu=1, memory="1Gi", concurrency=1, workers=1),
    Deployment(label="capmid", cpu=4, memory="2Gi", concurrency=8, workers=4),
    Deployment(label="capbig", cpu=8, memory="4Gi", concurrency=80, workers=8),
]

#: (column, data element, approximate plaintext width in chars)
BATCH_CAP_COLUMNS = [
    ("ssn", "ssn", 11),
    ("dob", "digits", 10),
    ("name", "name", 11),
    ("email", "email", 32),
]


def probe_batch_cap(client, ctx: dict) -> list[dict]:
    """What actually determines the ~11,905 rows-per-request cap?

    Two candidate explanations:
      (a) Cloud Run capacity — bigger instance or higher containerConcurrency
          would raise it. Implausible: BigQuery decides the batch before it
          contacts Cloud Run, and cannot see containerConcurrency at all.
      (b) A byte budget on the request payload — then a WIDER column yields a
          SMALLER row cap, and Cloud Run sizing is irrelevant.

    This varies both axes against a function with max_batching_rows=1,000,000
    (far above any observed cap) and reads the real batch size off the logs.
    """
    records: list[dict] = []
    ds = f"`{ctx['project']}.{ctx['dataset']}`"
    tbl = f"`{ctx['project']}.{ctx['dataset']}.{ctx['table']}`"

    for cfg in BATCH_CAP_DEPLOYMENTS:
        if not ctx.get("skip_deploy"):
            print(f"    deploying {cfg.label} "
                  f"(cpu={cfg.cpu} conc={cfg.concurrency} workers={cfg.workers})")
            deploy(cfg, verbose=False)
            time.sleep(10)

        for column, element, width in BATCH_CAP_COLUMNS:
            sql = (f"SELECT SUM(LENGTH({ds}.fpe_decrypt_b1000000"
                   f"({column}, '{element}'))) "
                   f"FROM (SELECT {column} FROM {tbl} LIMIT {ctx['rows']})")
            t_start = datetime.now(timezone.utc) - timedelta(seconds=2)
            try:
                stats = run_query(client, sql)
                ok, err = True, None
            except Exception as exc:  # noqa: BLE001
                stats, ok, err = {}, False, str(exc)[:300]
            t_end = datetime.now(timezone.utc) + timedelta(seconds=2)
            time.sleep(LOG_SETTLE_S)
            logs = summarise_logs(fetch_logs(ctx["project"], ctx["service"],
                                             t_start, t_end)) if ok else {}
            cap = logs.get("batch_rows_max") or 0
            rec = {
                "phase": "batch_cap", "config": cfg.label, "probe": "batch_cap",
                "cpu": cfg.cpu, "concurrency": cfg.concurrency,
                "workers": cfg.workers, "column": column,
                "plaintext_width": width, "succeeded": ok, "error": err,
                # If the cap is a byte budget, cap * bytes-per-row is constant.
                "implied_bytes_per_row": (
                    round(5_000_000 / cap, 1) if cap else None),
                **stats, **logs,
            }
            records.append(rec)
            print(f"      {cfg.label:<9} {column:<6} (~{width:>2}ch)  "
                  f"cap={cap:>7,}  median={logs.get('batch_rows_median', 0):>8,}  "
                  f"requests={logs.get('log_batches', 0):>4}")
            if err:
                print(f"        {err[:180]}")
    return records


def probe_input_size(client, ctx: dict) -> list[dict]:
    """Where does the ~256 KiB batching budget end and the 5 MiB limit begin?

    These two limits look like they conflict: if BigQuery never sends more than
    ~256 KiB, the documented 5 MB per-row input limit could never be reached.
    They don't, because the budget is a batching *target* — BigQuery packs rows
    until the next would overflow it, but always sends at least one row, however
    wide.

    Uses `hmac`, whose reply is 16 chars regardless of input size, so the 15 MB
    *response* ceiling cannot interfere with a probe of the *request* side.

    REPEAT() has its own output cap well under 5 MiB, so wide values are built
    by concatenating 1 MB chunks.
    """
    ds = f"`{ctx['project']}.{ctx['dataset']}`"
    records: list[dict] = []

    #: (label, SQL expression producing one value, approximate bytes)
    widths = [
        ("50 KB", "REPEAT('a', 50000)", 50_000),
        ("200 KB", "REPEAT('a', 200000)", 200_000),
        ("300 KB", "REPEAT('a', 300000)", 300_000),
        ("1 MB", "REPEAT('a', 1000000)", 1_000_000),
        ("3 MB", "CONCAT(" + ", ".join(["REPEAT('a',1000000)"] * 3) + ")", 3_000_000),
        ("5 MB", "CONCAT(" + ", ".join(["REPEAT('a',1000000)"] * 5) + ")", 5_000_000),
        ("6 MB", "CONCAT(" + ", ".join(["REPEAT('a',1000000)"] * 6) + ")", 6_000_000),
    ]

    for label, expr, nbytes in widths:
        n_rows = 8 if nbytes <= 1_000_000 else 3
        sql = (f"SELECT COUNT({ds}.hmac_b1000(v, 'ssn')) "
               f"FROM (SELECT {expr} AS v FROM UNNEST(GENERATE_ARRAY(1, {n_rows})))")
        t_start = datetime.now(timezone.utc) - timedelta(seconds=2)
        try:
            stats = run_query(client, sql)
            ok, err = True, None
        except Exception as exc:  # noqa: BLE001
            stats, ok, err = {}, False, str(exc)[:300]
        t_end = datetime.now(timezone.utc) + timedelta(seconds=2)
        time.sleep(LOG_SETTLE_S)
        logs = summarise_logs(fetch_logs(ctx["project"], ctx["service"],
                                         t_start, t_end)) if ok else {}
        rec = {
            "phase": "input_size", "config": LIMIT_DEPLOYMENT.label,
            "probe": "input_size", "variant": label,
            "bytes_per_row": nbytes, "rows_queried": n_rows,
            "succeeded": ok, "error": err, **stats, **logs,
        }
        records.append(rec)
        print(f"    {label:>7}/row  {'OK' if ok else 'FAILED':<7} "
              f"rows_per_request={logs.get('batch_rows_max', '-')}  "
              f"requests={logs.get('log_batches', '-')}")
        if err:
            print(f"      {err[:180]}")
    return records


def probe_search_pattern(client, ctx: dict) -> list[dict]:
    """Search by tokenising the term vs detokenising the column.

    FPE is deterministic, so looking up a known plaintext does not require
    decrypting the table: encrypt the search term once and compare ciphertext
    natively. This measures how much that inversion is actually worth, and
    confirms the remote function really is invoked once rather than per row.
    """
    project, dataset, table = ctx["project"], ctx["dataset"], ctx["table"]
    ds = f"`{project}.{dataset}`"
    tbl = f"`{project}.{dataset}.{table}`"

    # A real ciphertext from the table, and its plaintext, to search for.
    row = list(client.query(
        f"SELECT c.ssn AS clear, t.ssn AS token "
        f"FROM `{project}.{dataset}.{ctx['clear_table']}` c "
        f"JOIN {tbl} t USING (id) LIMIT 1"
    ).result())[0]
    clear, token = row["clear"], row["token"]

    variants = {
        # Anti-pattern: decrypt every row, then compare.
        "detokenize_column": (
            f"SELECT COUNT(*) FROM {tbl} "
            f"WHERE {ds}.fpe_decrypt_b5000(ssn, 'ssn') = '{clear}'"
        ),
        # Pattern: encrypt the term once, compare ciphertext natively.
        # DECLARE/SET forces exactly one invocation — an inline call in the
        # WHERE clause is not guaranteed to be constant-folded, because
        # BigQuery treats remote functions as non-deterministic.
        "tokenize_term": (
            f"DECLARE tok STRING;\n"
            f"SET tok = (SELECT {ds}.fpe_encrypt(''||'{clear}', 'ssn'));\n"
            f"SELECT COUNT(*) FROM {tbl} WHERE ssn = tok;"
        ),
        # Floor: the same scan with the token already known, no remote
        # function at all. Shows how much of tokenize_term is just the scan.
        "precomputed_token": (
            f"SELECT COUNT(*) FROM {tbl} WHERE ssn = '{token}'"
        ),
    }

    records: list[dict] = []
    for name, sql in variants.items():
        t_start = datetime.now(timezone.utc) - timedelta(seconds=2)
        try:
            stats = run_query(client, sql)
            ok, err = True, None
        except Exception as exc:  # noqa: BLE001
            stats, ok, err = {}, False, str(exc)[:300]
        t_end = datetime.now(timezone.utc) + timedelta(seconds=2)
        time.sleep(LOG_SETTLE_S)
        logs = summarise_logs(fetch_logs(project, ctx["service"],
                                         t_start, t_end)) if ok else {}
        rec = {
            "phase": "search_pattern", "config": LIMIT_DEPLOYMENT.label,
            "probe": "search", "variant": name, "succeeded": ok, "error": err,
            **stats, **logs,
        }
        records.append(rec)
        print(f"    {name:<19} {'OK' if ok else 'FAILED':<7} "
              f"{stats.get('elapsed_s', 0):>7.2f}s  "
              f"http_requests={logs.get('log_batches', 0):<6} "
              f"rows_sent={logs.get('log_rows_total', 0):,}  "
              f"bytes={stats.get('bytes_processed', 0):,}")
        if err:
            print(f"      {err[:200]}")
    return records


def probe_access_control(client, ctx: dict) -> list[dict]:
    """Authorized-view + entitlement-table shapes over tokenized data.

    Patterns A and B return IDENTICAL results (all rows, ssn masked where not
    entitled); only the SQL shape differs. Pattern C returns fewer rows — a row
    filter, not column masking — so it is not a drop-in replacement, and is
    included to show what dropping the masking requirement buys.

    Requires fpe/scripts/setup_access_control.sh to have been run.
    """
    ds = f"`{ctx['project']}.{ctx['dataset']}`"
    n = ctx["small_rows"]

    variants = {
        # A: conditional detokenization -> short-circuit -> 1 request per row.
        "A_case_masking": (
            f"SELECT SUM(LENGTH(ssn)) FROM (SELECT ssn FROM {ds}.v_ssn_case "
            f"WHERE id <= {n})"
        ),
        # B: UNION ALL of unconditional branches -> batching preserved.
        "B_union_all_masking": (
            f"SELECT SUM(LENGTH(ssn)) FROM (SELECT ssn FROM {ds}.v_ssn_union "
            f"WHERE id <= {n})"
        ),
        # C: entitlement as a row filter -> fewer rows AND batching preserved.
        "C_row_filter": (
            f"SELECT SUM(LENGTH(ssn)) FROM (SELECT ssn FROM {ds}.v_ssn_rowfilter "
            f"WHERE id <= {n})"
        ),
        # D: row AND column control across three columns, linear in columns.
        # email is NOT granted, so its CTE should scan zero rows and issue no
        # remote calls at all.
        "D_row_and_column": (
            f"SELECT SUM(LENGTH(ssn)) + SUM(LENGTH(email)) + SUM(LENGTH(name)) "
            f"FROM (SELECT ssn, email, name FROM {ds}.v_row_and_column "
            f"WHERE id <= {n})"
        ),
        # D as it would naively be written: one CASE per governed column.
        "D_naive_case_3col": (
            f"SELECT SUM(LENGTH(ssn)) + SUM(LENGTH(email)) + SUM(LENGTH(name)) "
            f"FROM (SELECT ssn, email, name FROM {ds}.v_row_and_column_naive "
            f"WHERE id <= {n})"
        ),
    }

    records: list[dict] = []
    for name, sql in variants.items():
        t_start = datetime.now(timezone.utc) - timedelta(seconds=2)
        try:
            stats = run_query(client, sql)
            ok, err = True, None
        except Exception as exc:  # noqa: BLE001
            stats, ok, err = {}, False, str(exc)[:300]
        t_end = datetime.now(timezone.utc) + timedelta(seconds=2)
        time.sleep(LOG_SETTLE_S)
        logs = summarise_logs(fetch_logs(ctx["project"], ctx["service"],
                                         t_start, t_end)) if ok else {}
        rec = {
            "phase": "access_control", "config": LIMIT_DEPLOYMENT.label,
            "probe": "access_control", "variant": name,
            "rows": n, "succeeded": ok, "error": err, **stats, **logs,
        }
        records.append(rec)
        print(f"    {name:<20} {'OK' if ok else 'FAILED':<7} "
              f"{stats.get('elapsed_s', 0):>8.2f}s  "
              f"http_requests={logs.get('log_batches', 0):<7} "
              f"rows/req={logs.get('batch_rows_median', 0)}")
        if err:
            print(f"      {err[:200]}")

    # Scenario 5: point lookup by plaintext, through an authorized view.
    # Combines the search pattern with access control -- the shape most
    # user-facing traffic takes. Needs a real plaintext/token pair.
    pair = list(client.query(
        f"SELECT c.ssn AS clear, t.ssn AS token "
        f"FROM `{ctx['project']}.{ctx['dataset']}.{ctx['clear_table']}` c "
        f"JOIN `{ctx['project']}.{ctx['dataset']}.{ctx['table']}` t USING (id) "
        f"WHERE MOD(c.id, 4) IN (0, 1) LIMIT 1"     # entitled branch
    ).result())[0]

    lookups = {
        # Filter on the view's decrypted output: every entitled row must be
        # decrypted before the comparison can be made.
        "F_naive_filter_on_plaintext": (
            f"SELECT COUNT(*) FROM {ds}.v_lookup_naive "
            f"WHERE ssn = '{pair['clear']}'"
        ),
        # Tokenize the term once, filter on ciphertext. The token predicate is
        # pushed below the decryption, so only the matching row is decrypted.
        "F_filter_on_token": (
            f"DECLARE tok STRING;\n"
            f"SET tok = (SELECT {ds}.fpe_encrypt(''||'{pair['clear']}', 'ssn'));\n"
            f"SELECT COUNT(*) FROM {ds}.v_lookup_by_token WHERE ssn_token = tok;"
        ),
        # Caller already holds the token: no remote function in the query at
        # all, so it does not count against the 10-concurrent-query limit.
        "F_caller_supplies_token": (
            f"SELECT COUNT(*) FROM {ds}.v_lookup_by_token "
            f"WHERE ssn_token = '{pair['token']}'"
        ),
    }
    for name, sql in lookups.items():
        t_start = datetime.now(timezone.utc) - timedelta(seconds=2)
        try:
            stats = run_query(client, sql)
            ok, err = True, None
        except Exception as exc:  # noqa: BLE001
            stats, ok, err = {}, False, str(exc)[:300]
        t_end = datetime.now(timezone.utc) + timedelta(seconds=2)
        time.sleep(LOG_SETTLE_S)
        logs = summarise_logs(fetch_logs(ctx["project"], ctx["service"],
                                         t_start, t_end)) if ok else {}
        records.append({
            "phase": "access_control", "config": LIMIT_DEPLOYMENT.label,
            "probe": "access_control", "variant": name,
            "rows": ctx["rows"], "succeeded": ok, "error": err, **stats, **logs,
        })
        print(f"    {name:<27} {'OK' if ok else 'FAILED':<7} "
              f"{stats.get('elapsed_s', 0):>7.2f}s  "
              f"rows_to_service={logs.get('log_rows_total', 0):>9,}  "
              f"http_requests={logs.get('log_batches', 0)}")
        if err:
            print(f"      {err[:200]}")

    # Dedup: same view semantics, cardinality-aware execution.
    for name, view in (("E_name_dedup", "v_name_dedup"),
                       ("E_name_naive", "v_name_naive")):
        sql = (f"SELECT SUM(LENGTH(name)) FROM (SELECT name FROM {ds}.{view} "
               f"WHERE id <= {ctx['rows']})")
        t_start = datetime.now(timezone.utc) - timedelta(seconds=2)
        try:
            stats = run_query(client, sql)
            ok, err = True, None
        except Exception as exc:  # noqa: BLE001
            stats, ok, err = {}, False, str(exc)[:300]
        t_end = datetime.now(timezone.utc) + timedelta(seconds=2)
        time.sleep(LOG_SETTLE_S)
        logs = summarise_logs(fetch_logs(ctx["project"], ctx["service"],
                                         t_start, t_end)) if ok else {}
        records.append({
            "phase": "access_control", "config": LIMIT_DEPLOYMENT.label,
            "probe": "access_control", "variant": name,
            "rows": ctx["rows"], "succeeded": ok, "error": err, **stats, **logs,
        })
        print(f"    {name:<20} {'OK' if ok else 'FAILED':<7} "
              f"{stats.get('elapsed_s', 0):>8.2f}s  "
              f"rows_to_service={logs.get('log_rows_total', 0):>9,}  "
              f"http_requests={logs.get('log_batches', 0)}")
        if err:
            print(f"      {err[:200]}")

    # Assert equivalence rather than claiming it in prose. A shape that is
    # faster but returns different data is not a fix.
    checks = {
        "A_vs_B": (
            f"SELECT COUNTIF(a.ssn IS DISTINCT FROM b.ssn) AS mismatches, "
            f"COUNT(*) AS compared "
            f"FROM (SELECT id, ssn FROM {ds}.v_ssn_case WHERE id <= {n}) a "
            f"FULL OUTER JOIN (SELECT id, ssn FROM {ds}.v_ssn_union WHERE id <= {n}) b "
            f"USING (id)"
        ),
        "D_vs_naive_case": (
            f"WITH d AS (SELECT id, ssn, email, name FROM {ds}.v_row_and_column "
            f"           WHERE id <= {n}), "
            f"     c AS (SELECT id, ssn, email, name "
            f"           FROM {ds}.v_row_and_column_naive WHERE id <= {n}) "
            f"SELECT COUNTIF(d.ssn IS DISTINCT FROM c.ssn "
            f"            OR d.email IS DISTINCT FROM c.email "
            f"            OR d.name IS DISTINCT FROM c.name) AS mismatches, "
            f"       COUNT(*) AS compared "
            f"FROM d FULL OUTER JOIN c USING (id)"
        ),
        "E_dedup_vs_naive": (
            f"SELECT COUNTIF(a.name IS DISTINCT FROM b.name) AS mismatches, "
            f"COUNT(*) AS compared "
            f"FROM (SELECT id, name FROM {ds}.v_name_dedup WHERE id <= {n}) a "
            f"FULL OUTER JOIN (SELECT id, name FROM {ds}.v_name_naive "
            f"                 WHERE id <= {n}) b USING (id)"
        ),
    }
    for label, check in checks.items():
        try:
            row = list(client.query(check).result())[0]
            equivalent = row["mismatches"] == 0
            print(f"    equivalence {label:<18} mismatches={row['mismatches']} "
                  f"of {row['compared']} -> "
                  f"{'EQUIVALENT' if equivalent else 'NOT EQUIVALENT'}")
            records.append({
                "phase": "access_control", "probe": "equivalence",
                "variant": label, "mismatches": row["mismatches"],
                "compared": row["compared"], "equivalent": equivalent,
            })
        except Exception as exc:  # noqa: BLE001
            print(f"    ! equivalence {label} failed: {str(exc)[:200]}")
    return records


def probe_placement(client, ctx: dict) -> list[dict]:
    """Where the remote function sits in the plan, holding the result identical.

    Every pair below returns the same answer. The only difference is how many
    rows reach the remote function: before or after the reducing operation.
    """
    project, dataset = ctx["project"], ctx["dataset"]
    ds = f"`{project}.{dataset}`"
    tbl = f"`{project}.{dataset}.{ctx['table']}`"
    n = ctx["rows"]

    variants = {
        # --- LIMIT: 100 rows wanted out of n ---
        "limit_AFTER_detok": (
            f"SELECT ssn FROM (SELECT {ds}.fpe_decrypt_b5000(ssn,'ssn') AS ssn "
            f"FROM (SELECT ssn FROM {tbl} LIMIT {n})) LIMIT 100"
        ),
        "limit_BEFORE_detok": (
            f"SELECT {ds}.fpe_decrypt_b5000(ssn,'ssn') AS ssn "
            f"FROM (SELECT ssn FROM {tbl} LIMIT 100)"
        ),
        # --- Aggregation: one number out of n rows ---
        # Counting distinct plaintexts is identical to counting distinct
        # tokens, because the tokenization is deterministic and injective.
        "aggregate_AFTER_detok": (
            f"SELECT COUNT(DISTINCT {ds}.fpe_decrypt_b5000(ssn,'ssn')) "
            f"FROM (SELECT ssn FROM {tbl} LIMIT {n})"
        ),
        "aggregate_BEFORE_detok": (
            f"SELECT COUNT(DISTINCT ssn) FROM (SELECT ssn FROM {tbl} LIMIT {n})"
        ),
        # --- Filter then detokenize the survivors ---
        "filter_AFTER_detok": (
            f"SELECT SUM(LENGTH(ssn)) FROM ("
            f"SELECT {ds}.fpe_decrypt_b5000(ssn,'ssn') AS ssn, id "
            f"FROM (SELECT id, ssn FROM {tbl} LIMIT {n})) WHERE MOD(id,1000)=0"
        ),
        "filter_BEFORE_detok": (
            f"SELECT SUM(LENGTH({ds}.fpe_decrypt_b5000(ssn,'ssn'))) FROM ("
            f"SELECT id, ssn FROM {tbl} LIMIT {n}) WHERE MOD(id,1000)=0"
        ),
    }

    records: list[dict] = []
    for name, sql in variants.items():
        t_start = datetime.now(timezone.utc) - timedelta(seconds=2)
        try:
            stats = run_query(client, sql)
            ok, err = True, None
        except Exception as exc:  # noqa: BLE001
            stats, ok, err = {}, False, str(exc)[:300]
        t_end = datetime.now(timezone.utc) + timedelta(seconds=2)
        time.sleep(LOG_SETTLE_S)
        logs = summarise_logs(fetch_logs(ctx["project"], ctx["service"],
                                         t_start, t_end)) if ok else {}
        rec = {
            "phase": "placement", "config": LIMIT_DEPLOYMENT.label,
            "probe": "placement", "variant": name, "rows": n,
            "succeeded": ok, "error": err, **stats, **logs,
        }
        records.append(rec)
        print(f"    {name:<24} {'OK' if ok else 'FAILED':<7} "
              f"{stats.get('elapsed_s', 0):>8.2f}s  "
              f"rows_to_service={logs.get('log_rows_total', 0):>9,}  "
              f"http_requests={logs.get('log_batches', 0)}")
        if err:
            print(f"      {err[:200]}")
    return records


PROBES = {
    "limits_queries": probe_concurrent_queries,
    "limits_response": probe_response_size,
    "limits_batching": probe_actual_batching,
    "limits_retries": probe_retries,
    "antipattern": probe_short_circuit,
    "search_pattern": probe_search_pattern,
    "access_control": probe_access_control,
    "placement": probe_placement,
    "batch_cap": probe_batch_cap,
    "input_size": probe_input_size,
}


# --------------------------------------------------------------------------
# Execution
# --------------------------------------------------------------------------


def deploy(cfg: Deployment, verbose: bool = True) -> None:
    env = {**os.environ, **cfg.env()}
    proc = subprocess.run(
        [str(REPO_ROOT / "fpe" / "scripts" / "deploy.sh")],
        env=env, capture_output=True, text=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"deploy failed for {cfg.label}:\n{proc.stdout}\n{proc.stderr}")
    if verbose:
        for line in proc.stdout.splitlines():
            if line.strip():
                print(f"      {line}")


def run_query(client: bigquery.Client, sql: str) -> dict:
    job_config = bigquery.QueryJobConfig(use_query_cache=False)
    t0 = time.time()
    job = client.query(sql, job_config=job_config)
    job.result()
    wall = time.time() - t0

    job = client.get_job(job.job_id, location=job.location)
    elapsed = (
        (job.ended - job.started).total_seconds()
        if job.ended and job.started
        else wall
    )
    return {
        "job_id": job.job_id,
        "elapsed_s": elapsed,
        "wall_s": wall,
        "slot_millis": job.slot_millis,
        "bytes_processed": job.total_bytes_processed,
    }


def fetch_log_entries(
    project: str,
    service: str,
    start: datetime,
    end: datetime,
    events: tuple[str, ...] = ("batch",),
) -> list[tuple[datetime, dict]]:
    """Pull the structured logs the service emitted during a window, with times.

    The timestamp is kept, not discarded, for two reasons: a whole cell's
    iterations are now fetched in one call and have to be split apart again,
    and reconstructing *when* each request ran is the only way to measure how
    many really overlapped (see `peak_concurrency`).

    `events` selects which event types to include — the retry probe needs
    "batch_error" too, since a retried invocation never produces a "batch".
    """
    event_filter = " OR ".join(f'jsonPayload.event="{e}"' for e in events)
    filt = (
        f'resource.type="cloud_run_revision" '
        f'resource.labels.service_name="{service}" '
        f'({event_filter}) '
        f'timestamp>="{start.isoformat()}" timestamp<="{end.isoformat()}"'
    )
    proc = subprocess.run(
        ["gcloud", "logging", "read", filt, f"--project={project}",
         "--format=json", "--limit=100000"],
        capture_output=True, text=True,
    )
    if proc.returncode != 0:
        print(f"      ! log fetch failed: {proc.stderr.strip()[:200]}")
        return []
    try:
        entries = json.loads(proc.stdout or "[]")
    except json.JSONDecodeError:
        return []

    out: list[tuple[datetime, dict]] = []
    for e in entries:
        payload = e.get("jsonPayload")
        if not payload:
            continue
        raw = e.get("timestamp") or e.get("receiveTimestamp")
        try:
            ts = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        except (TypeError, ValueError):
            continue
        out.append((ts, payload))
    out.sort(key=lambda tp: tp[0])
    return out


def fetch_logs(
    project: str,
    service: str,
    start: datetime,
    end: datetime,
    events: tuple[str, ...] = ("batch",),
) -> list[dict]:
    """Payload-only view, for the probes that do not care when things happened."""
    return [p for _, p in fetch_log_entries(project, service, start, end, events)]


def peak_concurrency(entries: list[tuple[datetime, dict]]) -> dict:
    """How many requests were *actually* executing at once, across all workers.

    The per-request `inflight` field cannot answer this: it is counted inside
    one process, so with 16 sync workers it reads 1 no matter what. And
    `worker_processes` is a count of distinct pids seen over the whole window,
    which says every worker was used at some point, not that any two ran
    together.

    Each log line is written when the handler finishes, so the request occupied
    [t - total_ms, t]. Sweeping those intervals gives the real peak, and the
    time-weighted mean gives the sustained level — which is the one the Phase 2
    rule is about, since a single momentary peak does not set throughput.
    """
    spans = []
    for ts, p in entries:
        total_ms = p.get("total_ms")
        if total_ms is None:
            continue
        spans.append((ts.timestamp() - float(total_ms) / 1000.0, ts.timestamp()))
    if not spans:
        return {}

    events: list[tuple[float, int]] = []
    for a, b in spans:
        events.append((a, 1))
        events.append((b, -1))
    events.sort()

    live = peak = 0
    prev_t = events[0][0]
    area = 0.0
    for t, delta in events:
        area += live * (t - prev_t)
        prev_t = t
        live += delta
        peak = max(peak, live)
    window = events[-1][0] - events[0][0]
    return {
        "concurrency_peak": peak,
        "concurrency_mean": round(area / window, 2) if window > 0 else 0,
        "busy_window_s": round(window, 2),
    }


def summarise_logs(payloads: list[dict]) -> dict:
    """Turn raw batch logs into the service-side view of one trial."""
    if not payloads:
        return {"log_batches": 0}

    rows = [int(p.get("rows", 0)) for p in payloads]
    inflight = [int(p.get("inflight", 0)) for p in payloads]
    per_row = [float(p.get("us_per_row", 0)) for p in payloads if p.get("us_per_row")]
    instances = {p.get("instance") for p in payloads if p.get("instance")}
    procs = {(p.get("instance"), p.get("pid")) for p in payloads}
    cpu_row = [float(p["cpu_us_per_row"]) for p in payloads
               if p.get("cpu_us_per_row") is not None]
    share = [float(p["cpu_share"]) for p in payloads if p.get("cpu_share") is not None]

    return {
        "cpu_us_per_row_median": round(statistics.median(cpu_row), 2) if cpu_row else None,
        "cpu_share_median": round(statistics.median(share), 3) if share else None,
        "log_batches": len(payloads),
        "log_rows_total": sum(rows),
        "batch_rows_mean": round(statistics.mean(rows), 1) if rows else 0,
        "batch_rows_median": statistics.median(rows) if rows else 0,
        "batch_rows_max": max(rows) if rows else 0,
        "instances": len(instances),
        "worker_processes": len(procs),
        "inflight_max": max(inflight) if inflight else 0,
        "inflight_mean": round(statistics.mean(inflight), 2) if inflight else 0,
        "us_per_row_median": round(statistics.median(per_row), 1) if per_row else 0,
        "us_per_row_p95": (
            round(sorted(per_row)[int(len(per_row) * 0.95)], 1)
            if len(per_row) > 20 else None
        ),
    }


def summarise_entries(entries: list[tuple[datetime, dict]]) -> dict:
    """`summarise_logs` plus the reconstructed cross-worker concurrency."""
    return {
        **summarise_logs([p for _, p in entries]),
        **peak_concurrency(entries),
    }


def build_sql(project: str, dataset: str, table: str, trial: Trial, rows: int,
              table_rows: int | None = None) -> str:
    """The measurement query, replicating the source table when `rows` exceeds it.

    The transit floor is ~400,000 rows/s, so a 25-second run of a cheap workload
    needs ~10,000,000 rows and the demo table holds 1,000,000. Cross-joining
    against `GENERATE_ARRAY` supplies them for one table scan.

    Repeating values is safe here: every mode's cost is per row and independent
    of the value, and BigQuery treats remote functions as non-deterministic, so
    it will not collapse the duplicates (that is the same property §5 relies on
    when it forces a single invocation with DECLARE/SET).
    """
    fn = f"`{project}.{dataset}`.{trial.name}"
    src = f"`{project}.{dataset}.{table}`"
    if table_rows and rows > table_rows:
        copies = -(-rows // table_rows)  # ceil
        inner = (f"SELECT t.{trial.column} FROM {src} t, "
                 f"UNNEST(GENERATE_ARRAY(1, {copies})) LIMIT {rows}")
    else:
        inner = f"SELECT {trial.column} FROM {src} LIMIT {rows}"
    return (
        f"SELECT SUM(LENGTH({fn}({trial.column}, '{trial.data_element}')))\n"
        f"FROM ({inner})"
    )


# --------------------------------------------------------------------------
# Adaptive run sizing
#
# The scaling study spans workloads from ~400,000 rows/s (transit floor) to
# ~500 rows/s (one slot, per-row I/O) — a factor of 800. No fixed row count
# gives all of those a measurable run: whatever number makes the slow cell
# finish this century makes the fast cell finish before it has warmed up.
#
# So each cell is piloted, then sized to a common *duration*. Throughput is a
# rate and stays comparable; what does not is the fixed per-query overhead
# share, which is why every cell targets the same seconds rather than each
# picking its own.
# --------------------------------------------------------------------------

#: Never size below this: too few rows and BigQuery's own query overhead, not
#: the service, is what is being timed.
MIN_SIZED_ROWS = 5_000

#: First pilot. Small enough to be nearly free on the slowest cell in the study
#: (one sync worker at 2 ms/row = ~4 s).
PILOT_ROWS = 2_000

#: Growth cap per pilot step. Protects against a pilot dominated by query
#: overhead projecting an absurd row count in one jump.
PILOT_GROWTH_CAP = 8

#: Requests per slot the smallest pilot must offer. Below one request per slot
#: the run is quantised by batch size rather than by the service: a 10,000-row
#: pilot at batch 5,000 is two HTTP requests, so eight slots measure the
#: throughput of two and the rate that comes out is meaningless. This bit — a
#: pilot projected 584,100 rows for a 25 s target and produced 169 s runs.
PILOT_REQUESTS_PER_SLOT = 2


def size_run(client: bigquery.Client, sql_for, target_s: float, max_rows: int,
             floor_rows: int = 0) -> tuple[int, list[tuple[int, float]], bool]:
    """Pilot a cell and return (rows, pilot history, hit_ceiling).

    Doubles as the warmup: the first pilot query pays cipher construction and
    lazy imports, which would otherwise land on iteration 1.

    Two pilot points give the intercept as well as the slope — elapsed is
    `query overhead + rows / rate`, and ignoring the overhead consistently
    undershoots on cheap workloads where it is most of the measurement. But the
    two-point estimate is only used when the two elapsed times actually differ:
    when they do not, `(r1 - r0) / (e1 - e0)` divides by noise and returns a
    rate an order of magnitude too high. Every projection is additionally
    bounded by 1.5x the naive proportional estimate, so no single bad fit can
    run away.
    """
    rows, history = max(PILOT_ROWS, floor_rows), []
    for _ in range(5):
        rows = max(MIN_SIZED_ROWS if history else 1, min(rows, max_rows))
        elapsed = run_query(client, sql_for(rows))["elapsed_s"]
        history.append((rows, elapsed))
        if elapsed >= 0.6 * target_s or rows >= max_rows:
            break

        proportional = rows * target_s / max(elapsed, 0.05)
        projected = proportional
        if len(history) >= 2:
            (r0, e0), (r1, e1) = history[-2], history[-1]
            # Require the elapsed times to be separated by more than noise
            # before trusting a slope drawn through them.
            if e1 - e0 > 0.2 * e1:
                rate = (r1 - r0) / (e1 - e0)
                overhead = e1 - r1 / rate
                projected = rate * (target_s - overhead)
        rows = int(max(rows * 2, min(projected, proportional * 1.5,
                                     rows * PILOT_GROWTH_CAP)))
    rows = max(MIN_SIZED_ROWS, min(rows, max_rows))
    return rows, history, rows >= max_rows


def run_cell(client: bigquery.Client, ctx: dict, phase_name: str,
             cfg: Deployment, trial: Trial, *, sentinel: bool = False) -> list[dict]:
    """Pilot, size, and run every iteration of one (deployment, trial) cell.

    All iterations run back to back and the service logs are fetched once for
    the whole cell, then split by timestamp. Paying LOG_SETTLE_S per iteration
    instead cost more wall clock than the iterations themselves.
    """
    project, dataset, table = ctx["project"], ctx["dataset"], ctx["table"]
    iterations, target_s = ctx["iterations"], ctx["target_seconds"]

    def sql_for(n: int) -> str:
        return build_sql(project, dataset, table, trial, n, ctx.get("table_rows"))

    # --- size the run (the pilot doubles as the warmup) ---
    try:
        if trial.rows is not None:
            rows, pilot, capped = trial.rows, [], False
            run_query(client, sql_for(min(PILOT_ROWS, rows)))
        elif ctx["adaptive"]:
            rows, pilot, capped = size_run(
                client, sql_for, target_s, ctx["max_rows"],
                floor_rows=PILOT_REQUESTS_PER_SLOT * cfg.slots * trial.batch)
        else:
            rows, pilot, capped = ctx["rows"], [], False
            run_query(client, sql_for(PILOT_ROWS))
    except Exception as exc:  # noqa: BLE001
        print(f"    ! {trial.name}: pilot/warmup failed: {str(exc)[:200]}")
        return []

    note = "  [AT TABLE CEILING — run shorter than target]" if capped else ""
    if pilot:
        trace = " -> ".join(f"{r:,}r/{e:.1f}s" for r, e in pilot)
        print(f"    pilot {trace}  => {rows:,} rows/iteration{note}")
    time.sleep(3)

    # --- iterations, back to back ---
    windows: list[tuple[datetime, datetime, dict]] = []
    sql = sql_for(rows)
    for i in range(iterations):
        t0 = datetime.now(timezone.utc) - timedelta(seconds=2)
        try:
            stats = run_query(client, sql)
        except Exception as exc:  # noqa: BLE001
            print(f"    ! {trial.name} iter {i+1} failed: {str(exc)[:300]}")
            continue
        t1 = datetime.now(timezone.utc) + timedelta(seconds=2)
        stats["rps"] = rows / stats["elapsed_s"] if stats["elapsed_s"] else 0
        windows.append((t0, t1, stats))
        print(f"    {trial.name} it{i+1}: {stats['elapsed_s']:.2f}s  "
              f"{stats['rps']:,.0f} rps")

    if not windows:
        return []

    # --- one log fetch for the whole cell, split by timestamp ---
    time.sleep(LOG_SETTLE_S)
    entries = fetch_log_entries(project, ctx["service"], windows[0][0], windows[-1][1])

    records = []
    for i, (t0, t1, stats) in enumerate(windows):
        mine = [(ts, p) for ts, p in entries if t0 <= ts <= t1]
        stats.update(summarise_entries(mine))
        records.append({
            "phase": phase_name,
            "config": cfg.label,
            **{f"cfg_{k}": v for k, v in asdict(cfg).items() if k != "label"},
            "cfg_slots": cfg.slots,
            "mode": trial.mode,
            "function": trial.name,
            "workload": trial.workload,
            "batch": trial.batch,
            "rows": rows,
            "rows_at_ceiling": capped,
            "iteration": i + 1,
            "sentinel": sentinel,
            **stats,
        })

    rps = [r["rps"] for r in records]
    print(f"    -> {trial.name} @ {cfg.label}: median {statistics.median(rps):,.0f} rps "
          f"(min {min(rps):,.0f} max {max(rps):,.0f}, {max(rps)/max(min(rps), 1):.2f}x "
          f"spread over {len(rps)} runs)  "
          f"conc_peak={records[-1].get('concurrency_peak','?')} "
          f"procs={records[-1].get('worker_processes','?')} "
          f"µs/row={records[-1].get('us_per_row_median','?')}")
    return records


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--phase", default="all",
                    help=f"one of {sorted(PHASES)}, 'all', 'limits' or 'study'")
    ap.add_argument("--rows", type=int, default=500_000,
                    help="row count when --no-adaptive; otherwise only a fallback")
    ap.add_argument("--iterations", type=int, default=3)
    ap.add_argument("--out", default=None)
    ap.add_argument("--list", action="store_true",
                    help="print the plan and exit without deploying")
    ap.add_argument("--skip-deploy", action="store_true",
                    help="reuse the currently deployed revision (single-config runs)")
    ap.add_argument("--adaptive", action="store_true",
                    help="size each cell to --target-seconds instead of --rows. "
                         "Required for the scaling study, whose cells differ in "
                         "throughput by over 100x")
    ap.add_argument("--target-seconds", type=float, default=25.0,
                    help="wall-clock target per iteration under --adaptive")
    ap.add_argument("--max-rows", type=int, default=20_000_000,
                    help="ceiling on adaptive sizing. Above the source table's "
                         "row count the table is replicated by cross join, so "
                         "this is a cost guard, not a data limit")
    ap.add_argument("--resume", action="store_true",
                    help="skip cells already complete in the output file")
    ap.add_argument("--no-sentinel", action="store_true",
                    help="skip the end-of-phase drift check (one extra deploy)")
    # Parameterise the study phases without a phase per variant.
    ap.add_argument("--workloads", help="comma-separated W-ids for workload_matrix")
    ap.add_argument("--workload", help="single W-id for rule_check/scale_axis")
    ap.add_argument("--slots", help="comma-separated slot counts for rule_check")
    ap.add_argument("--workers", type=int, help="Phase 1 winner, for scale_axis")
    ap.add_argument("--threads", type=int, help="Phase 1 winner, for scale_axis")
    ap.add_argument("--worker-class", help="Phase 1 winner, for scale_axis")
    args = ap.parse_args()

    OPTS.update({k: v for k, v in {
        "workloads": args.workloads.split(",") if args.workloads else None,
        "workload": args.workload,
        "slots": [int(s) for s in args.slots.split(",")] if args.slots else None,
        "workers": args.workers,
        "threads": args.threads,
        "worker_class": args.worker_class,
    }.items() if v is not None})

    load()
    project = require("PROJECT_ID")
    dataset = require("FPE_DATASET")
    table = require("FPE_TABLE_TOKENIZED")
    service = require("FPE_SERVICE")

    known = sorted(PHASES) + sorted(PROBES)
    if args.phase == "all":
        names = sorted(PHASES.keys() - set(STUDY_PHASES))
    elif args.phase == "limits":
        names = sorted(PROBES)
    elif args.phase == "study":
        names = list(STUDY_PHASES)
    else:
        names = args.phase.split(",")
    for n in names:
        if n not in PHASES and n not in PROBES:
            print(f"unknown phase {n!r}; known: {known} "
                  f"(or 'all' / 'limits' / 'study')", file=sys.stderr)
            return 2

    perf_names = [n for n in names if n in PHASES]
    probe_names = [n for n in names if n in PROBES]

    plan = [(n, cfg, trials) for n in perf_names for cfg, trials in PHASES[n]()]
    n_queries = sum(len(t) for _, _, t in plan) * args.iterations
    sizing = (f"~{args.target_seconds:.0f}s/iteration, adaptive rows"
              if args.adaptive else f"{args.rows:,} rows each")

    print(f"Sweep plan: {len(plan) + (1 if probe_names else 0)} deployment(s), "
          f"{n_queries} measured queries ({sizing}, {args.iterations} iteration(s))")
    for n, cfg, trials in plan:
        fns = ",".join(t.name for t in trials)
        print(f"  [{n}] {cfg.label}: cpu={cfg.cpu} conc={cfg.concurrency} "
              f"w={cfg.workers} t={cfg.threads} {cfg.worker_class} "
              f"slots={cfg.slots} mem={cfg.memory} max={cfg.max_instances} | {fns}")
    for n in probe_names:
        print(f"  [{n}] limit probe on {LIMIT_DEPLOYMENT.label} "
              f"(cpu={LIMIT_DEPLOYMENT.cpu} conc={LIMIT_DEPLOYMENT.concurrency} "
              f"w={LIMIT_DEPLOYMENT.workers} max={LIMIT_DEPLOYMENT.max_instances})")
    if args.list:
        return 0

    out_path = Path(args.out) if args.out else (
        REPO_ROOT / "fpe" / "results" / f"sweep_raw_{args.phase.replace(',', '-')}.jsonl"
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Resume: a study run is hours long, and losing it to one transient error
    # would be worse than the small risk of mixing two runs' records. Cells are
    # keyed on (phase, config, function), which is exactly what a cell is.
    done: dict[tuple, int] = {}
    if args.resume and out_path.exists():
        for line in out_path.read_text().splitlines():
            if not line.strip():
                continue
            r = json.loads(line)
            if r.get("sentinel"):
                continue
            key = (r.get("phase"), r.get("config"), r.get("function"))
            done[key] = done.get(key, 0) + 1
        complete = sum(1 for v in done.values() if v >= args.iterations)
        print(f"Resuming: {complete} cell(s) already complete in {out_path.name}")

    sink = out_path.open("a")
    client = bigquery.Client(project=project)
    results: list[dict] = []

    table_rows = client.get_table(f"{project}.{dataset}.{table}").num_rows
    ctx = {
        "project": project, "dataset": dataset, "table": table, "service": service,
        "iterations": args.iterations, "target_seconds": args.target_seconds,
        "adaptive": args.adaptive, "rows": args.rows, "table_rows": table_rows,
        "max_rows": args.max_rows if args.adaptive else min(args.rows, table_rows),
    }
    if args.adaptive:
        print(f"Adaptive sizing on: target {args.target_seconds:.0f}s/iteration, "
              f"up to {args.max_rows:,} rows "
              f"(source table has {table_rows:,}; replicated above that)")

    def emit(records: list[dict]) -> None:
        for rec in records:
            results.append(rec)
            sink.write(json.dumps(rec) + "\n")
        sink.flush()

    #: First cell of each phase, re-run at the end as a drift check.
    first_of_phase: dict[str, tuple[Deployment, Trial]] = {}

    for phase_name, cfg, trials in plan:
        first_of_phase.setdefault(phase_name, (cfg, trials[0]))
        pending = [t for t in trials
                   if done.get((phase_name, cfg.label, t.name), 0) < args.iterations]
        if not pending:
            print(f"\n=== [{phase_name}] {cfg.label} — already complete, skipping ===")
            continue

        print(f"\n=== [{phase_name}] {cfg.label} "
              f"(cpu={cfg.cpu} conc={cfg.concurrency} workers={cfg.workers} "
              f"threads={cfg.threads} {cfg.worker_class} slots={cfg.slots} "
              f"mem={cfg.memory} max={cfg.max_instances}) ===")
        if not args.skip_deploy:
            deploy(cfg)
            # Let the revision settle and the autoscaler report steady state.
            time.sleep(10)
        for trial in pending:
            emit(run_cell(client, ctx, phase_name, cfg, trial))

    # --- drift sentinel ---------------------------------------------------
    # Configs are not interleaved (a deploy costs more than the iterations do),
    # so drift over a multi-hour phase is a real threat to every comparison in
    # it. Re-running the phase's first cell at the end tests for it directly.
    if not args.no_sentinel and not args.skip_deploy:
        for phase_name, (cfg, trial) in first_of_phase.items():
            print(f"\n=== [{phase_name}] drift sentinel: re-running {cfg.label} ===")
            deploy(cfg)
            time.sleep(10)
            emit(run_cell(client, ctx, phase_name, cfg, trial, sentinel=True))

    if probe_names:
        print(f"\n### Limit probes (deployment: {LIMIT_DEPLOYMENT.label}) ###")
        if not args.skip_deploy:
            deploy(LIMIT_DEPLOYMENT)
            time.sleep(10)
        probe_ctx = {
            "project": project,
            "dataset": dataset,
            "table": table,
            "service": service,
            "clear_table": os.environ.get("FPE_TABLE_CLEAR", "pii_clear"),
            "rows": args.rows,
            # Limit probes are about ceilings and shapes, not throughput, so
            # they use a smaller row count to stay cheap. The short-circuit
            # probe especially: one HTTP request per row at 500k rows would
            # take hours and prove nothing extra.
            "small_rows": min(args.rows, 20_000),
            "skip_deploy": args.skip_deploy,
        }
        for name in probe_names:
            print(f"\n=== [{name}] ===")
            try:
                for rec in PROBES[name](client, probe_ctx):
                    results.append(rec)
                    sink.write(json.dumps(rec) + "\n")
                    sink.flush()
            except Exception as exc:  # noqa: BLE001
                print(f"    ! probe {name} aborted: {str(exc)[:300]}")

    sink.close()
    print(f"\nRaw results appended to {out_path}")
    print(f"Analyse with: python fpe/scripts/analyze.py {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
