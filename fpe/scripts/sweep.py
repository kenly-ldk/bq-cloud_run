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

from config._loader import load, require  # noqa: E402

from google.cloud import bigquery  # noqa: E402


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


@dataclass(frozen=True)
class Trial:
    """One BigQuery query configuration run against a deployment."""

    mode: str
    batch: int
    column: str = "ssn"
    data_element: str = "ssn"


# The baseline used by every phase that is not varying infrastructure.
# 4 vCPU / 4 sync workers: workers == vCPU is the correct pairing for
# CPU-bound CPython, and is what Phase C exists to demonstrate.
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


PHASES = {
    "batch": phase_batch,
    "concurrency": phase_concurrency,
    "cpu": phase_cpu,
    "scale": phase_scale,
    "modes": phase_modes,
    "throttling": phase_throttling,
}


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


def fetch_logs(
    project: str,
    service: str,
    start: datetime,
    end: datetime,
    events: tuple[str, ...] = ("batch",),
) -> list[dict]:
    """Pull the structured logs the service emitted during a window.

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
    return [e.get("jsonPayload", {}) for e in entries if e.get("jsonPayload")]


def summarise_logs(payloads: list[dict]) -> dict:
    """Turn raw batch logs into the service-side view of one trial."""
    if not payloads:
        return {"log_batches": 0}

    rows = [int(p.get("rows", 0)) for p in payloads]
    inflight = [int(p.get("inflight", 0)) for p in payloads]
    per_row = [float(p.get("us_per_row", 0)) for p in payloads if p.get("us_per_row")]
    instances = {p.get("instance") for p in payloads if p.get("instance")}
    procs = {(p.get("instance"), p.get("pid")) for p in payloads}

    return {
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


def build_sql(project: str, dataset: str, table: str, trial: Trial, rows: int) -> str:
    fn = f"`{project}.{dataset}`.{trial.mode}_b{trial.batch}"
    return (
        f"SELECT SUM(LENGTH({fn}({trial.column}, '{trial.data_element}')))\n"
        f"FROM (SELECT {trial.column} FROM `{project}.{dataset}.{table}` "
        f"LIMIT {rows})"
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--phase", default="all",
                    help=f"one of {sorted(PHASES)} or 'all'")
    ap.add_argument("--rows", type=int, default=500_000)
    ap.add_argument("--iterations", type=int, default=3)
    ap.add_argument("--out", default=None)
    ap.add_argument("--list", action="store_true",
                    help="print the plan and exit without deploying")
    ap.add_argument("--skip-deploy", action="store_true",
                    help="reuse the currently deployed revision (single-config runs)")
    args = ap.parse_args()

    load()
    project = require("PROJECT_ID")
    dataset = require("FPE_DATASET")
    table = require("FPE_TABLE_TOKENIZED")
    service = require("FPE_SERVICE")

    known = sorted(PHASES) + sorted(PROBES)
    if args.phase == "all":
        names = sorted(PHASES)
    elif args.phase == "limits":
        names = sorted(PROBES)
    else:
        names = [args.phase]
    for n in names:
        if n not in PHASES and n not in PROBES:
            print(f"unknown phase {n!r}; known: {known} (or 'all' / 'limits')",
                  file=sys.stderr)
            return 2

    perf_names = [n for n in names if n in PHASES]
    probe_names = [n for n in names if n in PROBES]

    plan = [(n, cfg, trials) for n in perf_names for cfg, trials in PHASES[n]()]
    n_queries = sum(len(t) for _, _, t in plan) * args.iterations

    print(f"Sweep plan: {len(plan) + (1 if probe_names else 0)} deployment(s), "
          f"{n_queries} measured queries "
          f"({args.rows:,} rows each, {args.iterations} iteration(s))")
    for n, cfg, trials in plan:
        modes = ",".join(sorted({t.mode for t in trials}))
        batches = ",".join(str(t.batch) for t in trials)
        print(f"  [{n}] {cfg.label}: cpu={cfg.cpu} conc={cfg.concurrency} "
              f"w={cfg.workers} t={cfg.threads} {cfg.worker_class} "
              f"max={cfg.max_instances} | modes={modes} batches={batches}")
    for n in probe_names:
        print(f"  [{n}] limit probe on {LIMIT_DEPLOYMENT.label} "
              f"(cpu={LIMIT_DEPLOYMENT.cpu} conc={LIMIT_DEPLOYMENT.concurrency} "
              f"w={LIMIT_DEPLOYMENT.workers} max={LIMIT_DEPLOYMENT.max_instances})")
    if args.list:
        return 0

    out_path = Path(args.out) if args.out else (
        REPO_ROOT / "fpe" / "results" / f"sweep_raw_{args.phase}.jsonl"
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    sink = out_path.open("a")

    client = bigquery.Client(project=project)
    results: list[dict] = []

    for phase_name, cfg, trials in plan:
        print(f"\n=== [{phase_name}] {cfg.label} "
              f"(cpu={cfg.cpu} conc={cfg.concurrency} workers={cfg.workers} "
              f"threads={cfg.threads} {cfg.worker_class} max={cfg.max_instances}) ===")
        if not args.skip_deploy:
            deploy(cfg)
            # Let the revision settle and the autoscaler report steady state.
            time.sleep(10)

        for trial in trials:
            sql = build_sql(project, dataset, table, trial, args.rows)

            # Warm the instance: first request pays cipher construction and
            # any lazy import cost, which would otherwise pollute iteration 1.
            warm = build_sql(project, dataset, table, trial, 2000)
            try:
                run_query(client, warm)
            except Exception as exc:  # noqa: BLE001
                print(f"    ! warmup failed for {trial.mode}_b{trial.batch}: "
                      f"{str(exc)[:200]}")
                continue
            time.sleep(3)

            samples = []
            for i in range(args.iterations):
                t_start = datetime.now(timezone.utc) - timedelta(seconds=2)
                try:
                    stats = run_query(client, sql)
                except Exception as exc:  # noqa: BLE001
                    print(f"    ! {trial.mode}_b{trial.batch} iter {i+1} failed: "
                          f"{str(exc)[:300]}")
                    continue
                t_end = datetime.now(timezone.utc) + timedelta(seconds=2)

                stats["rps"] = args.rows / stats["elapsed_s"] if stats["elapsed_s"] else 0
                # Logs lag ingestion slightly; give them a moment to land.
                time.sleep(LOG_SETTLE_S)
                stats.update(summarise_logs(fetch_logs(project, service, t_start, t_end)))

                record = {
                    "phase": phase_name,
                    "config": cfg.label,
                    **{f"cfg_{k}": v for k, v in asdict(cfg).items() if k != "label"},
                    "mode": trial.mode,
                    "batch": trial.batch,
                    "rows": args.rows,
                    "iteration": i + 1,
                    **stats,
                }
                samples.append(record)
                results.append(record)
                sink.write(json.dumps(record) + "\n")
                sink.flush()

                print(
                    f"    {trial.mode}_b{trial.batch} it{i+1}: "
                    f"{stats['elapsed_s']:.2f}s  {stats['rps']:,.0f} rps  "
                    f"inst={stats.get('instances','?')} "
                    f"procs={stats.get('worker_processes','?')} "
                    f"inflight_max={stats.get('inflight_max','?')} "
                    f"batch_med={stats.get('batch_rows_median','?')} "
                    f"us/row={stats.get('us_per_row_median','?')}"
                )

            if samples:
                el = [s["elapsed_s"] for s in samples]
                print(f"    -> {trial.mode}_b{trial.batch} median "
                      f"{statistics.median(el):.2f}s "
                      f"({args.rows / statistics.median(el):,.0f} rps)")

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
