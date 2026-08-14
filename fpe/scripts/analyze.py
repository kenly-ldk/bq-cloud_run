#!/usr/bin/env python3
"""Turn raw sweep JSONL into the markdown tables that go in docs/.

    python fpe/scripts/analyze.py fpe/scripts/sweep_raw_all.jsonl
    python fpe/scripts/analyze.py fpe/scripts/*.jsonl --out docs/results/tables.md

Iterations are collapsed with the median, not the mean: Cloud Run cold-ish
instances and BigQuery slot contention both produce occasional long tails that
would drag a mean around and imply differences that aren't there.
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path


def load(paths: list[str]) -> list[dict]:
    rows: list[dict] = []
    for p in paths:
        for line in Path(p).read_text().splitlines():
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def _median(values: list, key: str):
    vals = [v[key] for v in values if isinstance(v.get(key), (int, float))]
    return statistics.median(vals) if vals else None


def _fmt(value, spec: str = "", dash: str = "—") -> str:
    if value is None:
        return dash
    try:
        return format(value, spec) if spec else str(value)
    except (TypeError, ValueError):
        return str(value)


def table(headers: list[str], rows: list[list[str]]) -> str:
    out = ["| " + " | ".join(headers) + " |",
           "| " + " | ".join(["---"] * len(headers)) + " |"]
    out += ["| " + " | ".join(r) + " |" for r in rows]
    return "\n".join(out)


def group(records: list[dict], *keys: str) -> dict[tuple, list[dict]]:
    out: dict[tuple, list[dict]] = defaultdict(list)
    for r in records:
        out[tuple(r.get(k) for k in keys)].append(r)
    return dict(out)


# --- per-phase renderers --------------------------------------------------


def render_perf(records: list[dict], phase: str, group_keys: list[str],
                label_headers: list[str]) -> str:
    rows = []
    for key, samples in sorted(group(records, *group_keys).items(),
                               key=lambda kv: [
                                   (v if isinstance(v, (int, float)) else str(v))
                                   for v in kv[0]]):
        el = _median(samples, "elapsed_s")
        rows.append(
            [*[str(k) for k in key],
             _fmt(el, ".2f"),
             _fmt(_median(samples, "rps"), ",.0f"),
             _fmt(_median(samples, "us_per_row_median"), ".0f"),
             _fmt(_median(samples, "instances"), ".0f"),
             _fmt(_median(samples, "worker_processes"), ".0f"),
             _fmt(_median(samples, "inflight_max"), ".0f"),
             _fmt(_median(samples, "batch_rows_median"), ",.0f"),
             str(len(samples))]
        )
    return table(
        [*label_headers, "Median elapsed (s)", "Rows/s", "µs/row (svc)",
         "Instances", "Worker procs", "Peak in-flight", "Actual batch rows", "n"],
        rows,
    )


RENDERERS = {
    "batch": lambda r: ("Batch size sweep — `max_batching_rows`",
                        render_perf(r, "batch", ["batch"], ["max_batching_rows"])),
    "concurrency": lambda r: (
        "containerConcurrency x gunicorn worker model (all at 4 vCPU)",
        render_perf(r, "concurrency",
                    ["cfg_concurrency", "cfg_workers", "cfg_threads",
                     "cfg_worker_class"],
                    ["containerConcurrency", "workers", "threads", "class"])),
    "cpu": lambda r: ("Vertical scaling — vCPU with workers == vCPU",
                      render_perf(r, "cpu", ["cfg_cpu", "cfg_workers"],
                                  ["vCPU", "workers"])),
    "scale": lambda r: ("Horizontal scaling — maxScale",
                        render_perf(r, "scale", ["cfg_max_instances"],
                                    ["maxScale"])),
    "modes": lambda r: ("Cost decomposition by workload",
                        render_perf(r, "modes", ["mode"], ["mode"])),
    "throttling": lambda r: ("CPU throttling",
                             render_perf(r, "throttling", ["cfg_cpu_throttling"],
                                         ["cpu-throttling"])),
}


def render_limits_queries(records: list[dict]) -> tuple[str, str]:
    rows = [
        [str(r["concurrent_queries"]), f"{r['succeeded']}/{r['concurrent_queries']}",
         _fmt(r.get("wall_s"), ".1f"), _fmt(r.get("aggregate_rps"), ",.0f"),
         "yes" if r.get("quota_error") else "no"]
        for r in sorted(records, key=lambda r: r["concurrent_queries"])
    ]
    return (
        "Concurrent queries containing remote functions (documented limit: 10/project)",
        table(["Queries fired", "Succeeded", "Wall (s)", "Aggregate rows/s",
               "Quota error"], rows),
    )


def render_limits_response(records: list[dict]) -> tuple[str, str]:
    # Early runs keyed this probe on batch size; it is keyed on reply width now,
    # because BigQuery's ~11,905-row request cap makes row count unable to reach
    # the ceiling. Tolerate both so old raw files still render.
    def _key(r: dict):
        return r.get("reply_width") or r.get("batch") or 0

    rows = [
        [_fmt(r.get("reply_width"), ",") if r.get("reply_width")
         else f"(batch {r.get('batch', 0):,})",
         _fmt(r.get("rows_per_request"), ","),
         _fmt(r.get("est_response_mb"), ".1f"),
         "OK" if r["succeeded"] else "FAILED",
         _fmt(r.get("elapsed_s"), ".2f")]
        for r in sorted(records, key=_key)
    ]
    return (
        "HTTP response size ceiling via `bloat` mode "
        "(documented limit: 15 MB for Cloud Run / gen2)",
        table(["Reply width (B)", "Rows/request", "Est. response (MB)",
               "Result", "Elapsed (s)"], rows),
    )


def render_limits_batching(records: list[dict]) -> tuple[str, str]:
    rows = [
        [str(r.get("requested_batch")),
         "OK" if r["succeeded"] else "FAILED",
         _fmt(r.get("batch_rows_median"), ",.0f"),
         _fmt(r.get("batch_rows_max"), ",.0f"),
         _fmt(r.get("log_batches"), ",.0f"),
         _fmt(r.get("elapsed_s"), ".2f")]
        for r in records
    ]
    return (
        "Requested vs actual batch size — is `max_batching_rows` honoured?",
        table(["max_batching_rows", "Result", "Actual median rows",
               "Actual max rows", "HTTP requests", "Elapsed (s)"], rows),
    )


def render_limits_retries(records: list[dict]) -> tuple[str, str]:
    rows = [
        [r["variant"], "OK" if r["succeeded"] else "FAILED",
         _fmt(r.get("elapsed_s"), ".2f"),
         _fmt(r.get("endpoint_invocations"), ",.0f"),
         r.get("expectation", "")]
        for r in records
    ]
    return (
        "Retry behaviour (BigQuery retries 408/429/500/503/504, up to 20 attempts)",
        table(["Variant", "Query result", "Elapsed (s)", "Endpoint invocations",
               "Expectation"], rows),
    )


def render_antipattern(records: list[dict]) -> tuple[str, str]:
    rows = [
        [r["variant"], "OK" if r["succeeded"] else "FAILED",
         _fmt(r.get("elapsed_s"), ".2f"),
         _fmt(r.get("log_batches"), ",.0f"),
         _fmt(r.get("batch_rows_median"), ",.0f"),
         _fmt(r.get("rows"), ",.0f")]
        for r in records
    ]
    return (
        "Short-circuit evaluation disables batching",
        table(["Query shape", "Result", "Elapsed (s)", "HTTP requests",
               "Median rows/request", "Rows"], rows),
    )


def _variant_table(records: list[dict], title: str, first_col: str) -> tuple[str, str]:
    """Shared shape for the query-pattern probes: one row per SQL variant."""
    rows = []
    equivalence = []
    for r in records:
        if r.get("probe") == "equivalence":
            equivalence.append(
                f"`{r['variant']}`: {r['mismatches']} mismatches of "
                f"{r['compared']} rows — "
                f"**{'equivalent' if r['equivalent'] else 'NOT equivalent'}**"
            )
            continue
        rows.append([
            f"`{r['variant']}`",
            "OK" if r.get("succeeded") else "FAILED",
            _fmt(r.get("elapsed_s"), ".2f"),
            _fmt(r.get("log_batches"), ","),
            _fmt(r.get("log_rows_total"), ","),
            _fmt(r.get("batch_rows_median"), ",.0f"),
        ])
    body = table([first_col, "Result", "Elapsed (s)", "HTTP requests",
                  "Rows to service", "Median rows/request"], rows)
    if equivalence:
        body += "\n\nResult-equivalence checks:\n\n" + "\n".join(
            f"- {e}" for e in equivalence)
    return title, body


PROBE_RENDERERS = {
    "limits_queries": render_limits_queries,
    "limits_response": render_limits_response,
    "limits_batching": render_limits_batching,
    "limits_retries": render_limits_retries,
    "antipattern": render_antipattern,
    "search_pattern": lambda r: _variant_table(
        r, "Search: tokenize the term vs detokenize the column", "Approach"),
    "access_control": lambda r: _variant_table(
        r, "Authorized-view + entitlement patterns", "Pattern"),
    "placement": lambda r: _variant_table(
        r, "Where the remote function sits in the plan", "Shape"),
}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("paths", nargs="+")
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    records = load(args.paths)
    if not records:
        print("no records found", file=sys.stderr)
        return 1

    by_phase = group(records, "phase")
    sections: list[str] = []

    for (phase,), recs in by_phase.items():
        if phase in RENDERERS:
            title, body = RENDERERS[phase](recs)
        elif phase in PROBE_RENDERERS:
            title, body = PROBE_RENDERERS[phase](recs)
        else:
            continue
        sections.append(f"### {title}\n\n{body}\n")

    out = "\n".join(sections)
    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(out)
        print(f"Wrote {len(sections)} table(s) to {args.out}")
    else:
        print(out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
