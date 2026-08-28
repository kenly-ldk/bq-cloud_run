#!/usr/bin/env python3
"""Turn raw sweep JSONL into the markdown tables that go in docs/.

    python fpe/scripts/analyze.py fpe/results/sweep_raw_all.jsonl
    python fpe/scripts/analyze.py fpe/results/*.jsonl --out fpe/results/tables.md

Iterations are collapsed with the median, not the mean: Cloud Run cold-ish
instances and BigQuery slot contention both produce occasional long tails that
would drag a mean around and imply differences that aren't there.

For the scaling-study phases the median alone is not enough, so those tables
also carry min, max and an explicit **overlap verdict**. Two runs of the same
configuration have been measured differing by 2.16x, which is wider than most
of the effects being looked for; a difference in medians between two configs
whose [min, max] ranges overlap is not evidence of anything. `rank_by_rps`
enforces that rather than leaving it to the reader.
"""

from __future__ import annotations

import argparse
import json
import re
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


# --- the overlap rule -----------------------------------------------------


def _stats(samples: list[dict]) -> dict:
    rps = [s["rps"] for s in samples if isinstance(s.get("rps"), (int, float))]
    if not rps:
        return {}
    return {
        "n": len(rps),
        "median": statistics.median(rps),
        "min": min(rps),
        "max": max(rps),
        "spread": max(rps) / min(rps) if min(rps) else float("inf"),
    }


def rank_by_rps(records: list[dict], *group_keys: str) -> list[tuple]:
    """(key, stats, verdict) per config, best median first.

    `verdict` is the honest comparison against the best-median config:

        "best"              highest median
        "slower"            its max is below the best's min — a real difference
        "indistinguishable" the ranges overlap, so the medians say nothing

    The point is that the third case is the common one. Reporting a ranking
    without it is how a 1.41x sweep gets read as a result when the run-to-run
    noise inside it is 2.16x.
    """
    rows = []
    for key, samples in group(records, *group_keys).items():
        st = _stats([s for s in samples if not s.get("sentinel")])
        if st:
            rows.append((key, st))
    if not rows:
        return []
    rows.sort(key=lambda kv: -kv[1]["median"])
    best = rows[0][1]
    out = []
    for i, (key, st) in enumerate(rows):
        if i == 0:
            verdict = "best"
        elif st["max"] < best["min"]:
            verdict = "slower"
        else:
            verdict = "indistinguishable"
        out.append((key, st, verdict))
    return out


def render_ranked(records: list[dict], group_keys: list[str],
                  label_headers: list[str], extra: list[tuple[str, str, str]] = ()) -> str:
    """Ranked table with min/median/max and the overlap verdict.

    `extra` is (header, record key, format) for service-side columns worth
    carrying alongside — µs/row, reconstructed concurrency, and so on.
    """
    ranked = rank_by_rps(records, *group_keys)
    if not ranked:
        return "_no records_"
    best = ranked[0][1]["median"]
    by_key = group(records, *group_keys)

    rows = []
    for key, st, verdict in ranked:
        samples = by_key[key]
        cells = [
            *[str(k) for k in key],
            f"{st['median']:,.0f}", f"{st['min']:,.0f}", f"{st['max']:,.0f}",
            f"{st['spread']:.2f}x",
            f"{st['median'] / best:.2f}x",
            {"best": "**best**", "slower": "slower",
             "indistinguishable": "= best (overlaps)"}[verdict],
        ]
        for _, rec_key, spec in extra:
            cells.append(_fmt(_median(samples, rec_key), spec))
        cells.append(str(st["n"]))
        rows.append(cells)

    headers = [*label_headers, "Median rows/s", "Min", "Max", "Spread",
               "vs best", "Verdict", *[h for h, _, _ in extra], "n"]
    body = table(headers, rows)

    tied = [k for k, _, v in ranked if v == "indistinguishable"]
    if tied:
        names = ", ".join("/".join(str(x) for x in k) for k in tied)
        body += (f"\n\nIndistinguishable from the best config at these sample "
                 f"sizes (ranges overlap): {names}.")
    return body


def render_drift(records: list[dict]) -> str:
    """Did the measurement drift over the phase?

    Configs are not interleaved, so a slow trend in BigQuery slot availability
    or Cloud Run placement would show up as a fake ordering effect. The sweep
    re-runs each phase's first config at the end; if that re-run no longer
    overlaps the original, every comparison in the phase is suspect.
    """
    lines = []
    for (phase, config), samples in group(
            [r for r in records if r.get("sentinel") is not None],
            "phase", "config").items():
        first = _stats([s for s in samples if not s.get("sentinel")])
        again = _stats([s for s in samples if s.get("sentinel")])
        if not first or not again:
            continue
        overlaps = first["min"] <= again["max"] and again["min"] <= first["max"]
        lines.append(
            f"- `{phase}` / `{config}`: first {first['median']:,.0f} rows/s "
            f"[{first['min']:,.0f}–{first['max']:,.0f}], re-run "
            f"{again['median']:,.0f} [{again['min']:,.0f}–{again['max']:,.0f}] — "
            + ("**no drift** (ranges overlap)" if overlaps
               else "**DRIFTED — comparisons in this phase are not safe**")
        )
    return "\n".join(lines)


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
    "cpu": lambda r: ("Vertical scaling — vCPU, workers set equal to vCPU",
                      render_perf(r, "cpu", ["cfg_cpu", "cfg_workers"],
                                  ["vCPU", "workers"])),
    "scale": lambda r: ("Horizontal scaling — maxScale",
                        render_perf(r, "scale", ["cfg_max_instances"],
                                    ["maxScale"])),
    "modes": lambda r: ("Cost decomposition by workload",
                        render_perf(r, "modes", ["mode"], ["mode"])),
    "concurrency_only": lambda r: (
        "containerConcurrency isolated (cpu=4, workers=4, sync, maxScale=1)",
        render_perf(r, "concurrency_only", ["cfg_concurrency"],
                    ["containerConcurrency"])),
    "workers_only": lambda r: (
        "Worker count isolated (cpu=4, containerConcurrency=80, sync, maxScale=1)",
        render_perf(r, "workers_only", ["cfg_workers"], ["workers"])),
    "throttling": lambda r: ("CPU throttling",
                             render_perf(r, "throttling", ["cfg_cpu_throttling"],
                                         ["cpu-throttling"])),
}


# --- scaling study --------------------------------------------------------

#: Service-side columns the study tables carry alongside throughput. µs/row is
#: how a reader locates their own workload on the table; the reconstructed
#: concurrency is what the Phase 2 rule predicts and so is the thing being
#: tested, not decoration.
STUDY_EXTRA = [
    ("µs/row (svc)", "us_per_row_median", ".0f"),
    ("Peak concurrency", "concurrency_peak", ".0f"),
    ("Mean concurrency", "concurrency_mean", ".1f"),
    ("Rows/iteration", "rows", ",.0f"),
]


def render_calibrate_cpu(records: list[dict]) -> tuple[str, str]:
    """Fit the container's rounds -> µs/row curve and print it ready to paste.

    Read off the service logs rather than query wall clock: wall clock includes
    BigQuery scheduling and transit, which at 1 µs/row is most of it.
    """
    pts = []
    for (fn,), samples in group(records, "function").items():
        m = re.search(r"cpu_r(\d+)_b", str(fn))
        us = _median(samples, "us_per_row_median")
        if m and us:
            pts.append((int(m.group(1)), us))
    pts.sort()
    if not pts:
        return "Container CPU calibration", "_no records_"

    rows = [[f"{r:,}", f"{u:.2f}"] for r, u in pts]
    body = table(["`rounds`", "µs/row measured on the container"], rows)

    # Theil-Sen (median of pairwise slopes), not least squares. The cheap end
    # of this curve is the end the study cares about — a native-code PEP lives
    # at single-digit µs/row — but the expensive end has the largest absolute
    # residuals, so a least-squares fit is steered by exactly the points that
    # matter least, and one throttled run at the top can drive the intercept
    # negative. The median slope ignores it.
    #
    # rounds < 4 is excluded: there the per-row loop and hex overhead is a
    # large share of the measurement and would tilt the slope on its own.
    fit = [(r, u) for r, u in pts if r >= 4]
    if len(fit) >= 2:
        slopes = [(u1 - u0) / (r1 - r0)
                  for i, (r0, u0) in enumerate(fit)
                  for r1, u1 in fit[i + 1:] if r1 != r0]
        slope = statistics.median(slopes)
        floor = max(0.0, statistics.median(u - slope * r for r, u in fit))
        worst = max(fit, key=lambda ru: abs(ru[1] - (floor + slope * ru[0])))
        body += (
            f"\n\nTheil-Sen fit over `rounds` >= 4: "
            f"**µs/row = {floor:.2f} + {slope:.4f} x rounds**  "
            f"(largest residual at rounds={worst[0]}: "
            f"{worst[1]:.1f} measured vs {floor + slope * worst[0]:.1f} fitted)\n\n"
            "Paste into `fpe/scripts/calibration.py`:\n\n"
            "```python\n"
            f"CONTAINER = Curve(floor_us={floor:.2f}, "
            f"us_per_round={slope:.4f}, measured_on=\"cloud-run-4vcpu-gen2\")\n"
            "```\n\n"
            "Then regenerate the remote functions — the cost parameters are "
            "baked into the function names, so a re-calibration renames them "
            "rather than silently changing what a stale name measures."
        )
    return "Container CPU calibration — `rounds` to µs/row", body


def render_profile(records: list[dict]) -> tuple[str, str]:
    """The two numbers the decision guide is keyed on, per workload point.

    Measured at one slot and containerConcurrency 1 — the same conditions a
    reader would profile their own service under, because `cpu_share` measures
    contention rather than the workload as soon as requests overlap.
    """
    rows = []
    for (wid,), samples in sorted(group(records, "workload").items(),
                                  key=lambda kv: str(kv[0])):
        us = _median(samples, "us_per_row_median")
        cpu = _median(samples, "cpu_us_per_row_median")
        share = _median(samples, "cpu_share_median")
        rows.append([
            str(wid),
            f"`{samples[0].get('function', '?')}`",
            _fmt(us, ",.1f"), _fmt(cpu, ",.1f"),
            _fmt(share, ".3f"),
            # wait/service is what the slot rule is a function of, and it falls
            # straight out of the two measured numbers.
            "inf" if share is not None and share <= 0
            else _fmt((1 - share) / share if share else None, ".1f"),
            _fmt(_median(samples, "rps"), ",.0f"),
        ])
    return (
        "Workload profile — the two numbers the guide is keyed on "
        "(1 slot, containerConcurrency 1)",
        table(["Workload", "Function", "µs/row (wall)", "µs/row (CPU)",
               "CPU share", "wait/service", "Rows/s at 1 slot"], rows),
    )


def render_workload_matrix(records: list[dict]) -> tuple[str, str]:
    """One ranked worker-model table per workload point.

    Split by workload rather than shown as one grid, because the whole claim is
    that the ranking *changes* between workloads. Putting them in one table
    would invite reading down a column that has no common meaning.
    """
    sections = []
    for (wid,), recs in sorted(group(records, "workload").items(),
                               key=lambda kv: str(kv[0])):
        fn = recs[0].get("function", "?")
        sections.append(
            f"**{wid}** — `{fn}`\n\n"
            + render_ranked(recs, ["cfg_worker_class", "cfg_workers",
                                   "cfg_threads", "cfg_slots"],
                            ["class", "workers", "threads", "slots"],
                            STUDY_EXTRA)
        )
    return ("Workload x worker model (cpu=4, containerConcurrency=80, maxScale=1)",
            "\n\n".join(sections))


def render_rule_check(records: list[dict]) -> tuple[str, str]:
    return (
        "Falsification: the fitted slot rule against a held-out workload",
        render_ranked(records, ["cfg_slots", "cfg_workers", "cfg_threads"],
                      ["slots", "workers", "threads"], STUDY_EXTRA),
    )


def render_scale_axis(records: list[dict]) -> tuple[str, str]:
    """Vertical and horizontal, split per workload — different questions each.

    Splitting by workload is not cosmetic. The two workloads run three orders of
    magnitude apart (400,000 rows/s against 11,000), so pooling them puts a 44x
    "spread" in a column that is supposed to report run-to-run noise, and every
    verdict in it becomes meaningless. The two arms are told apart by the config
    label, because the vCPU arm also runs at maxScale 1 and so cannot be
    separated on `cfg_max_instances` alone.
    """
    parts = []
    for (wid,), recs in sorted(group(records, "workload").items(),
                               key=lambda kv: str(kv[0])):
        vertical = [r for r in recs if "-cpu" in str(r.get("config", ""))]
        horizontal = [r for r in recs if "-max" in str(r.get("config", ""))]
        if vertical:
            parts.append(
                f"**{wid} vertical — vCPU, worker count rescaled with it**\n\n"
                + render_ranked(vertical, ["cfg_cpu", "cfg_workers", "cfg_threads"],
                                ["vCPU", "workers", "threads"], STUDY_EXTRA))
        if horizontal:
            parts.append(
                f"**{wid} horizontal — maxScale**\n\n"
                + render_ranked(horizontal, ["cfg_max_instances"], ["maxScale"],
                                STUDY_EXTRA + [("Instances", "instances", ".0f")]))
    return "Vertical vs horizontal scaling, per workload", "\n\n".join(parts)


def render_batch_at_cheap(records: list[dict]) -> tuple[str, str]:
    return (
        "Batch size at a cheap workload — does the flat curve stay flat?",
        render_ranked(records, ["batch"], ["`max_batching_rows`"],
                      STUDY_EXTRA + [("Actual rows/request", "batch_rows_median",
                                      ",.0f")]),
    )


RENDERERS.update({
    "calibrate_cpu": render_calibrate_cpu,
    "profile": render_profile,
    "workload_matrix": render_workload_matrix,
    "rule_check": render_rule_check,
    "scale_axis": render_scale_axis,
    "batch_at_cheap": render_batch_at_cheap,
})


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
    "input_size": lambda r: (
        "Per-row input size: the 256 KiB batching budget vs the 5 MiB hard limit",
        table(["Bytes/row", "Result", "Rows per request", "HTTP requests"],
              [[f"{x['bytes_per_row']:,}", "OK" if x["succeeded"] else "FAILED",
                _fmt(x.get("batch_rows_max"), ",.0f"),
                _fmt(x.get("log_batches"), ",.0f")]
               for x in sorted(r, key=lambda x: x["bytes_per_row"])])),
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

    drift = render_drift(records)
    if drift:
        sections.append(
            "### Drift check\n\n"
            "Configurations are not interleaved — a deploy costs more wall clock "
            "than the iterations it precedes — so each phase re-runs its first "
            "configuration at the end. If the re-run no longer overlaps the "
            "original, the ordering of everything in that phase is suspect.\n\n"
            f"{drift}\n"
        )

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
