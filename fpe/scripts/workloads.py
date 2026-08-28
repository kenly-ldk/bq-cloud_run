#!/usr/bin/env python3
"""The workload points the scaling study is keyed on.

One place, because three things have to agree on them: the sweep that runs the
matrix, the generator that creates the BigQuery remote functions, and the
analysis that labels the results. A workload is fully described by the mode and
its cost parameters, and its remote-function name is derived from them — so
changing a cost changes the function name and you cannot silently measure a
stale function.

The two coordinates every point is placed on:

    cpu_us    per-row CPU cost in µs   ("service time")
    wait_ms   per-row blocking wait    ("wait time")

Their ratio is what the Phase 2 rule is a function of:

    optimal concurrent slots ~= cores x (1 + wait / service)

Deviation from plans/cloud-run-scaling-decision-guide.md, recorded on purpose:
the plan specified 20 ms per-row waits for W5/W6. That is not measurable here.
A 20 ms per-row sleep costs 50 rows/s per slot, so saturating 128 slots for 30 s
needs a request that takes 5,000 x 20 ms = 100 s, and a single-slot run of the
same row count would take over two hours. 2 ms keeps the same *shape* — a pure
per-row remote call — inside a measurable dynamic range, and is a realistic
same-region API latency rather than a cross-region one.
"""

from __future__ import annotations

from dataclasses import dataclass, field

try:
    from calibration import CONTAINER
except ImportError:  # imported as a package/module path
    from .calibration import CONTAINER  # type: ignore[no-redef]


@dataclass(frozen=True)
class Workload:
    """One point on the CPU/wait plane, and the remote function that realises it."""

    id: str
    mode: str
    represents: str
    #: Nominal per-row CPU cost in µs. Realised via `rounds` on the container
    #: curve for the synthetic modes; descriptive only for `noop`/`fpe_decrypt`.
    cpu_us: float = 0.0
    #: Nominal per-row blocking wait in ms. Integer only — `time.sleep` below
    #: ~1 ms carries tens of percent of timer-granularity error.
    wait_ms: int = 0
    batch: int = 5000

    @property
    def rounds(self) -> int:
        """`rounds` for the synthetic modes; 0 where the cost is not synthetic."""
        if self.mode not in ("cpu", "mixed"):
            return 0
        return CONTAINER.rounds_for_us(self.cpu_us)

    @property
    def params(self) -> dict[str, str]:
        """user_defined_context entries beyond `mode`."""
        if self.mode == "mixed":
            return {"rounds": str(self.rounds), "sleep_ms": str(self.wait_ms)}
        if self.mode == "cpu":
            return {"rounds": str(self.rounds)}
        if self.mode in ("io", "io_row"):
            return {"sleep_ms": str(self.wait_ms)}
        return {}

    @property
    def tag(self) -> str:
        """Parameter suffix for the function name. Empty for parameterless modes."""
        if self.mode == "mixed":
            return f"_r{self.rounds}s{self.wait_ms}"
        if self.mode == "cpu":
            return f"_r{self.rounds}"
        if self.mode in ("io", "io_row"):
            return f"_s{self.wait_ms}"
        return ""

    @property
    def function(self) -> str:
        return f"{self.mode}{self.tag}_b{self.batch}"

    @property
    def wait_over_service(self) -> float:
        """wait/service. inf for a pure-wait workload, 0 for a pure-CPU one."""
        if self.cpu_us <= 0:
            return float("inf") if self.wait_ms else 0.0
        return (self.wait_ms * 1000.0) / self.cpu_us

    def predicted_slots(self, cores: int) -> float:
        """cores x (1 + wait/service) — the rule Phase 2 sets out to falsify."""
        return cores * (1.0 + self.wait_over_service)

    @property
    def rows_per_s_per_slot(self) -> float:
        """Throughput ceiling for ONE slot, from the nominal costs alone.

        Used to size runs adaptively before anything has been measured; the
        pilot query supersedes it as soon as there is a real number.
        """
        per_row_s = (self.cpu_us / 1e6) + (self.wait_ms / 1e3)
        return 1e9 if per_row_s <= 0 else 1.0 / per_row_s


#: Phase 1's workload points. W2 and W5 map onto real customer deployments and
#: are the two run at full worker-model coverage; see MATRIX in sweep.py.
WORKLOADS: dict[str, Workload] = {
    w.id: w
    for w in (
        Workload("W1", "noop", "transit floor"),
        Workload("W2", "mixed", "production Protegrity PEP (native crypto)",
                 cpu_us=5),
        Workload("W3", "mixed", "moderate transform", cpu_us=30),
        Workload("W4", "fpe_decrypt", "today's baseline, pure-Python FF3-1",
                 cpu_us=118),
        Workload("W5", "io_row", "Developer Edition / any per-row remote call",
                 wait_ms=2),
        Workload("W6", "mixed", "realistic hybrid, wait/service = 10",
                 cpu_us=100, wait_ms=1),
        # Held out of the Phase 1 fit. Phase 2 predicts its optimum from the
        # rule fitted on W1-W6 and then measures it; a rule that was only ever
        # fitted is not a finding.
        Workload("W7", "mixed", "HELD OUT for falsification, wait/service = 20",
                 cpu_us=50, wait_ms=1),
    )
}

#: Fitted on these; W7 is the held-out test point.
FIT_SET = ("W1", "W2", "W3", "W4", "W5", "W6")
HELD_OUT = "W7"


def describe() -> str:
    rows = [
        f"{'id':<4} {'mode':<11} {'fn':<22} {'cpu µs':>7} {'wait ms':>8}"
        f" {'w/s':>8} {'slots@4':>8} {'rows/s/slot':>12}  represents"
    ]
    for w in WORKLOADS.values():
        ratio = w.wait_over_service
        rows.append(
            f"{w.id:<4} {w.mode:<11} {w.function:<22} {w.cpu_us:>7.0f}"
            f" {w.wait_ms:>8} {ratio:>8.1f} {w.predicted_slots(4):>8.0f}"
            f" {w.rows_per_s_per_slot:>12,.0f}  {w.represents}"
        )
    return "\n".join(rows)


if __name__ == "__main__":
    print(describe())
