#!/usr/bin/env python3
"""Calibration for the synthetic `cpu` / `mixed` modes: rounds <-> µs/row.

The whole scaling study rests on being able to *dial* a per-row cost, so that a
workload can be placed at 5 µs/row (a native-code PEP) or 118 µs/row
(pure-Python FF3-1) on demand. `_cpu_burn` in the service does `rounds` sha256
iterations per row, which is a fixed work unit; this module converts between
that unit and wall-clock µs.

Two curves, because they differ by ~2x and only one of them matters:

    LOCAL       measured on the workstation. Useful for a sanity check and for
                designing a matrix offline, but NOT what the sweep should use.
    CONTAINER   measured on Cloud Run by `sweep.py --phase calibrate_cpu`.
                This is the one the sweep and the remote-function generator
                read, because the container's vCPU is what actually runs.

Re-measure the container curve with:

    python fpe/scripts/sweep.py --phase calibrate_cpu

then paste the printed Curve(...) over CONTAINER below and regenerate the
remote functions.

The model is deliberately two-parameter:

    us_per_row = floor_us + us_per_round * rounds

`floor_us` is the per-row loop, encode and hex cost with rounds=0 — small, but
it is the reason a 1 µs/row target is not reachable by this mode at all.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Curve:
    """A linear rounds -> µs/row calibration measured on one platform."""

    floor_us: float
    us_per_round: float
    measured_on: str

    def us_for_rounds(self, rounds: int) -> float:
        return self.floor_us + self.us_per_round * rounds

    def rounds_for_us(self, target_us: float) -> int:
        """Rounds needed to hit `target_us` per row, or 0 if it is below the floor.

        Returning 0 rather than raising is deliberate: a target below the floor
        means "as cheap as this mode can be", which is a legitimate corner of
        the workload plane. Callers that care should compare `us_for_rounds` of
        the answer against the target — `check()` does exactly that.
        """
        if target_us <= self.floor_us:
            return 0
        return max(1, round((target_us - self.floor_us) / self.us_per_round))

    def check(self, target_us: float) -> tuple[int, float, float]:
        """(rounds, achievable µs/row, relative error) for a requested target."""
        rounds = self.rounds_for_us(target_us)
        actual = self.us_for_rounds(rounds)
        return rounds, actual, (actual - target_us) / target_us if target_us else 0.0


#: Workstation, AMD EPYC 7B12, CPython 3.12.7, 2026-08-27. Median of 7 passes
#: over 3,000 rows at each of rounds in {0, 10, 20, 50, 100, 200, 400, 800,
#: 1600}; the per-round slope varied by ~9% across that span, which is the
#: honest precision of this number on a shared VM.
LOCAL = Curve(floor_us=0.40, us_per_round=0.4666, measured_on="workstation-epyc-7b12")

#: Cloud Run container, 4 vCPU, gen2, us-central1, 2026-08-27. Measured by
#: `sweep.py --phase calibrate_cpu` — one sync worker at containerConcurrency 1,
#: so nothing contends for a core and this is the container's raw single-slot
#: cost. Theil-Sen over rounds in {4 .. 512}, read off the service logs rather
#: than query wall clock (at 1 µs/row the wall clock is almost entirely transit).
#: Raw records: fpe/results/sweep_raw_study_calib.jsonl.
#:
#: The container is **2.33x slower per round than the workstation**, which is
#: why the local curve cannot be used to dial a target: it would have placed
#: every workload point at less than half its intended cost.
CONTAINER = Curve(floor_us=1.09, us_per_round=1.0879,
                  measured_on="cloud-run-4vcpu-gen2")

#: Per-row costs the study is keyed on, in µs. W2 (5) is the production-PEP
#: hypothesis, W3 (30) a moderate transform, W4 (118) today's FF3-1 baseline.
TARGET_US = (2, 5, 10, 20, 30, 50, 118, 200)

#: target µs/row -> `rounds`, on the container. This is the lookup the sweep and
#: fpe/scripts/generate_remote_functions.py both read.
ROUNDS_FOR_US: dict[int, int] = {
    int(t): CONTAINER.rounds_for_us(t) for t in TARGET_US
}


def describe(curve: Curve) -> str:
    lines = [
        f"{curve.measured_on}: us_per_row = {curve.floor_us:.2f} "
        f"+ {curve.us_per_round:.4f} * rounds",
        f"{'target µs':>10} {'rounds':>8} {'achievable':>12} {'error':>8}",
    ]
    for t in TARGET_US:
        rounds, actual, err = curve.check(t)
        lines.append(f"{t:>10} {rounds:>8} {actual:>11.2f}µs {err:>+7.1%}")
    return "\n".join(lines)


if __name__ == "__main__":
    print(describe(LOCAL))
    print()
    print(describe(CONTAINER))
