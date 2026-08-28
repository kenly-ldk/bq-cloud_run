"""BigQuery Remote Function endpoint — vendor-free FPE + benchmarking modes.

Speaks the BigQuery remote function protocol:

    POST /  {"requestId": ..., "caller": ..., "sessionUser": ...,
             "userDefinedContext": {...},
             "calls": [[value, data_element], ...]}
    200     {"replies": [...]}                     # same length/order as calls
    4xx     {"errorMessage": "..."}

`mode` comes from the function's user_defined_context, so one deployed service
backs every remote function in fpe/sql/. Modes:

    fpe_encrypt   FF3-1 format-preserving encryption      (CPU-bound, ~73us/row)
    fpe_decrypt   FF3-1 inverse
    hmac          HMAC-SHA256 truncated token             (CPU-bound, ~2us/row)
    noop          echo the input                          (pure transit floor)
    cpu           N sha256 rounds per row                 (synthetic CPU knob)
    io            sleep once per batch                    (synthetic I/O knob)
    io_row        sleep once per row                      (synthetic per-row I/O)
    mixed         N sha256 rounds AND a sleep, per row    (spans the CPU/IO plane)
    bloat         echo widened to `width` chars per row    (response-size limit probe)
    error         fail `fail_pct` of batches with `fail_code` (retry-behaviour probe)

`io` and `io_row` model genuinely different architectures and both are needed:
`io` is one bulk downstream call per batch (the Protegrity service's shape),
`io_row` is a remote call per row (the Developer Edition's shape). The first
amortises to nothing over a large batch; the second does not amortise at all.

`mixed` exists so the CPU/I-O *plane* can be spanned rather than only its two
axes. A workload sitting at 70% CPU / 30% wait is not reachable by any of the
single-axis modes, and that is exactly where real services live.

Every request emits one structured JSON log line carrying pid, thread, batch
size, in-flight concurrency and timings. That is the primary raw signal for the
concurrency study: Cloud Monitoring tells you how many *instances* ran, these
logs tell you what happened *inside* each one.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import sys
import threading
import time
import uuid

from flask import Flask, request, jsonify

from fpe_engine import Engine, EmailEngine

app = Flask(__name__)

# --- configuration -------------------------------------------------------

FPE_KEY = os.environ.get("FPE_KEY", "EF4359D8D580AA4F7F036D6F04FC6A94")
FPE_TWEAK = os.environ.get("FPE_TWEAK", "D8E7920AFA330A")
HMAC_KEY = os.environ.get("HMAC_KEY", FPE_KEY).encode()
LOG_REQUESTS = os.environ.get("FPE_LOG_REQUESTS", "true").lower() == "true"

def _instance_id() -> str:
    """Cloud Run instance ID from the metadata server.

    Every gunicorn worker in the same container must report the SAME value —
    that is what lets the analyser separate "how many instances did Cloud Run
    run?" from "how many worker processes were inside each one?". A per-process
    uuid would conflate the two, so the metadata server is the only correct
    source here; the uuid is a local-dev fallback only.
    """
    try:
        import urllib.request

        req = urllib.request.Request(
            "http://metadata.google.internal/computeMetadata/v1/instance/id",
            headers={"Metadata-Flavor": "Google"},
        )
        with urllib.request.urlopen(req, timeout=1.0) as resp:
            return resp.read().decode()[-12:]
    except Exception:  # noqa: BLE001 - not on GCP, or metadata unavailable
        return "local-" + uuid.uuid4().hex[:6]


INSTANCE_ID = _instance_id()
REVISION = os.environ.get("K_REVISION", "unknown")
PID = os.getpid()

_engine = Engine(FPE_KEY, FPE_TWEAK)
_email = EmailEngine(_engine)

# --- in-flight accounting ------------------------------------------------
# Per-process, not per-instance: with >1 gunicorn worker each process keeps its
# own counters. The sweep analyser aggregates across pids from the logs.

_inflight = 0
_inflight_peak = 0
_requests_total = 0
_rows_total = 0
_counter_lock = threading.Lock()


class _InFlight:
    """Context manager tracking concurrent requests inside this process."""

    def __enter__(self) -> int:
        global _inflight, _inflight_peak, _requests_total
        with _counter_lock:
            _inflight += 1
            _requests_total += 1
            if _inflight > _inflight_peak:
                _inflight_peak = _inflight
            self.observed = _inflight
        return self.observed

    def __exit__(self, *exc) -> None:
        global _inflight
        with _counter_lock:
            _inflight -= 1


def _log(payload: dict) -> None:
    """Emit one structured line; Cloud Logging parses stdout JSON natively."""
    if not LOG_REQUESTS:
        return
    print(json.dumps(payload), file=sys.stdout, flush=True)


# --- per-row transforms --------------------------------------------------


def _hmac_token(value: str) -> str:
    return hmac.new(HMAC_KEY, value.encode(), hashlib.sha256).hexdigest()[:16]


def _cpu_burn(value: str, rounds: int) -> str:
    """Deterministic synthetic CPU load: `rounds` sha256 iterations per row.

    A fixed work unit rather than a wall-clock target, so it stays honest under
    CPU throttling — if Cloud Run gives the container less CPU, this takes
    longer rather than silently doing less work.
    """
    digest = value.encode()
    for _ in range(rounds):
        digest = hashlib.sha256(digest).digest()
    return digest.hex()[:16]


class _InducedFailure(Exception):
    """Raised by `error` mode; carries the HTTP status BigQuery should see."""

    def __init__(self, code: int) -> None:
        super().__init__(f"induced failure with status {code}")
        self.code = code


def _apply(values: list[str], de: str, mode: str, params: dict) -> list[str]:
    if mode == "fpe_encrypt":
        fn = _email.encrypt if de == "email" else _engine.encrypt
        return [fn(v, de) for v in values]
    if mode == "fpe_decrypt":
        fn = _email.decrypt if de == "email" else _engine.decrypt
        return [fn(v, de) for v in values]
    if mode == "hmac":
        return [_hmac_token(v) for v in values]
    if mode == "noop":
        return values
    if mode == "cpu":
        rounds = int(params.get("rounds", 100))
        return [_cpu_burn(v, rounds) for v in values]
    if mode == "io":
        # One sleep per batch, modelling a single bulk downstream call — the
        # shape the Protegrity service actually has.
        time.sleep(float(params.get("sleep_ms", 50)) / 1000.0)
        return values
    if mode == "io_row":
        # One sleep per ROW, modelling a per-row remote call — the Developer
        # Edition's shape. Unlike `io` this does not amortise over batch size,
        # so a 12,000-row request costs 12,000 x sleep_ms of wall clock. Keep
        # sleep_ms small, or the request outlives the Cloud Run timeout.
        delay = float(params.get("sleep_ms", 5)) / 1000.0
        for _ in values:
            time.sleep(delay)
        return values
    if mode == "mixed":
        # `rounds` of CPU AND `sleep_ms` of wait, per row, interleaved. The
        # ratio between them is the wait/service ratio the worker-count rule is
        # a function of, so this is the mode that spans the plane.
        rounds = int(params.get("rounds", 0))
        delay = float(params.get("sleep_ms", 0)) / 1000.0
        out = []
        for v in values:
            out.append(_cpu_burn(v, rounds) if rounds else v)
            if delay:
                time.sleep(delay)
        return out
    if mode == "bloat":
        # Deliberately inflate the response so a modest row count can cross
        # BigQuery's 15 MB remote-function response ceiling on demand.
        width = int(params.get("width", 1000))
        return [(v * (width // max(len(v), 1) + 1))[:width] for v in values]
    if mode == "error":
        # BigQuery retries 408/429/500/503/504 up to 20 times per invocation.
        # Failing a deterministic fraction of batches exercises that path.
        pct = float(params.get("fail_pct", 100))
        code = int(params.get("fail_code", 503))
        seed = hashlib.sha256(("".join(values[:1])).encode()).digest()[0]
        if (seed / 255.0) * 100 < pct:
            raise _InducedFailure(code)
        return values
    raise ValueError(f"unknown mode {mode!r}")


def _parse_context(req: dict) -> dict:
    """user_defined_context arrives as a dict, but tolerate the list-of-pairs form."""
    ctx = req.get("userDefinedContext") or req.get("user_defined_context") or {}
    if isinstance(ctx, dict):
        return {str(k): v for k, v in ctx.items()}
    out: dict = {}
    for item in ctx:
        if isinstance(item, dict):
            out[str(item.get("key"))] = item.get("value")
        elif isinstance(item, (list, tuple)) and len(item) >= 2:
            out[str(item[0])] = item[1]
    return out


# --- routes --------------------------------------------------------------


@app.route("/", methods=["POST"])
def handle():
    t_start = time.perf_counter()
    with _InFlight() as observed_concurrency:
        try:
            req = request.get_json(force=True, silent=False) or {}
            calls = req.get("calls") or []
            ctx = _parse_context(req)
            mode = str(ctx.get("mode", "fpe_decrypt"))
            default_de = ctx.get("data_element")

            # Group by data element so each element is one vectorised pass. Rows
            # with a NULL value are held out and echoed back as NULL.
            by_element: dict[str, list[tuple[int, str]]] = {}
            replies: list = [None] * len(calls)
            for i, call in enumerate(calls):
                if not call:
                    continue
                value = call[0]
                if value is None:
                    continue
                de = (call[1] if len(call) > 1 else None) or default_de or "ssn"
                by_element.setdefault(str(de), []).append((i, str(value)))

            # Wall clock AND CPU clock around the same work. Their ratio is the
            # CPU share, and it is the second of the two numbers the Cloud Run
            # scaling guide is keyed on: it says how much of a request is
            # actually occupying a core rather than waiting, which is what
            # decides whether extra slots should be processes or threads.
            # thread_time, not process_time: under gthread several requests
            # share a process, and process_time would charge each of them for
            # all the others' CPU.
            t_work = time.perf_counter()
            c_work = time.thread_time()
            for de, items in by_element.items():
                out = _apply([v for _, v in items], de, mode, ctx)
                for (idx, _), transformed in zip(items, out):
                    replies[idx] = transformed
            work_ms = (time.perf_counter() - t_work) * 1000
            cpu_ms = (time.thread_time() - c_work) * 1000

            global _rows_total
            total_ms = (time.perf_counter() - t_start) * 1000
            with _counter_lock:
                _rows_total += len(calls)
            _log(
                {
                    "severity": "INFO",
                    "event": "batch",
                    "instance": INSTANCE_ID,
                    "revision": REVISION,
                    "pid": PID,
                    "thread": threading.current_thread().name,
                    "mode": mode,
                    "rows": len(calls),
                    "elements": {k: len(v) for k, v in by_element.items()},
                    "work_ms": round(work_ms, 3),
                    "cpu_ms": round(cpu_ms, 3),
                    "total_ms": round(total_ms, 3),
                    "inflight": observed_concurrency,
                    "inflight_peak": _inflight_peak,
                    "us_per_row": round(work_ms * 1000 / max(len(calls), 1), 2),
                    "cpu_us_per_row": round(cpu_ms * 1000 / max(len(calls), 1), 2),
                    # 1.0 = the request held a core throughout; 0.0 = it waited
                    # the whole time. Under contention this reads below 1 even
                    # for pure CPU work, because the thread is descheduled —
                    # which is exactly the signal that the box is oversubscribed.
                    "cpu_share": round(cpu_ms / work_ms, 3) if work_ms > 0 else None,
                }
            )
            return jsonify({"replies": replies})

        except _InducedFailure as exc:
            # Return the retryable status verbatim so BigQuery's retry machinery
            # engages, rather than collapsing it to a terminal 400.
            _log(
                {
                    "severity": "WARNING",
                    "event": "batch_error",
                    "instance": INSTANCE_ID,
                    "revision": REVISION,
                    "pid": PID,
                    "error": str(exc),
                    "error_type": "InducedFailure",
                    "status": exc.code,
                }
            )
            return jsonify({"errorMessage": str(exc)}), exc.code

        except Exception as exc:  # noqa: BLE001 - BQ wants the message, not a trace
            _log(
                {
                    "severity": "ERROR",
                    "event": "batch_error",
                    "instance": INSTANCE_ID,
                    "revision": REVISION,
                    "pid": PID,
                    "error": str(exc),
                    "error_type": type(exc).__name__,
                }
            )
            # 400 is deliberately NOT in BigQuery's retry set (408/429/500/503/504),
            # so a genuine bug fails fast instead of being retried 20 times.
            return jsonify({"errorMessage": str(exc)[:900]}), 400


@app.route("/healthz", methods=["GET"])
def healthz():
    return jsonify({"status": "ok", "instance": INSTANCE_ID, "pid": PID})


@app.route("/stats", methods=["GET"])
def stats():
    """Per-process counters. With >1 worker you hit a different pid each call."""
    from fpe_engine import PASSTHROUGH_COUNTER

    return jsonify(
        {
            "instance": INSTANCE_ID,
            "pid": PID,
            "requests_total": _requests_total,
            "rows_total": _rows_total,
            "inflight": _inflight,
            "inflight_peak": _inflight_peak,
            "threads_alive": threading.active_count(),
            "cpu_count": os.cpu_count(),
            "passthrough": PASSTHROUGH_COUNTER,
            "config": {
                "workers": os.environ.get("FPE_WORKERS"),
                "threads": os.environ.get("FPE_THREADS"),
                "worker_class": os.environ.get("FPE_WORKER_CLASS"),
            },
        }
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
