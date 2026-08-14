"""Gunicorn config driven entirely by env vars.

The worker model is one of the knobs under study, so it must be changeable per
Cloud Run revision without rebuilding the image. Set FPE_WORKERS / FPE_THREADS /
FPE_WORKER_CLASS in the service YAML and redeploy.

Why this matters for a CPU-bound Python service:

    sync    + N workers  -> true parallelism up to N processes (needs N <= cpu)
    gthread + T threads  -> NO parallelism for pure-Python CPU work; the GIL
                            serialises it. Only helps when work releases the GIL
                            (sleep, sockets) — i.e. our `io` mode, not `fpe_*`.

So `containerConcurrency` on Cloud Run is an upper bound on requests admitted,
not on requests *executed in parallel*. The app's worker count is the real
limit. Mismatching the two is the single most common Cloud Run tuning error.
"""

import os

bind = f"0.0.0.0:{os.environ.get('PORT', '8080')}"

workers = int(os.environ.get("FPE_WORKERS", "1"))
threads = int(os.environ.get("FPE_THREADS", "1"))
worker_class = os.environ.get("FPE_WORKER_CLASS", "sync")

# Large batches (100k rows) take tens of seconds; the default 30s timeout would
# kill them mid-flight and surface as a BigQuery remote function failure.
timeout = int(os.environ.get("FPE_TIMEOUT", "600"))
graceful_timeout = 30

# Keep connections warm: BigQuery opens many short-lived HTTP/1.1 connections
# per query and reconnect cost shows up in the transit floor.
keepalive = int(os.environ.get("FPE_KEEPALIVE", "65"))

# Cloud Run streams stdout/stderr to Cloud Logging.
accesslog = "-" if os.environ.get("FPE_ACCESS_LOG", "false").lower() == "true" else None
errorlog = "-"
loglevel = os.environ.get("FPE_LOG_LEVEL", "info")

# Bound the request body: 100k rows of email is comfortably under 32 MB, but
# gunicorn's default line/field limits are for headers only, so nothing to raise.
worker_tmp_dir = "/dev/shm"


def on_starting(server):
    server.log.info(
        "fpe service starting: workers=%s threads=%s class=%s cpu_count=%s",
        workers,
        threads,
        worker_class,
        os.cpu_count(),
    )
