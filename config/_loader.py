"""Tiny env-file loader so Python scripts read the same shared.env as Bash.

Usage:
    from config._loader import load, require
    load()                              # loads <repo>/config/shared.env
    project = require("PROJECT_ID")

File values always win over the current os.environ — same as `set -a; source
shared.env` in bash. A sibling shared.env.local, if present, is layered on top
and overrides shared.env.
"""

from __future__ import annotations

import os
from pathlib import Path

CONFIG_DIR = Path(__file__).resolve().parent
REPO_ROOT = CONFIG_DIR.parent


def _parse(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key:
            # Expand both ~ and $VAR / ${VAR} so Python sees the same final
            # string bash would produce from `set -a; source <file>; set +a`.
            out[key] = os.path.expanduser(os.path.expandvars(value))
    return out


def load(env_file: str | os.PathLike[str] | None = None) -> dict[str, str]:
    """Load shared.env (and shared.env.local overlay if present) into os.environ.

    Returns the merged dict for inspection. File values overwrite os.environ —
    matches `set -a; source shared.env; source shared.env.local; set +a`.
    """
    primary = Path(env_file) if env_file else CONFIG_DIR / "shared.env"
    overlay = CONFIG_DIR / "shared.env.local"

    merged: dict[str, str] = {}
    if primary.exists():
        merged.update(_parse(primary))
    if overlay.exists():
        merged.update(_parse(overlay))

    for key, value in merged.items():
        os.environ[key] = value

    # Mirror our portable vars into the standard GCP env vars so BOTH in-process
    # Python clients (bigquery.Client, monitoring_v3.*) AND any subprocess-spawned
    # gcloud see consistent credentials. Same effect as config/prelude.sh.
    creds = merged.get("GCP_CREDENTIALS_FILE", "").strip()
    if creds:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = creds
    config_name = merged.get("GCLOUD_CONFIG_NAME", "").strip()
    if config_name:
        os.environ["CLOUDSDK_ACTIVE_CONFIG_NAME"] = config_name
    project_id = merged.get("PROJECT_ID", "").strip()
    if project_id:
        os.environ["GOOGLE_CLOUD_PROJECT"] = project_id

    return merged


def require(key: str) -> str:
    """Return os.environ[key] or raise a helpful error if missing/empty."""
    value = os.environ.get(key, "")
    if not value:
        raise RuntimeError(
            f"Required env var {key!r} is not set. "
            f"Add it to {CONFIG_DIR / 'shared.env.local'} or export it before running."
        )
    return value
