# BigQuery Remote Functions on Cloud Run — PII Tokenization Demos

Two independent demos of calling a Cloud Run service from BigQuery via a
[remote function](https://docs.cloud.google.com/bigquery/docs/remote-functions),
plus a measured study of how the thing actually performs.

| Demo | Crypto | External dependency | Status |
| --- | --- | --- | --- |
| [`fpe/`](fpe/) | FF3-1 format-preserving encryption, in-process | **None** | **Active** — deployed, tested, benchmarked |
| [`protegrity/`](protegrity/) | Protegrity PEP (vendor) | Protegrity API credentials | ⚠️ **Unmaintained** — see below |

Both speak the same BigQuery remote function protocol and both are deployed to
Cloud Run; they differ only in what happens to a value once it arrives.

> [!WARNING]
> **`protegrity/` is unmaintained and unverified.** The Protegrity API access it
> needs is no longer available, so nothing in it has been deployed or tested
> since. Its benchmark numbers are known to be unreliable, and it contains at
> least one route that would crash on call. It is kept as a reference for the
> vendor integration only — [see its README](protegrity/README.md) for the
> specific issues.
>
> **Start with [`fpe/`](fpe/).** It exercises the identical BigQuery →
> Cloud Run remote function path without any vendor dependency.

Most of the performance study applies to both demos, because most of it is
about BigQuery's protocol rather than the crypto —
[which parts transfer, and which don't](docs/performance-tuning.md#does-this-apply-to-the-protegrity-demo).

## Layout

```text
├── config/                   # shared.env contract (see below)
├── docs/
│   ├── performance-tuning.md # the study — applies to BOTH demos, start here
│   └── plans/                # designed-but-not-yet-run follow-up studies
├── shared/
│   └── generate_mock_data.py # PII mock data generator, used by both demos
├── fpe/                      # vendor-free demo, and the measurement rig
│   ├── service/              # Flask app, FF3-1 engine, Dockerfile, Knative template
│   ├── sql/                  # generated remote functions + access-control patterns
│   ├── scripts/              # provision / build / deploy / sweep / analyze
│   └── results/              # generated result tables + raw JSONL
└── protegrity/               # ⚠️ unmaintained vendor demo, kept for reference
    ├── service/              # Flask app + vendored appython & developer SDKs
    ├── sql/
    └── scripts/
```

## Configuration

Every script reads one config contract, so nothing is hardcoded to a workstation:

- `config/shared.env` — committed defaults and placeholders.
- `config/shared.env.local` — **gitignored**, your real values, layered on top.

Create the local overlay before running anything:

```bash
cat > config/shared.env.local <<'EOF'
PROJECT_ID=your-project-id
GCP_CREDENTIALS_FILE=            # empty = ambient ADC
GCLOUD_CONFIG_NAME=              # empty = active gcloud config
EOF
```

Bash scripts `source config/prelude.sh`; Python uses `config._loader.load()`.
Both export the same three variables downstream — `GOOGLE_APPLICATION_CREDENTIALS`,
`CLOUDSDK_ACTIVE_CONFIG_NAME`, `GOOGLE_CLOUD_PROJECT` — so `gcloud` subprocesses
and `google.cloud.*` clients always agree on identity and project.

## Quick start (FPE demo)

```bash
pyenv virtualenv 3.12.7 bq-cloud-run-fpe && pyenv local bq-cloud-run-fpe
pip install ff3 flask gunicorn google-cloud-bigquery google-cloud-monitoring

./fpe/scripts/provision.sh          # APIs, Artifact Registry, dataset, connection, IAM
./fpe/scripts/build.sh              # Cloud Build -> Artifact Registry
./fpe/scripts/deploy.sh             # Knative manifest -> Cloud Run
python fpe/scripts/generate_remote_functions.py --apply
./fpe/scripts/setup_data.sh         # tokenized table + roundtrip verification
./fpe/scripts/setup_access_control.sh
```

Then:

```sql
SELECT `proj.fpe_perf_demo`.fpe_encrypt('123-45-6789', 'ssn');   -- 516-91-2276
SELECT `proj.fpe_perf_demo`.fpe_decrypt('516-91-2276', 'ssn');   -- 123-45-6789
```

## The study

[`docs/performance-tuning.md`](docs/performance-tuning.md) is the substantive
part of this repo. It measures, on real infrastructure:

- what `max_batching_rows` actually does (BigQuery caps it far below what you ask);
- why `containerConcurrency` alone buys nothing for CPU-bound Python;
- every documented remote-function limit, probed until it breaks;
- the query shapes that silently disable batching and cost you ~180x;
- authorized-view + entitlement patterns for row- and column-level access
  control that keep batching intact.

Reproduce any of it with `python fpe/scripts/sweep.py --phase <name>`.
