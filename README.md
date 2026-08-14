# BigQuery Remote Functions on Cloud Run — PII Tokenization Demos

Two independent demos of calling a Cloud Run service from BigQuery via a
[remote function](https://docs.cloud.google.com/bigquery/docs/remote-functions),
plus a measured study of how the thing actually performs.

| Demo | Crypto | External dependency | Use it when |
| --- | --- | --- | --- |
| [`fpe/`](fpe/) | FF3-1 format-preserving encryption, in-process | **None** | You want a runnable demo, or you want to study tuning and limits |
| [`protegrity/`](protegrity/) | Protegrity PEP (vendor) | Protegrity API credentials | You have a Protegrity entitlement and want the vendor integration |

Both speak the same BigQuery remote function protocol and both are deployed to
Cloud Run; they differ only in what happens to a value once it arrives.

## Layout

```text
├── config/                   # shared.env contract (see below)
├── docs/
│   ├── performance-tuning.md # the concurrency + limits study — start here
│   └── results/              # generated result tables
├── shared/
│   └── generate_mock_data.py # PII mock data generator, used by both demos
├── fpe/                      # vendor-free demo
│   ├── service/              # Flask app, FF3-1 engine, Dockerfile, Knative template
│   ├── sql/                  # generated remote functions + access-control patterns
│   └── scripts/              # provision / build / deploy / sweep / analyze
└── protegrity/               # vendor demo (unchanged, still requires API access)
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
- the query shapes that silently disable batching and cost you ~200x;
- authorized-view + entitlement patterns for row- and column-level access
  control that keep batching intact.

Reproduce any of it with `python fpe/scripts/sweep.py --phase <name>`.
