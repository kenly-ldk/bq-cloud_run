# BigQuery + Protegrity Cloud Run Integration

This repository provides a template and guide for integrating BigQuery with Protegrity (Format Preserving Encryption - FPE) using BigQuery Remote Functions and a Cloud Run sidecar architecture.

## Architecture

BigQuery invokes a Cloud Run service via a Remote Function. The Cloud Run service acts as a multiplexer/router, forwarding requests to a local Protegrity PEP (Policy Enforcement Point) sidecar container running in the same Pod.

## Repository Structure

```text
├── README.md               # This guide
├── cloud_run/              # Flask App & Deployment configurations
│   ├── main.py             # Python app routing BigQuery requests to Protegrity
│   ├── Dockerfile          # App container Dockerfile
│   ├── requirements.txt    # App python dependencies
│   └── protegrity-sidecar.yaml # Cloud Run sidecar deployment specification
├── sql/
│   ├── create_remote_functions.sql # SQL definitions for BigQuery Remote Functions
└── scripts/
    ├── generate_mock_data.py   # Script to generate PII mock data for testing
    └── benchmark_scale.py      # Script to run scale tests (1M rows)
```

## Setup Guide

### 1. Deploy Cloud Run Sidecar Service

#### 📋 Prerequisites & Architecture Context
The Protegrity integration requires a **Multi-Container Sidecar** architecture on Cloud Run. A single Pod runs **4 containers** sharing the same network namespace (`localhost`):

*   **`app` container**: Runs the Flask application (`main.py`) which receives BigQuery Remote Function calls.
*   **`classification-service` sidecar**: The primary gateway.
*   **`pattern-provider` sidecar**: Compute-intensive service that classifies entities using patterns.
*   **`context-provider` sidecar**: Lighter service that classifies entities using context.

#### 🛠️ Step A: Acquire and Prep Images

This step involves acquiring the proprietary Protegrity sidecar images, building your custom Python wrapper (`app`) image, and pushing all of them to your Google Artifact Registry.

##### 1. 📦 How to Acquire and Prepare Protegrity Sidecar Images
The Protegrity sidecar images are proprietary and must be obtained directly from Protegrity.

If evaluating using the official [Protegrity Developer Edition Repo](https://github.com/Protegrity-Developer-Edition/protegrity-developer-edition), you can clone it and use `docker compose pull` to download the images without running the services:

```bash
git clone https://github.com/Protegrity-Developer-Edition/protegrity-developer-edition.git
cd protegrity-developer-edition

# Pull the images defined in the docker-compose schema
docker compose pull
```

##### 2. Tag and Push Sidecars to Artifact Registry
Tag the images you just pulled and push them to your private Google Artifact Registry:

```bash
# 1. Classification Service
docker tag ghcr.io/protegrity-developer-edition/classification_service:1.1.1-300.e37cd434 <REGION>-docker.pkg.dev/<PROJECT_ID>/<REPOSITORY>/protegrity-classification-service:latest
docker push <REGION>-docker.pkg.dev/<PROJECT_ID>/<REPOSITORY>/protegrity-classification-service:latest

# 2. Pattern Provider
docker tag ghcr.io/protegrity-developer-edition/pattern_classification_provider:1.1.1-213.85581853 <REGION>-docker.pkg.dev/<PROJECT_ID>/<REPOSITORY>/protegrity-pattern-provider:latest
docker push <REGION>-docker.pkg.dev/<PROJECT_ID>/<REPOSITORY>/protegrity-pattern-provider:latest

# 3. Context Provider
docker tag ghcr.io/protegrity-developer-edition/context_classification_provider:1.1.1-180.3e9a57f6 <REGION>-docker.pkg.dev/<PROJECT_ID>/<REPOSITORY>/protegrity-context-provider:latest
docker push <REGION>-docker.pkg.dev/<PROJECT_ID>/<REPOSITORY>/protegrity-context-provider:latest
```

##### 3. Build & Push the App Image
Build the Cloud Run app image (the Flask wrapper) using Google Cloud Build and push it to your Artifact Registry:

```bash
gcloud builds submit --tag <REGION>-docker.pkg.dev/<PROJECT_ID>/<REPOSITORY>/pii-transform-service-protegrity:latest cloud_run/
```

#### 🚀 Step B: Configure & Deploy

1.  **Configure Credentials & Image Paths**: Modify `cloud_run/protegrity-sidecar.yaml` to replace placeholders with your Artifact Registry image paths and Protegrity credentials.

    If using **Protegrity Developer Edition**, obtain your API credentials (Email, Password, API Key) from the Protegrity portal and inject them into the `app` container environment variables:

    ```yaml
    - image: <REGION>-docker.pkg.dev/<PROJECT_ID>/<REPOSITORY>/pii-transform-service-protegrity:latest
      name: app
      env:
        - name: DEV_EDITION_EMAIL
          value: "<YOUR_PROTEGRITY_DEV_EMAIL>"
        - name: DEV_EDITION_PASSWORD
          value: "<YOUR_PROTEGRITY_DEV_PASSWORD>"
        - name: DEV_EDITION_API_KEY
          value: "<YOUR_PROTEGRITY_DEV_API_KEY>"
    ```

    > [!WARNING]
    > Do not commit raw passwords to version control! In production, use **Google Secret Manager** to inject these securely.

2.  **Deploy using the replace command**:

    ```bash
    gcloud run services replace cloud_run/protegrity-sidecar.yaml --region <REGION>
    ```

    > [!NOTE]
    > **Why `replace` instead of `deploy`?** We use `replace` because we are deploying a **full declarative YAML manifest** (Knative `kind: Service`) required for multi-container sidecars. Standard `deploy` is typically used for single-container command-line deployments.
    > 
    > [!IMPORTANT]
    > **Total Pod Sizing Required**: **5.5 vCPUs** and **11 GiB Memory** per instance.
    >
    > **Container Sizing Breakdown**:
    > *   `app`: 0.25 vCPU, 512Mi
    > *   `classification-service`: 0.25 vCPU, 512Mi
    > *   `pattern-provider`: 4.0 vCPU, 8Gi
    > *   `context-provider`: 1.0 vCPU, 2Gi
    >
    >
    > > The provided `cloud_run/protegrity-sidecar.yaml` file already incorporates these default resource limits.


### 2. Define BigQuery Remote Functions

The `sql/create_remote_functions.sql` file defines external functions that BigQuery uses to communicate with Cloud Run. It includes:

*   **Standard Operation Functions**:
    *   `pii_detokenize_fpe_multi(val, de)`: Default detokenization (decryption).
    *   `pii_tokenize_fpe_multi(val, de)`: Real-time tokenization (encryption) for optimized searching.
    *   `pii_noop(val)`: No-operation function (bypasses Protegrity) used for network latency benchmarking.
*   **Performance Benchmarking Functions** (`_b100` to `_b100000`):
    *   Functions with hardcoded `max_batching_rows` to test throughput bottlenecks.

Run these SQL statements in your BigQuery console. Be sure to replace `<YOUR_PROJECT_ID>` and `<YOUR_PROJECT_NUMBER>` placeholders with your actual Google Cloud parameters.

### 3. Generate Mock Data & Test

You can use the provided script to generate sample data for testing:

```bash
python scripts/generate_mock_data.py 1000000 # Generates 1M rows
```

The script generates a CSV with the following schema:
*   `id`: `INTEGER` - Unique row identifier.
*   `name`: `STRING` - Clear-text full name.
*   `email`: `STRING` - Clear-text email address (Target for `email` protection).
*   `ssn`: `STRING` - Clear-text Social Security Number (Target for `ssn` protection).
*   `dob`: `DATE` - Date of Birth (Format: `YYYY-MM-DD`).

Load this **clear-text** data into a BigQuery table using the `bq` CLI:

```bash
bq load \
  --source_format=CSV \
  --skip_leading_rows=1 \
  your_dataset.pii_table_clear \
  pii_data.csv \
  id:INTEGER,name:STRING,email:STRING,ssn:STRING,dob:DATE
```

Then, use the remote functions to tokenize it into a new table:

```sql
-- Example: Tokenize clear-text mock data into a new tokenized table
CREATE OR REPLACE TABLE your_dataset.pii_table_tokenized AS
SELECT 
  id,
  name,
  pii_transform_demo.pii_tokenize_fpe_multi(email, 'email') as email,
  pii_transform_demo.pii_tokenize_fpe_multi(ssn, 'ssn') as ssn,
  dob
FROM your_dataset.pii_table_clear;
```

### 4. Pure Network Latency Testing (Isolate Transit Overhead)

To understand the difference between network transit overhead and Protegrity cryptographic processing time, you can run a **Noop (No Operation)** test. A Noop function bypasses the Protegrity engine and returns instantly from the Flask app.

### Pre-requisite: Create Noop Function
Ensure you have created the `pii_noop` function using `sql/create_remote_functions.sql`.

### Run Latency Benchmark

The `scripts/benchmark_latency.py` script runs three tests against a 1,000-row limit and calculates the per-row overhead:
1.  **Plain SELECT**: Baseline BigQuery reading speed.
2.  **Noop Query**: Measures Network Transit + Flask setup time.
3.  **FPE Query**: Measures End-to-End time (Network + Protegrity).

```bash
export GOOGLE_CLOUD_PROJECT="your-project-id"
export BIGQUERY_DATASET="your_dataset"
export BIGQUERY_TABLE="your_tokenized_table"
python scripts/benchmark_latency.py
```

### Benchmark Results (10,000 Rows, 10x Run Average)

Below are the actual results demonstrating the separation of network transit and compute overhead:

| Test Case | Avg Elapsed Latency (seconds) | Delta (Transit/Compute) |
| :--- | :--- | :--- |
| **Plain SELECT (Base)** | 0.321s | - |
| **Noop Query (Network Transit)** | 0.450s | +0.129s (Network Overhead) |
| **FPE Detokenization (End-to-end)** | 0.500s | +0.050s (Compute Overhead) |

*Note: The difference between Noop and Baseline is the network overhead. The difference between FPE and Noop is the Protegrity processing overhead.*


### 5. End-to-End Performance Benchmarking (1M Rows)

Run the scale test script to analyze performance across different batch sizes:

```bash
export GOOGLE_CLOUD_PROJECT="your-project-id"
export BIGQUERY_DATASET="your_dataset"
python scripts/benchmark_scale.py
```

## Performance Plateaus & Sweet Spots

Based on automated scale tests for **1 Million Rows** on a single Cloud Run instance with the following configuration:
*   **Total Pod Size**: 5.5 vCPU, 11 GiB Memory
*   **Concurrency**: 20 (Simultaneous requests per instance)
*   **minScale**: 1 (Pre-warmed to avoid cold starts)
*   **maxScale**: 1 (Isolated to a single instance for pure benchmarking)

| Batch Size | Latency Avg (s) | Latency Min (s) | Latency Max (s) | Throughput Avg (RPS) | Throughput Min (RPS) | Throughput Max (RPS) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 10,000 | 25.65s | 22.65s | 36.60s | ~40,385 | ~27,325 | ~44,146 |
| 20,000 | 20.77s | 19.47s | 23.54s | ~48,376 | ~42,475 | ~51,369 |
| 50,000 | 19.85s | 19.41s | 20.18s | **~50,400** | ~49,554 | ~51,509 |
| 100,000 | 20.87s | 19.63s | 24.90s | ~48,318 | ~40,167 | ~50,945 |

#### The 50,000 Sweet Spot
The 50,000 batch size represents the point where the payload perfectly utilizes the concurrency window of a single Cloud Run instance. 
*   **1M rows / 50,000 = 20 requests.**
*   The Cloud Run instance accepts all 20 requests simultaneously (concurrency = 20), resulting in zero queuing and peak efficiency!




## Best Practices: Filter Before Detokenization

If you need to detokenize a dataset for reporting or analytics, always apply non-sensitive filters (e.g., date ranges, regions, categories) **before** calling the detokenization function. This ensures you only pay the compute/network cost for the rows you actually need.

```sql
-- HIGH PERFORMANCE PATTERN
WITH filtered_data AS (
  SELECT email FROM pii_table_tokenized
  WHERE event_date = '2026-03-31' -- Non-sensitive filter applied first!
)
SELECT pii_detokenize_fpe_multi(email, 'email') FROM filtered_data;
```

### Benchmark Evidence: Low Latency for Filtered Small Datasets

> [!TIP]
> You can reproduce these results in your environment by running the single-batch benchmark script:
> ```bash
> python scripts/benchmark_single_batch.py
> ```

When you filter your data first (e.g., to a single day or region), the number of rows sent to the remote function drops significantly. The table below proves that for datasets under 10,000 rows, the response time is consistently under 0.5 seconds:

| Rows Processed | Latency Avg (s) | Throughput Avg (RPS) |
| :--- | :--- | :--- |
| 100 | 0.64s | ~208 |
| 1,000 | 0.40s | ~2,495 |
| 2,000 | 0.57s | ~3,742 |
| 5,000 | 0.46s | ~11,463 |
| 10,000 | 0.44s | **~24,003** |

> [!NOTE]
> These benchmarks confirm that processing a single batch of up to 10,000 rows takes less than 0.5 seconds on average.

## Best Practices: Search vs Detokenize

When working with tokenized data, querying by detokenizing a whole table is inefficient. Instead, **tokenize the search term** using the remote function and query the tokenized table directly:

```sql
-- HIGH PERFORMANCE PATTERN
SELECT * FROM pii_table_tokenized
WHERE email = pii_tokenize_fpe_multi('user@example.com', 'email');
```

