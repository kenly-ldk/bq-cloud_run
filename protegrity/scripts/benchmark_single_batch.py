import time
import json
import os
from google.cloud import bigquery

# Product ID and Dataset (Use environment variables or defaults)
PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT", "<YOUR_PROJECT_ID>")
DATASET_ID = os.environ.get("BIGQUERY_DATASET", "pii_transform_demo")

if PROJECT_ID == "<YOUR_PROJECT_ID>":
     print("Error: Please set GOOGLE_CLOUD_PROJECT environment variable or edit the script.")
     exit(1)

# BigQuery Client
client = bigquery.Client(project=PROJECT_ID)

# Batch sizes to test (Rows = Batch Size)
BATCH_SIZES = [100, 1000, 2000, 5000, 10000]
ITERATIONS = 5

results = {}

for bs in BATCH_SIZES:
    print(f"\n--- Benchmarking Single Batch Size: {bs} (Rows={bs}) ---")
    results[bs] = []
    
    # Query reading exactly 'bs' rows and applying the corresponding function
    query = f"""
    SELECT SUM(LENGTH({DATASET_ID}.pii_detokenize_b{bs}(email, 'email')))
    FROM (SELECT email FROM {DATASET_ID}.pii_table_tokenized LIMIT {bs})
    """
    
    for i in range(ITERATIONS):
        print(f"Iteration {i+1}/{ITERATIONS} for Batch Size {bs}...")
        job_config = bigquery.QueryJobConfig(use_query_cache=False)
        
        start_time = time.time()
        query_job = client.query(query, job_config=job_config)
        # Wait for the job to complete
        query_job.result()
        end_time = time.time()
        
        client_latency = end_time - start_time
        job_id = query_job.job_id
        
        # Get server side metrics via job.ended and job.started (datetimes)
        completed_job = client.get_job(job_id)
        
        if completed_job.ended and completed_job.started:
            latency_sec = (completed_job.ended - completed_job.started).total_seconds()
        else:
            latency_sec = client_latency
            print(f"Warning: Falling back to client latency for job {job_id}")
        
        # RPS is calculated as bs / latency (Rows treated = bs)
        rps = float(bs) / latency_sec if latency_sec > 0 else 0
        
        print(f"  Job ID: {job_id}, Server Latency: {latency_sec:.3f}s, RPS: {rps:.2f}")
        
        results[bs].append({
            "job_id": job_id,
            "latency": latency_sec,
            "rps": rps
        })

# Save results to JSON in the current directory
output_file = "benchmark_single_batch_results.json"
with open(output_file, 'w') as f:
    json.dump(results, f, indent=2)

print(f"\nResults saved to {output_file}")

# Print Summary Table
print("\n=== Summary Table (Rows = Batch Size) ===")
print("| Batch Size | Latency Avg (s) | Latency Min (s) | Latency Max (s) | Throughput Avg (RPS) | Throughput Min (RPS) | Throughput Max (RPS) |")
print("| :--- | :--- | :--- | :--- | :--- | :--- | :--- |")

for bs in BATCH_SIZES:
    latencies = [r["latency"] for r in results[bs]]
    rps_values = [r["rps"] for r in results[bs]]
    
    avg_lat = sum(latencies) / len(latencies) if latencies else 0
    min_lat = min(latencies) if latencies else 0
    max_lat = max(latencies) if latencies else 0
    
    avg_rps = sum(rps_values) / len(rps_values) if rps_values else 0
    min_rps = min(rps_values) if rps_values else 0
    max_rps = max(rps_values) if rps_values else 0
    
    print(f"| {bs} | {avg_lat:.3f}s | {min_lat:.3f}s | {max_lat:.3f}s | ~{avg_rps:,.2f} | ~{min_rps:,.2f} | ~{max_rps:,.2f} |")
