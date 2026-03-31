import time
import os
from google.cloud import bigquery

def run_query(bq_client, query, description):
    print(f"\n--- Running {description} ---")
    start_time = time.time()
    query_job = bq_client.query(query)
    results = query_job.result() # Wait for query to finish
    end_time = time.time()
    duration = end_time - start_time
    
    # Get job stats
    job = bq_client.get_job(query_job.job_id)
    slot_millis = job.slot_millis
    processed_bytes = job.total_bytes_processed
    
    print(f"Duration: {duration:.2f} seconds")
    print(f"Slot Milliseconds: {slot_millis}")
    print(f"Total Bytes Processed: {processed_bytes}")
    
    # Count rows
    row_count = sum(1 for _ in results)
    print(f"Rows Processed: {row_count}")
    
    return {
        "duration": duration,
        "slot_millis": slot_millis,
        "row_count": row_count
    }

def main():
    project_id = os.environ.get("GOOGLE_CLOUD_PROJECT", "kenly-demo-1")
    dataset_id = os.environ.get("BIGQUERY_DATASET", "pii_transform_demo")
    table_id = os.environ.get("BIGQUERY_TABLE", "pii_table_tokenized")
    
    print(f"Using Project: {project_id}, Dataset: {dataset_id}, Table: {table_id}")
    
    bq_client = bigquery.Client(project=project_id)
    
    # 1. Baseline Query (Plain SELECT)
    query_baseline = f"SELECT email FROM `{project_id}.{dataset_id}.{table_id}` LIMIT 1000"
    baseline_stats = run_query(bq_client, query_baseline, "Baseline Query (Plain SELECT)")
    
    # 2. Noop Remote Function Query
    query_noop = f"SELECT pii_transform_demo.pii_noop(email) FROM `{project_id}.{dataset_id}.{table_id}` LIMIT 1000"
    noop_stats = run_query(bq_client, query_noop, "Noop Remote Function Query")
    
    # 3. FPE Detokenization Query
    query_fpe = f"SELECT pii_transform_demo.pii_detokenize_fpe_multi(email, 'email') FROM `{project_id}.{dataset_id}.{table_id}` LIMIT 1000"
    fpe_stats = run_query(bq_client, query_fpe, "FPE Detokenization Query")
    
    # Compare
    if noop_stats["row_count"] > 0:
        per_row_baseline = baseline_stats["duration"] / baseline_stats["row_count"]
        per_row_noop = noop_stats["duration"] / noop_stats["row_count"]
        per_row_fpe = fpe_stats["duration"] / fpe_stats["row_count"]
        
        per_row_overhead_noop = per_row_noop - per_row_baseline
        per_row_overhead_fpe = per_row_fpe - per_row_noop # Difference between Noop and FPE is Protegrity processing time!
        
        print("\n=== Benchmarking Summary ===")
        print(f"Baseline per row: {per_row_baseline*1000:.4f} ms")
        print(f"Noop per row (Network + Flask): {per_row_noop*1000:.4f} ms")
        print(f"FPE per row (Total): {per_row_fpe*1000:.4f} ms")
        print(f"Network Transit Overhead per row: {per_row_overhead_noop*1000:.4f} ms")
        print(f"Protegrity Processing Overhead per row: {per_row_overhead_fpe*1000:.4f} ms")

if __name__ == "__main__":
    main()
