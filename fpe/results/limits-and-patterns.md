### Per-row input size: the 256 KiB batching budget vs the 5 MiB hard limit

| Bytes/row | Result | Rows per request | HTTP requests |
| --- | --- | --- | --- |
| 50,000 | OK | 6 | 2 |
| 200,000 | OK | 2 | 4 |
| 300,000 | OK | 1 | 8 |
| 1,000,000 | OK | 1 | 8 |
| 3,000,000 | OK | 1 | 3 |
| 5,000,000 | OK | 1 | 3 |
| 6,000,000 | FAILED | — | — |

### Short-circuit evaluation disables batching

| Query shape | Result | Elapsed (s) | HTTP requests | Median rows/request | Rows |
| --- | --- | --- | --- | --- | --- |
| plain | OK | 1.52 | 4 | 5,000 | 20,000 |
| inside_case | OK | 261.87 | 10,046 | 1 | 20,000 |
| hoisted | OK | 262.87 | 10,046 | 1 | 20,000 |
| plain | OK | 1.59 | 4 | 5,000 | 20,000 |
| inside_case | OK | 240.32 | 9,963 | 1 | 20,000 |
| hoisted_subquery | OK | 256.61 | 9,963 | 1 | 20,000 |
| filter_then_apply | OK | 1.36 | 2 | 4,982 | 20,000 |

### Requested vs actual batch size — is `max_batching_rows` honoured?

| max_batching_rows | Result | Actual median rows | Actual max rows | HTTP requests | Elapsed (s) |
| --- | --- | --- | --- | --- | --- |
| 10000 | OK | 10,000 | 10,000 | 40 | 13.03 |
| 50000 | OK | 11,905 | 11,905 | 32 | 9.90 |
| 100000 | OK | 11,905 | 11,905 | 32 | 7.13 |
| 250000 | OK | 11,905 | 11,905 | 34 | 6.42 |
| 500000 | OK | 11,905 | 11,905 | 34 | 6.87 |
| 1000000 | OK | 11,905 | 11,905 | 31 | 8.21 |
| auto | OK | 11,905 | 11,905 | 34 | 7.09 |

### Concurrent queries containing remote functions (documented limit: 10/project)

| Queries fired | Succeeded | Wall (s) | Aggregate rows/s | Quota error |
| --- | --- | --- | --- | --- |
| 1 | 1/1 | 3.1 | 6,543 | no |
| 2 | 2/2 | 3.9 | 10,326 | no |
| 4 | 4/4 | 4.0 | 20,244 | no |
| 8 | 8/8 | 3.9 | 40,777 | no |
| 10 | 10/10 | 4.5 | 44,432 | no |
| 12 | 12/12 | 5.6 | 42,893 | no |
| 16 | 16/16 | 6.9 | 46,113 | no |
| 16 | 16/16 | 164.4 | 97,326 | no |

### HTTP response size ceiling via `bloat` mode (documented limit: 15 MB for Cloud Run / gen2)

| Reply width (B) | Rows/request | Est. response (MB) | Result | Elapsed (s) |
| --- | --- | --- | --- | --- |
| (batch 1,000) | — | 1.0 | OK | 0.62 |
| 1,000 | 11,905 | 11.9 | OK | 1.94 |
| 1,200 | 11,905 | 14.3 | OK | 1.94 |
| 1,500 | 11,905 | 17.9 | FAILED | — |
| 2,000 | 11,905 | 23.8 | FAILED | — |
| 3,000 | 11,905 | 35.7 | FAILED | — |
| (batch 5,000) | — | 5.0 | OK | 0.81 |
| (batch 10,000) | — | 10.0 | OK | 1.16 |
| (batch 15,000) | — | 15.0 | OK | 1.07 |
| (batch 20,000) | — | 20.0 | OK | 1.21 |
| (batch 50,000) | — | 50.0 | OK | 1.70 |

### Retry behaviour (BigQuery retries 408/429/500/503/504, up to 20 attempts)

| Variant | Query result | Elapsed (s) | Endpoint invocations | Expectation |
| --- | --- | --- | --- | --- |
| always_503 | FAILED | 115.38 | 99 | fails after retries |
| half_503 | FAILED | 113.52 | 43 | succeeds via retries |
| always_400 | FAILED | 1.08 | 5 | fails fast, no retries |

### Search: tokenize the term vs detokenize the column

| Approach | Result | Elapsed (s) | HTTP requests | Rows to service | Median rows/request |
| --- | --- | --- | --- | --- | --- |
| `detokenize_column` | OK | 20.72 | 192 | 960,000 | 5,000 |
| `tokenize_term` | OK | 2.38 | 1 | 1 | 1 |
| `precomputed_token` | OK | 0.77 | 0 | — | — |

### Authorized-view + entitlement patterns

| Pattern | Result | Elapsed (s) | HTTP requests | Rows to service | Median rows/request |
| --- | --- | --- | --- | --- | --- |
| `A_case_masking` | OK | 25.78 | 987 | 987 | 1 |
| `B_union_all_masking` | OK | 0.79 | 1 | 987 | 987 |
| `C_row_filter` | OK | 0.96 | 1 | 987 | 987 |
| `D_row_and_column` | OK | 1.59 | 76 | 1,885 | 24 |
| `D_naive_case_3col` | OK | 50.74 | 1,974 | 1,974 | 1 |
| `E_name_dedup` | OK | 0.95 | 1 | 63 | 63 |
| `E_name_naive` | OK | 13.46 | 101 | 500,409 | 5,000 |
| `F_naive_filter_on_plaintext` | OK | 20.52 | 200 | 1,000,000 | 5,000 |
| `F_filter_on_token` | OK | 1.79 | 1 | 1 | 1 |
| `F_caller_supplies_token` | OK | 0.42 | 0 | — | — |

Result-equivalence checks:

- `A_vs_B`: 0 mismatches of 1947 rows — **equivalent**
- `D_vs_naive_case`: 0 mismatches of 987 rows — **equivalent**
- `E_dedup_vs_naive`: 0 mismatches of 987 rows — **equivalent**

### Where the remote function sits in the plan

| Shape | Result | Elapsed (s) | HTTP requests | Rows to service | Median rows/request |
| --- | --- | --- | --- | --- | --- |
| `limit_AFTER_detok` | OK | 4.63 | 30 | 150,000 | 5,000 |
| `limit_BEFORE_detok` | OK | 0.97 | 1 | 100 | 100 |
| `aggregate_AFTER_detok` | OK | 7.56 | 80 | 400,000 | 5,000 |
| `aggregate_BEFORE_detok` | OK | 0.34 | 0 | — | — |
| `filter_AFTER_detok` | OK | 0.55 | 1 | 397 | 397 |
| `filter_BEFORE_detok` | OK | 0.57 | 1 | 397 | 397 |

### Batch size sweep — `max_batching_rows`

| max_batching_rows | Median elapsed (s) | Rows/s | µs/row (svc) | Instances | Worker procs | Peak in-flight | Actual batch rows | n |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 100 | 19.09 | 21,401 | 72 | 1 | 4 | 1 | 100 | 2 |
| 500 | 28.05 | 14,268 | 79 | 1 | 4 | 1 | 500 | 2 |
| 1000 | 13.80 | 29,068 | 88 | 1 | 4 | 1 | 1,000 | 2 |
| 2500 | 12.47 | 32,093 | 76 | 1 | 4 | 1 | 2,500 | 2 |
| 5000 | 13.79 | 29,108 | 87 | 1 | 4 | 1 | 5,000 | 2 |
| 10000 | 12.73 | 31,416 | 78 | 1 | 4 | 1 | 10,000 | 2 |
| 25000 | 14.32 | 28,007 | 89 | 1 | 4 | 1 | 11,905 | 2 |
| 50000 | 14.10 | 28,655 | 77 | 1 | 4 | 1 | 11,905 | 2 |

### containerConcurrency x gunicorn worker model (all at 4 vCPU)

| containerConcurrency | workers | threads | class | Median elapsed (s) | Rows/s | µs/row (svc) | Instances | Worker procs | Peak in-flight | Actual batch rows | n |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 1 | 1 | sync | 59.32 | 6,882 | 82 | 1 | 1 | 1 | 5,000 | 2 |
| 1 | 4 | 1 | sync | 42.05 | 9,518 | 94 | 1 | 4 | 1 | 5,000 | 2 |
| 4 | 4 | 1 | sync | 21.68 | 18,886 | 122 | 1 | 4 | 1 | 5,000 | 2 |
| 8 | 1 | 1 | sync | 44.36 | 9,423 | 82 | 1 | 1 | 1 | 5,000 | 2 |
| 8 | 1 | 8 | gthread | 50.57 | 7,911 | 308 | 1 | 1 | 6 | 5,000 | 2 |
| 8 | 2 | 4 | gthread | 29.76 | 13,582 | 118 | 1 | 2 | 4 | 5,000 | 2 |
| 8 | 4 | 1 | sync | 16.07 | 25,728 | 110 | 1 | 4 | 1 | 5,000 | 2 |
| 16 | 1 | 1 | sync | 25.17 | 15,895 | 58 | 1 | 1 | 1 | 5,000 | 2 |
| 16 | 8 | 1 | sync | 15.56 | 25,858 | 111 | 1 | 8 | 1 | 5,000 | 2 |
| 16 | 16 | 1 | sync | 17.78 | 22,501 | 140 | 1 | 16 | 1 | 5,000 | 2 |

### Vertical scaling — vCPU with workers == vCPU

| vCPU | workers | Median elapsed (s) | Rows/s | µs/row (svc) | Instances | Worker procs | Peak in-flight | Actual batch rows | n |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 1 | 39.60 | 10,104 | 90 | 1 | 1 | 1 | 5,000 | 2 |
| 2 | 2 | 42.03 | 10,702 | 108 | 1 | 2 | 1 | 5,000 | 2 |
| 4 | 4 | 19.68 | 20,656 | 103 | 1 | 4 | 1 | 5,000 | 2 |
| 8 | 8 | 12.97 | 31,120 | 180 | 1 | 8 | 1 | 5,000 | 2 |

### Cost decomposition by workload

| mode | Median elapsed (s) | Rows/s | µs/row (svc) | Instances | Worker procs | Peak in-flight | Actual batch rows | n |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| cpu | 25.44 | 16,469 | 128 | 1 | 4 | 1 | 5,000 | 2 |
| fpe_decrypt | 15.90 | 26,002 | 118 | 1 | 4 | 1 | 5,000 | 2 |
| hmac | 1.87 | 214,505 | 7 | 1 | 4 | 1 | 5,000 | 2 |
| io | 2.28 | 175,679 | 10 | 1 | 4 | 1 | 5,000 | 2 |
| noop | 0.99 | 403,075 | 0 | 1 | 4 | 1 | 5,000 | 2 |

### Horizontal scaling — maxScale

| maxScale | Median elapsed (s) | Rows/s | µs/row (svc) | Instances | Worker procs | Peak in-flight | Actual batch rows | n |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 18.66 | 21,442 | 125 | 1 | 4 | 1 | 5,000 | 2 |
| 2 | 14.79 | 27,058 | 135 | 2 | 8 | 1 | 5,000 | 2 |
| 4 | 8.40 | 48,957 | 130 | 4 | 15 | 1 | 5,000 | 2 |
| 8 | 5.91 | 67,898 | 125 | 5 | 19 | 1 | 5,000 | 2 |

### CPU throttling

| cpu-throttling | Median elapsed (s) | Rows/s | µs/row (svc) | Instances | Worker procs | Peak in-flight | Actual batch rows | n |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| false | 24.23 | 16,738 | 148 | 1 | 4 | 1 | 5,000 | 2 |
| true | 17.04 | 23,497 | 122 | 1 | 4 | 1 | 5,000 | 2 |
