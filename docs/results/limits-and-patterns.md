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
| 1 | 1/1 | 18.4 | 54,202 | no |
| 2 | 2/2 | 3.9 | 10,326 | no |
| 2 | 2/2 | 25.9 | 77,352 | no |
| 4 | 4/4 | 4.0 | 20,244 | no |
| 4 | 4/4 | 55.3 | 72,352 | no |
| 8 | 8/8 | 3.9 | 40,777 | no |
| 8 | 8/8 | 102.1 | 78,366 | no |
| 10 | 10/10 | 4.5 | 44,432 | no |
| 10 | 10/10 | 128.1 | 78,058 | no |
| 12 | 12/12 | 5.6 | 42,893 | no |
| 12 | 12/12 | 129.2 | 92,882 | no |
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
| `A_case_masking` | OK | 35.04 | 964 | 964 | 1 |
| `B_union_all_masking` | OK | 0.78 | 1 | 987 | 987 |
| `C_row_filter` | OK | 0.84 | 1 | 987 | 987 |
| `A_case_masking` | OK | 29.44 | 975 | 975 | 1 |
| `B_union_all_masking` | OK | 1.40 | 1 | 987 | 987 |
| `C_row_filter` | OK | 1.37 | 1 | 987 | 987 |
| `D_row_and_column` | OK | 1.67 | 74 | 1,816 | 24 |
| `D_naive_case_3col` | OK | 52.47 | 1,953 | 1,953 | 1 |
| `E_name_dedup` | OK | 2.05 | 0 | — | — |
| `E_name_naive` | OK | 12.73 | 101 | 500,409 | 5,000 |

Result-equivalence checks:

- `A_vs_B`: 0 mismatches of 1947 rows — **equivalent**
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
