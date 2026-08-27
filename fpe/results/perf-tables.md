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

### Vertical scaling — vCPU, workers set equal to vCPU

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

### containerConcurrency isolated (cpu=4, workers=4, sync, maxScale=1)

| containerConcurrency | Median elapsed (s) | Rows/s | µs/row (svc) | Instances | Worker procs | Peak in-flight | Actual batch rows | n |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 42.80 | 9,348 | 97 | 1 | 4 | 1 | 5,000 | 2 |
| 2 | 23.05 | 17,357 | 93 | 1 | 4 | 1 | 5,000 | 2 |
| 4 | 21.10 | 19,136 | 118 | 1 | 4 | 1 | 5,000 | 2 |
| 6 | 17.27 | 23,322 | 112 | 1 | 4 | 1 | 5,000 | 2 |
| 8 | 21.14 | 18,975 | 149 | 1 | 4 | 1 | 5,000 | 2 |
| 12 | 27.86 | 16,584 | 137 | 1 | 4 | 1 | 5,000 | 2 |
| 16 | 27.16 | 16,500 | 137 | 1 | 4 | 1 | 5,000 | 2 |
| 32 | 22.47 | 17,947 | 173 | 1 | 4 | 1 | 5,000 | 2 |
| 80 | 18.28 | 22,463 | 138 | 1 | 4 | 1 | 5,000 | 2 |

### Worker count isolated (cpu=4, containerConcurrency=80, sync, maxScale=1)

| workers | Median elapsed (s) | Rows/s | µs/row (svc) | Instances | Worker procs | Peak in-flight | Actual batch rows | n |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 37.94 | 10,543 | 86 | 1 | 1 | 1 | 5,000 | 3 |
| 2 | 31.52 | 12,689 | 153 | 1 | 2 | 1 | 5,000 | 3 |
| 4 | 14.83 | 26,980 | 131 | 1 | 4 | 1 | 5,000 | 3 |
| 8 | 16.12 | 24,806 | 272 | 1 | 8 | 1 | 5,000 | 3 |
| 16 | 11.52 | 34,737 | 328 | 1 | 16 | 1 | 5,000 | 3 |
| 32 | 13.71 | 29,172 | 795 | 1 | 32 | 1 | 5,000 | 3 |
