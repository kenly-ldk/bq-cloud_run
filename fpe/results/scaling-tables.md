### Batch size at a cheap workload — does the flat curve stay flat?

| `max_batching_rows` | Median rows/s | Min | Max | Spread | vs best | Verdict | µs/row (svc) | Peak concurrency | Mean concurrency | Rows/iteration | Actual rows/request | n |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 50000 | 319,735 | 298,792 | 330,305 | 1.11x | 1.00x | **best** | 21 | 16 | 8.1 | 6,035,003 | 11,904 | 5 |
| 10000 | 279,412 | 222,985 | 317,333 | 1.42x | 0.87x | = best (overlaps) | 24 | 16 | 8.8 | 6,680,181 | 10,000 | 5 |
| 25000 | 276,776 | 230,581 | 298,107 | 1.29x | 0.87x | slower | 25 | 16 | 8.4 | 5,508,124 | 11,904 | 5 |
| 5000 | 270,718 | 258,033 | 277,540 | 1.08x | 0.85x | slower | 24 | 16 | 7.5 | 6,321,814 | 5,000 | 5 |
| 1000 | 256,978 | 219,800 | 273,287 | 1.24x | 0.80x | slower | 16 | 19 | 4.3 | 5,718,536 | 1,000 | 5 |
| 2500 | 222,174 | 131,134 | 287,705 | 2.19x | 0.69x | slower | 30 | 17 | 6.9 | 5,120,000 | 2,500 | 5 |
| 500 | 203,246 | 190,138 | 213,676 | 1.12x | 0.64x | slower | 8 | 20 | 2.9 | 3,797,245 | 500 | 5 |
| 100 | 56,742 | 50,583 | 58,913 | 1.16x | 0.18x | slower | 6 | 21 | 0.7 | 1,040,643 | 100 | 5 |

Indistinguishable from the best config at these sample sizes (ranges overlap): 10000.

### Container CPU calibration — `rounds` to µs/row

| `rounds` | µs/row measured on the container |
| --- | --- |
| 1 | 1.90 |
| 2 | 3.20 |
| 4 | 5.40 |
| 8 | 9.80 |
| 16 | 19.00 |
| 32 | 36.80 |
| 64 | 70.70 |
| 128 | 137.70 |
| 256 | 269.10 |
| 512 | 584.20 |

Theil-Sen fit over `rounds` >= 4: **µs/row = 1.09 + 1.0879 x rounds**  (largest residual at rounds=512: 584.2 measured vs 558.1 fitted)

Paste into `fpe/scripts/calibration.py`:

```python
CONTAINER = Curve(floor_us=1.09, us_per_round=1.0879, measured_on="cloud-run-4vcpu-gen2")
```

Then regenerate the remote functions — the cost parameters are baked into the function names, so a re-calibration renames them rather than silently changing what a stale name measures.

### Workload x worker model (cpu=4, containerConcurrency=80, maxScale=1)

**W1** — `noop_b5000`

| class | workers | threads | slots | Median rows/s | Min | Max | Spread | vs best | Verdict | µs/row (svc) | Peak concurrency | Mean concurrency | Rows/iteration | n |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| sync | 4 | 1 | 4 | 959,108 | 821,064 | 988,776 | 1.20x | 1.00x | **best** | 0 | 12 | 0.6 | 15,311,200 | 5 |
| sync | 16 | 1 | 16 | 842,318 | 725,637 | 921,277 | 1.27x | 0.88x | = best (overlaps) | 0 | 16 | 2.0 | 20,000,000 | 5 |
| sync | 1 | 1 | 1 | 313,476 | 209,304 | 324,235 | 1.55x | 0.33x | slower | 0 | 2 | 0.4 | 5,120,000 | 5 |
| gthread | 1 | 32 | 32 | 302,468 | 281,493 | 314,525 | 1.12x | 0.32x | slower | 0 | 6 | 0.9 | 8,727,114 | 5 |

Indistinguishable from the best config at these sample sizes (ranges overlap): sync/16/1/16.

**W2** — `mixed_r4s0_b5000`

| class | workers | threads | slots | Median rows/s | Min | Max | Spread | vs best | Verdict | µs/row (svc) | Peak concurrency | Mean concurrency | Rows/iteration | n |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| sync | 16 | 1 | 16 | 417,034 | 392,845 | 446,140 | 1.14x | 1.00x | **best** | 11 | 16 | 5.0 | 11,148,145 | 5 |
| sync | 32 | 1 | 32 | 392,645 | 364,689 | 406,828 | 1.12x | 0.94x | = best (overlaps) | 22 | 26 | 8.2 | 8,774,054 | 5 |
| sync | 4 | 1 | 4 | 327,915 | 274,796 | 352,308 | 1.28x | 0.79x | slower | 5 | 6 | 2.2 | 9,832,215 | 5 |
| sync | 8 | 1 | 8 | 290,418 | 229,565 | 327,911 | 1.43x | 0.70x | slower | 12 | 9 | 4.1 | 5,097,710 | 5 |
| gthread | 2 | 16 | 32 | 231,672 | 175,904 | 253,844 | 1.44x | 0.56x | slower | 8 | 14 | 3.3 | 6,609,602 | 5 |
| gthread | 4 | 16 | 64 | 206,459 | 122,180 | 243,486 | 1.99x | 0.50x | slower | 28 | 22 | 6.3 | 7,039,424 | 5 |
| sync | 2 | 1 | 2 | 196,875 | 151,761 | 220,502 | 1.45x | 0.47x | slower | 5 | 3 | 1.4 | 5,813,525 | 5 |
| sync | 1 | 1 | 1 | 180,158 | 114,100 | 191,616 | 1.68x | 0.43x | slower | 3 | 2 | 0.7 | 4,790,585 | 5 |
| gthread | 1 | 8 | 8 | 115,254 | 70,628 | 129,305 | 1.83x | 0.28x | slower | 12 | 8 | 2.2 | 2,596,440 | 5 |
| gthread | 1 | 32 | 32 | 100,365 | 90,356 | 110,228 | 1.22x | 0.24x | slower | 15 | 11 | 2.6 | 2,280,400 | 5 |

Indistinguishable from the best config at these sample sizes (ranges overlap): sync/32/1/32.

**W3** — `mixed_r27s0_b5000`

| class | workers | threads | slots | Median rows/s | Min | Max | Spread | vs best | Verdict | µs/row (svc) | Peak concurrency | Mean concurrency | Rows/iteration | n |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| sync | 32 | 1 | 32 | 124,671 | 66,063 | 130,619 | 1.98x | 1.00x | **best** | 20 | 7 | 2.6 | 2,560,000 | 5 |
| sync | 16 | 1 | 16 | 124,530 | 102,696 | 131,653 | 1.28x | 1.00x | = best (overlaps) | 75 | 16 | 8.6 | 3,226,820 | 5 |
| sync | 4 | 1 | 4 | 116,975 | 63,390 | 117,512 | 1.85x | 0.94x | = best (overlaps) | 26 | 5 | 3.0 | 2,560,000 | 5 |
| gthread | 1 | 32 | 32 | 49,584 | 48,698 | 52,167 | 1.07x | 0.40x | slower | 70 | 14 | 4.2 | 1,134,429 | 5 |
| sync | 1 | 1 | 1 | 31,608 | 28,280 | 33,058 | 1.17x | 0.25x | slower | 28 | 2 | 0.8 | 640,000 | 5 |

Indistinguishable from the best config at these sample sizes (ranges overlap): sync/16/1/16, sync/4/1/4.

**W4** — `fpe_decrypt_b5000`

| class | workers | threads | slots | Median rows/s | Min | Max | Spread | vs best | Verdict | µs/row (svc) | Peak concurrency | Mean concurrency | Rows/iteration | n |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| gthread | 4 | 16 | 64 | 17,163 | 15,367 | 19,863 | 1.29x | 1.00x | **best** | 130 | 6 | 2.8 | 640,000 | 5 |
| gthread | 1 | 32 | 32 | 7,310 | 6,016 | 7,821 | 1.30x | 0.43x | slower | 400 | 6 | 3.0 | 320,000 | 5 |

**W5** — `io_row_s2_b5000`

| class | workers | threads | slots | Median rows/s | Min | Max | Spread | vs best | Verdict | µs/row (svc) | Peak concurrency | Mean concurrency | Rows/iteration | n |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| gthread | 4 | 16 | 64 | 11,602 | 11,459 | 11,799 | 1.03x | 1.00x | **best** | 2136 | 30 | 20.0 | 640,000 | 5 |
| sync | 32 | 1 | 32 | 9,853 | 9,828 | 9,936 | 1.01x | 0.85x | slower | 2092 | 30 | 15.6 | 320,000 | 5 |
| gthread | 1 | 32 | 32 | 9,736 | 9,583 | 9,807 | 1.02x | 0.84x | slower | 2112 | 28 | 14.2 | 320,000 | 5 |
| gthread | 2 | 16 | 32 | 7,360 | 7,280 | 9,776 | 1.34x | 0.63x | slower | 2113 | 28 | 12.1 | 320,000 | 5 |
| sync | 16 | 1 | 16 | 4,894 | 4,796 | 4,946 | 1.03x | 0.42x | slower | 2120 | 16 | 8.1 | 160,000 | 5 |
| sync | 8 | 1 | 8 | 3,718 | 3,692 | 3,727 | 1.01x | 0.32x | slower | 2085 | 8 | 7.5 | 80,000 | 5 |
| gthread | 1 | 8 | 8 | 3,700 | 3,677 | 3,711 | 1.01x | 0.32x | slower | 2097 | 8 | 7.5 | 80,000 | 5 |
| sync | 4 | 1 | 4 | 1,336 | 1,325 | 1,346 | 1.02x | 0.12x | slower | 2110 | 4 | 2.1 | 28,967 | 5 |
| sync | 2 | 1 | 2 | 776 | 742 | 783 | 1.06x | 0.07x | slower | 2104 | 2 | 1.4 | 24,908 | 5 |
| sync | 1 | 1 | 1 | 466 | 465 | 469 | 1.01x | 0.04x | slower | 2087 | 1 | 0.9 | 9,788 | 5 |

**W6** — `mixed_r91s1_b5000`

| class | workers | threads | slots | Median rows/s | Min | Max | Spread | vs best | Verdict | µs/row (svc) | Peak concurrency | Mean concurrency | Rows/iteration | n |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| sync | 32 | 1 | 32 | 16,688 | 12,196 | 17,441 | 1.43x | 1.00x | **best** | 1160 | 30 | 14.6 | 320,000 | 5 |
| gthread | 4 | 16 | 64 | 11,621 | 11,342 | 19,830 | 1.75x | 0.70x | = best (overlaps) | 1201 | 24 | 12.6 | 640,000 | 5 |
| gthread | 2 | 16 | 32 | 9,771 | 9,270 | 11,908 | 1.28x | 0.59x | slower | 1306 | 23 | 10.1 | 320,000 | 5 |
| sync | 16 | 1 | 16 | 7,979 | 7,654 | 8,133 | 1.06x | 0.48x | slower | 1300 | 17 | 8.0 | 160,000 | 5 |
| gthread | 1 | 32 | 32 | 6,298 | 3,670 | 6,348 | 1.73x | 0.38x | slower | 4084 | 28 | 21.2 | 320,000 | 5 |
| gthread | 1 | 8 | 8 | 4,113 | 2,768 | 4,549 | 1.64x | 0.25x | slower | 1426 | 8 | 5.4 | 160,000 | 5 |
| sync | 4 | 1 | 4 | 3,032 | 3,011 | 3,059 | 1.02x | 0.18x | slower | 1141 | 4 | 2.9 | 83,451 | 5 |
| sync | 1 | 1 | 1 | 846 | 843 | 857 | 1.02x | 0.05x | slower | 1137 | 1 | 0.9 | 20,654 | 5 |

Indistinguishable from the best config at these sample sizes (ranges overlap): gthread/4/16/64.

### Workload profile — the two numbers the guide is keyed on (1 slot, containerConcurrency 1)

| Workload | Function | µs/row (wall) | µs/row (CPU) | CPU share | wait/service | Rows/s at 1 slot |
| --- | --- | --- | --- | --- | --- | --- |
| W1 | `noop_b5000` | 0.1 | 0.1 | 0.995 | 0.0 | 200,200 |
| W2 | `mixed_r4s0_b5000` | 4.3 | 4.3 | 1.000 | 0.0 | 105,876 |
| W3 | `mixed_r27s0_b5000` | 27.9 | 27.9 | 1.000 | 0.0 | 29,214 |
| W4 | `fpe_decrypt_b5000` | 86.7 | 86.7 | 1.000 | 0.0 | 9,495 |
| W5 | `io_row_s2_b5000` | 2,113.8 | 24.9 | 0.012 | 82.3 | 461 |
| W6 | `mixed_r91s1_b5000` | 1,201.2 | 119.6 | 0.100 | 9.0 | 811 |
| W7 | `mixed_r45s1_b5000` | 1,164.6 | 83.4 | 0.072 | 12.9 | 818 |

### Falsification: the fitted slot rule against a held-out workload

| slots | workers | threads | Median rows/s | Min | Max | Spread | vs best | Verdict | µs/row (svc) | Peak concurrency | Mean concurrency | Rows/iteration | n |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 128 | 4 | 32 | 23,312 | 22,485 | 23,857 | 1.06x | 1.00x | **best** | 1133 | 30 | 22.8 | 1,280,000 | 5 |
| 256 | 4 | 64 | 17,261 | 12,386 | 21,607 | 1.74x | 0.74x | slower | 1201 | 30 | 23.4 | 2,560,000 | 5 |
| 96 | 4 | 24 | 12,847 | 12,660 | 22,827 | 1.80x | 0.55x | = best (overlaps) | 1138 | 27 | 12.6 | 960,000 | 5 |
| 64 | 4 | 16 | 12,098 | 11,727 | 20,440 | 1.74x | 0.52x | slower | 1166 | 28 | 13.0 | 640,000 | 5 |
| 32 | 4 | 8 | 10,387 | 7,792 | 17,012 | 2.18x | 0.45x | slower | 1214 | 28 | 11.2 | 320,000 | 5 |
| 16 | 4 | 4 | 6,696 | 5,337 | 6,779 | 1.27x | 0.29x | slower | 1159 | 16 | 6.0 | 160,000 | 5 |
| 8 | 1 | 8 | 5,371 | 5,149 | 5,452 | 1.06x | 0.23x | slower | 1156 | 8 | 5.3 | 161,310 | 5 |

Indistinguishable from the best config at these sample sizes (ranges overlap): 96/4/24.

### Vertical vs horizontal scaling, per workload

**W2 vertical — vCPU, worker count rescaled with it**

| vCPU | workers | threads | Median rows/s | Min | Max | Spread | vs best | Verdict | µs/row (svc) | Peak concurrency | Mean concurrency | Rows/iteration | n |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 8 | 32 | 1 | 581,732 | 417,022 | 671,310 | 1.61x | 1.00x | **best** | 10 | 30 | 6.8 | 16,967,370 | 5 |
| 4 | 16 | 1 | 440,420 | 365,615 | 545,191 | 1.49x | 0.76x | = best (overlaps) | 8 | 16 | 5.1 | 9,746,930 | 5 |
| 2 | 8 | 1 | 207,425 | 155,873 | 224,007 | 1.44x | 0.36x | slower | 19 | 9 | 4.1 | 3,987,540 | 5 |
| 1 | 4 | 1 | 152,290 | 115,367 | 159,056 | 1.38x | 0.26x | slower | 3 | 4 | 1.0 | 2,560,000 | 5 |

Indistinguishable from the best config at these sample sizes (ranges overlap): 4/16/1.

**W2 horizontal — maxScale**

| maxScale | Median rows/s | Min | Max | Spread | vs best | Verdict | µs/row (svc) | Peak concurrency | Mean concurrency | Rows/iteration | Instances | n |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 4 | 410,409 | 355,351 | 426,374 | 1.20x | 1.00x | **best** | 15 | 16 | 6.7 | 8,351,823 | 1 | 5 |
| 1 | 394,688 | 314,267 | 487,339 | 1.55x | 0.96x | = best (overlaps) | 12 | 17 | 5.6 | 9,504,077 | 1 | 5 |
| 2 | 364,229 | 315,408 | 440,831 | 1.40x | 0.89x | = best (overlaps) | 17 | 17 | 6.6 | 7,925,268 | 1 | 5 |
| 8 | 348,209 | 335,658 | 391,205 | 1.17x | 0.85x | = best (overlaps) | 19 | 17 | 7.6 | 9,061,091 | 2 | 5 |

Indistinguishable from the best config at these sample sizes (ranges overlap): 1, 2, 8.

**W5 vertical — vCPU, worker count rescaled with it**

| vCPU | workers | threads | Median rows/s | Min | Max | Spread | vs best | Verdict | µs/row (svc) | Peak concurrency | Mean concurrency | Rows/iteration | n |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 8 | 8 | 16 | 12,832 | 12,696 | 12,917 | 1.02x | 1.00x | **best** | 2150 | 30 | 25.4 | 1,280,000 | 5 |
| 4 | 4 | 16 | 11,487 | 11,382 | 11,551 | 1.01x | 0.90x | slower | 2141 | 30 | 20.5 | 640,000 | 5 |
| 2 | 2 | 16 | 5,999 | 5,854 | 9,514 | 1.63x | 0.47x | slower | 2109 | 27 | 11.3 | 320,000 | 5 |
| 1 | 1 | 16 | 4,722 | 3,614 | 4,886 | 1.35x | 0.37x | slower | 2128 | 16 | 7.6 | 160,000 | 5 |

**W5 horizontal — maxScale**

| maxScale | Median rows/s | Min | Max | Spread | vs best | Verdict | µs/row (svc) | Peak concurrency | Mean concurrency | Rows/iteration | Instances | n |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 2 | 11,784 | 11,721 | 11,903 | 1.02x | 1.00x | **best** | 2105 | 30 | 20.2 | 640,000 | 1 | 5 |
| 1 | 11,657 | 11,572 | 11,803 | 1.02x | 0.99x | = best (overlaps) | 2115 | 30 | 19.4 | 640,000 | 1 | 5 |
| 8 | 11,644 | 11,543 | 11,736 | 1.02x | 0.99x | = best (overlaps) | 2138 | 30 | 20.2 | 640,000 | 1 | 5 |
| 4 | 11,564 | 9,607 | 11,575 | 1.20x | 0.98x | slower | 2147 | 30 | 20.7 | 640,000 | 1 | 5 |

Indistinguishable from the best config at these sample sizes (ranges overlap): 1, 8.

### Drift check

Configurations are not interleaved — a deploy costs more wall clock than the iterations it precedes — so each phase re-runs its first configuration at the end. If the re-run no longer overlaps the original, the ordering of everything in that phase is suspect.

- `workload_matrix` / `w5-s1`: first 466 rows/s [465–469], re-run 467 [459–468] — **no drift** (ranges overlap)
- `rule_check` / `w7-g1x8`: first 5,371 rows/s [5,149–5,452], re-run 5,414 [5,340–5,486] — **no drift** (ranges overlap)
- `scale_axis` / `w2-cpu1-s4`: first 152,290 rows/s [115,367–159,056], re-run 128,404 [80,770–140,343] — **no drift** (ranges overlap)
- `scale_axis` / `w5-cpu1-g1x16`: first 4,722 rows/s [3,614–4,886], re-run 4,934 [4,894–4,960] — **DRIFTED — comparisons in this phase are not safe**
