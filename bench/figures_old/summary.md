### E1. Latency of one query (1 client, read only)

| query | No cache | qcache | qcache + admission |
|---|---|---|---|
| cheap | 0.120 | 0.110 | 0.125 |
| heavy | 31.282 | 0.207 | 0.209 |
| unique | 0.139 | 0.153 | 0.152 |

(ms per query, median of the repetitions)

### E2. Throughput as the share of writes grows

| writes % | No cache | qcache | MySQL-style | Partitions only | Versions only |
|---|---|---|---|---|---|
| 0 | 110 | 30,346 | 29,708 | 29,815 | 29,887 |
| 1 | 107 | 347 | 341 | 354 | 362 |
| 5 | 108 | 128 | 121 | 125 | 118 |
| 10 | 117 | 112 | 117 | 112 | 114 |
| 20 | 128 | 113 | 120 | 116 | 114 |
| 50 | 192 | 176 | 178 | 174 | 175 |

(transactions/s, median of the repetitions)

### E3. Throughput as clients are added (5% writes)

| clients | No cache | qcache | MySQL-style | Partitions only | Versions only | qcache + admission |
|---|---|---|---|---|---|---|
| 1 | 5,359 | 5,187 | 5,182 | 5,210 | 5,226 | 5,306 |
| 2 | 10,395 | 10,079 | 9,787 | 10,036 | 10,064 | 10,120 |
| 4 | 17,244 | 16,825 | 17,129 | 16,934 | 16,981 | 17,119 |
| 8 | 14,125 | 23,113 | 23,196 | 23,234 | 14,318 | 20,994 |
| 16 | 23,160 | 30,801 | 22,218 | 23,223 | 22,875 | 23,212 |
| 32 | 34,771 | 34,843 | 34,749 | 34,816 | 34,484 | 34,503 |

(transactions/s, median of the repetitions)

### E4. Cost of one committing write

| cached results | qcache | MySQL-style | Partitions only |
|---|---|---|---|
| 1000 | 1.290 | 4.432 | 5.027 |
| 10000 | 2.029 | 5.612 | 1.339 |
| 50000 | 1.242 | 6.281 | 2.970 |

(ms per commit, median of the repetitions)

### E2. Hit rate

| writes % | qcache | MySQL-style | Partitions only | Versions only |
|---|---|---|---|---|
| 0 | 100% | 100% | 100% | 100% |
| 1 | 66% | 63% | 63% | 66% |
| 5 | 24% | 21% | 23% | 23% |
| 10 | 11% | 12% | 11% | 11% |
| 20 | 5% | 5% | 5% | 5% |
| 50 | 0% | 0% | 0% | 0% |
