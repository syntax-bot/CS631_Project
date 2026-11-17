### E1. Latency of one query (1 client, read only)

| query | No cache | qcache | qcache + admission |
|---|---|---|---|
| cheap | 0.018 | 0.020 | 0.020 |
| heavy | 7.139 | 0.029 | 0.029 |
| unique | 0.019 | 0.025 | 0.022 |

(ms per query, median of the repetitions)

### E2. Writes to the tables the queries read

| writes % | No cache | qcache | MySQL-style |
|---|---|---|---|
| 0 | 1,023 | 158,272 | 169,770 |
| 1 | 1,023 | 2,602 | 2,722 |
| 5 | 1,068 | 1,291 | 1,389 |
| 20 | 1,190 | 1,236 | 1,289 |

(transactions/s, median of the repetitions)

### E3. Writes to another table: the cost of invalidating

| writes % | No cache | qcache | MySQL-style | Partitions only | Versions only |
|---|---|---|---|---|---|
| 0 | 272,908 | 294,673 | 289,929 | 298,830 | 289,309 |
| 1 | 256,519 | 286,109 | 88,573 | 219,320 | 284,720 |
| 5 | 238,415 | 273,618 | 38,458 | 168,096 | 266,302 |
| 10 | 221,385 | 227,587 | 23,174 | 121,998 | 245,432 |
| 20 | 203,925 | 208,626 | 13,168 | 83,857 | 222,683 |
| 50 | 171,007 | 170,599 | 5,645 | 44,666 | 177,489 |

(transactions/s, median of the repetitions)

### E4. Throughput as clients are added (5% writes elsewhere)

| clients | No cache | qcache | MySQL-style | Partitions only | Versions only |
|---|---|---|---|---|---|
| 1 | 46,680 | 39,517 | 39,472 | 34,577 | 45,019 |
| 2 | 89,837 | 85,114 | 61,263 | 70,866 | 92,372 |
| 4 | 132,529 | 131,329 | 69,991 | 114,037 | 124,767 |
| 8 | 171,652 | 188,681 | 38,349 | 141,928 | 174,229 |
| 16 | 247,841 | 249,542 | 42,775 | 173,106 | 246,093 |
| 32 | 272,717 | 234,568 | 45,274 | 186,315 | 253,616 |

(transactions/s, median of the repetitions)

### E5. Cost of one committing write

| cached results | qcache | MySQL-style | Partitions only |
|---|---|---|---|
| 1000 | 0.023 | 0.020 | 0.030 |
| 10000 | 0.015 | 0.075 | 0.075 |
| 50000 | 0.016 | 0.310 | 0.295 |

(ms per commit, median of the repetitions)

### E2. Hit rate

| writes % | qcache | MySQL-style |
|---|---|---|
| 0 | 100% | 100% |
| 1 | 64% | 63% |
| 5 | 24% | 25% |
| 20 | 5% | 5% |

### E3. Hit rate

| writes % | qcache | MySQL-style | Partitions only | Versions only |
|---|---|---|---|---|
| 0 | 100% | 100% | 100% | 100% |
| 1 | 100% | 100% | 100% | 100% |
| 5 | 100% | 100% | 100% | 100% |
| 10 | 100% | 100% | 100% | 100% |
| 20 | 100% | 100% | 100% | 100% |
| 50 | 100% | 100% | 100% | 100% |
