
#### `backpressure`

| contestant | ops/s | CPU µs/op | alloc B/op | max outstanding | live B per in-flight | RSS peak MB |
|---|--:|--:|--:|--:|--:|--:|
| vt-plain | 13,050 | 29.50 | 1,118 | 17 | – | 215 |
| lark | 13,334 | 22.00 | 1,376 | 17 | – | 230 |
| lark-unordered | 13,054 | 24.50 | 1,562 | 17 | – | 242 |
| lark-async | 13,141 | 19.75 | 256 | 17 | – | 167 |
| lark-async-unordered | 13,124 | 17.50 | 374 | 17 | – | 170 |
| reactor | 14,288 | 8.50 | 382 | 16 | – | 174 |
| pekko | 13,564 | 18.50 | 432 | 17 | – | 247 |
| vt-naive | 359,024 | 10.25 | 1,892 | 19,845 | – | 262 |

#### `backpressure-native`

| contestant | ops/s | CPU µs/op | alloc B/op | max outstanding | live B per in-flight | RSS peak MB |
|---|--:|--:|--:|--:|--:|--:|
| vt-plain | 13,016 | 27.50 | 943 | 17 | – | 194 |
| lark | 13,422 | 20.50 | 1,162 | 17 | – | 222 |
| lark-unordered | 13,218 | 22.50 | 1,412 | 17 | – | 227 |
| lark-async | 4,988 | 28.50 | 318 | 17 | – | 232 |
| lark-async-unordered | 4,996 | 27.75 | 454 | 17 | – | 236 |
| reactor | 14,650 | 14.50 | 538 | 16 | – | 198 |
| pekko | 4,982 | 25.50 | 536 | 17 | – | 246 |
| vt-naive | 640,390 | 5.75 | 916 | 18,198 | – | 235 |

#### `fanout-1k`

| contestant | ops/s | CPU µs/op | alloc B/op | max outstanding | live B per in-flight | RSS peak MB |
|---|--:|--:|--:|--:|--:|--:|
| vt-plain | 96,832 | 12.28 | 1,142 | 1,001 | 1,946 | 444 |
| lark | 94,202 | 8.15 | 1,384 | 1,001 | 853 | 446 |
| lark-unordered | 94,728 | 7.57 | 1,603 | 1,001 | 1,586 | 465 |
| lark-async | 96,360 | 3.85 | 238 | 1,001 | 225 | 248 |
| lark-async-unordered | 96,062 | 5.55 | 368 | 1,001 | 309 | 300 |
| reactor | 98,090 | 4.75 | 894 | 1,000 | 344 | 350 |
| pekko | 97,476 | 8.53 | 428 | 1,001 | 309 | 396 |

#### `fanout-1k-native`

| contestant | ops/s | CPU µs/op | alloc B/op | max outstanding | live B per in-flight | RSS peak MB |
|---|--:|--:|--:|--:|--:|--:|
| vt-plain | 97,115 | 6.55 | 929 | 1,001 | 982 | 342 |
| lark | 96,510 | 3.90 | 1,109 | 1,001 | 1,047 | 417 |
| lark-unordered | 95,664 | 6.28 | 1,413 | 1,001 | 1,085 | 429 |
| lark-async | 81,298 | 1.97 | 297 | 1,001 | 223 | 334 |
| lark-async-unordered | 81,026 | 2.67 | 432 | 1,001 | 307 | 368 |
| reactor | 98,366 | 7.90 | 841 | 1,000 | 381 | 366 |
| pekko | 81,446 | 3.52 | 536 | 1,001 | 310 | 385 |

#### `fanout-10k`

| contestant | ops/s | CPU µs/op | alloc B/op | max outstanding | live B per in-flight | RSS peak MB |
|---|--:|--:|--:|--:|--:|--:|
| vt-plain | 358,350 | 8.71 | 1,274 | 10,001 | 846 | 1,556 |
| lark | 413,962 | 7.49 | 1,446 | 10,001 | 876 | 2,014 |
| lark-unordered | 517,124 | 6.66 | 1,660 | 10,001 | 1,320 | 2,392 |
| lark-async | 816,882 | 2.62 | 237 | 10,001 | 192 | 413 |
| lark-async-unordered | 748,560 | 3.08 | 364 | 10,001 | 276 | 494 |
| reactor | 521,981 | 2.57 | 948 | 10,000 | 320 | 1,088 |
| pekko | 445,352 | 3.85 | 427 | 10,001 | 278 | 594 |

#### `fanout-10k-native`

| contestant | ops/s | CPU µs/op | alloc B/op | max outstanding | live B per in-flight | RSS peak MB |
|---|--:|--:|--:|--:|--:|--:|
| vt-plain | 892,312 | 3.77 | 897 | 10,001 | 884 | 1,000 |
| lark | 904,368 | 3.88 | 1,186 | 10,001 | 1,014 | 1,449 |
| lark-unordered | 850,618 | 4.33 | 1,387 | 10,001 | 1,112 | 1,826 |
| lark-async | 805,816 | 1.10 | 297 | 10,001 | 200 | 460 |
| lark-async-unordered | 802,558 | 1.62 | 437 | 10,001 | 288 | 612 |
| reactor | 265,924 | 5.96 | 961 | 10,000 | 357 | 1,086 |
| pekko | 791,496 | 2.20 | 536 | 10,001 | 284 | 768 |

#### `inflight-100k`

| contestant | ops/s | CPU µs/op | alloc B/op | max outstanding | live B per in-flight | RSS peak MB |
|---|--:|--:|--:|--:|--:|--:|
| vt-plain | 284,762 | 11.19 | 1,249 | 100,001 | 1,002 | 3,340 |
| lark | 300,966 | 10.34 | 1,457 | 100,001 | 993 | 3,486 |
| lark-unordered | 347,651 | 9.70 | 1,583 | 100,001 | 1,172 | 3,726 |
| lark-async | 858,568 | 3.00 | 240 | 100,001 | 191 | 672 |
| lark-async-unordered | 726,444 | 3.72 | 368 | 100,001 | 266 | 850 |
| reactor | 454,594 | 5.07 | 931 | 94,186 | nan | 2,226 |
| pekko | 479,580 | 3.63 | 427 | 100,001 | 230 | 1,022 |

#### `inflight-100k-native`

| contestant | ops/s | CPU µs/op | alloc B/op | max outstanding | live B per in-flight | RSS peak MB |
|---|--:|--:|--:|--:|--:|--:|
| vt-plain | 1,252,969 | 3.14 | 938 | 100,001 | 631 | 2,111 |
| lark | 1,207,945 | 3.25 | 1,140 | 100,001 | 650 | 2,458 |
| lark-unordered | 966,463 | 4.08 | 1,297 | 100,001 | 578 | 2,872 |
| lark-async | 2,514,106 | 1.37 | 297 | 100,001 | 171 | 747 |
| lark-async-unordered | 2,080,368 | 1.77 | 425 | 100,001 | 260 | 1,151 |
| reactor | 10,782 | 96.62 | 934 | 100,000 | 343 | 2,086 |
| pekko | 1,005,142 | 3.33 | 548 | 100,001 | 212 | 1,382 |

#### `overload`: capacity drops from 500 to 100 halfway

| contestant | seconds | ops/s | p50 before ms | p99 before ms | p50 after ms | p99 after ms |
|---|--:|--:|--:|--:|--:|--:|
| reactor-1000 | 6.22 | 32,159 | 10.3 | 11.6 | 51.7 | 58.7 |
| reactor-tuned | 6.20 | 32,286 | 5.1 | 6.1 | 25.8 | 26.8 |
| lark-1000 | 6.20 | 32,283 | 10.1 | 12.2 | 51.4 | 53.9 |
| lark-adaptive | 7.25 | 27,590 | 5.1 | 6.6 | 5.1 | 10.1 |

#### JMH (ms per million elements, lower is better)

| benchmark | ms/op | ± | alloc B/op |
|---|--:|--:|--:|
| CpuChainBenchmark.lark | 24.12 | 1.92 | 56,007,890 |
| CpuChainBenchmark.loop | 0.84 | 0.08 | 0 |
| CpuChainBenchmark.pekko | 155.70 | 179.79 | 56,082,110 |
| CpuChainBenchmark.reactor | 15.13 | 1.49 | 27,998,291 |
| CpuChainBenchmark.sequence | 10.83 | 0.76 | 43,996,938 |
| HandOffBenchmark.lark | 136.75 | 73.00 | 40,239,360 |
| HandOffBenchmark.pekko | 1618.08 | 1138.23 | 121,508,097 |
| HandOffBenchmark.reactor | 24.79 | 0.76 | 31,999,613 |
| HandOffBenchmark.vtQueue | 427.83 | 92.32 | 22,660,598 |
