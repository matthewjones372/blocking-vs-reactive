
#### `backpressure`

| contestant | ops/s | CPU µs/op | alloc B/op | max outstanding | live B per in-flight | RSS peak MB |
|---|--:|--:|--:|--:|--:|--:|
| vt-plain | 13,061 | 28.00 | 1,194 | 17 | – | 208 |
| lark | 13,345 | 25.50 | 1,505 | 17 | – | 238 |
| reactor | 14,260 | 7.50 | 370 | 16 | – | 176 |
| pekko | 13,372 | 20.00 | 432 | 17 | – | 251 |
| vt-naive | 335,568 | 11.50 | 1,826 | 19,989 | – | 266 |

#### `backpressure-native`

| contestant | ops/s | CPU µs/op | alloc B/op | max outstanding | live B per in-flight | RSS peak MB |
|---|--:|--:|--:|--:|--:|--:|
| vt-plain | 12,860 | 28.25 | 891 | 17 | – | 194 |
| lark | 13,232 | 24.75 | 1,164 | 17 | – | 220 |
| reactor | 14,566 | 19.75 | 486 | 16 | – | 194 |
| pekko | 4,924 | 28.75 | 536 | 17 | – | 251 |
| vt-naive | 625,335 | 6.00 | 910 | 18,959 | – | 239 |

#### `fanout-1k`

| contestant | ops/s | CPU µs/op | alloc B/op | max outstanding | live B per in-flight | RSS peak MB |
|---|--:|--:|--:|--:|--:|--:|
| vt-plain | 96,148 | 12.12 | 1,206 | 1,001 | 638 | 436 |
| lark | 96,508 | 11.35 | 1,502 | 1,001 | 1,188 | 463 |
| reactor | 97,574 | 5.78 | 892 | 1,000 | 348 | 350 |
| pekko | 96,910 | 9.07 | 428 | 1,001 | 310 | 389 |

#### `fanout-1k-native`

| contestant | ops/s | CPU µs/op | alloc B/op | max outstanding | live B per in-flight | RSS peak MB |
|---|--:|--:|--:|--:|--:|--:|
| vt-plain | 96,118 | 7.05 | 964 | 1,001 | 897 | 342 |
| lark | 97,060 | 7.25 | 1,193 | 1,001 | 2,066 | 442 |
| reactor | 98,192 | 9.77 | 838 | 1,000 | 381 | 368 |
| pekko | 81,114 | 4.10 | 536 | 1,001 | 309 | 390 |

#### `fanout-10k`

| contestant | ops/s | CPU µs/op | alloc B/op | max outstanding | live B per in-flight | RSS peak MB |
|---|--:|--:|--:|--:|--:|--:|
| vt-plain | 246,988 | 12.33 | 1,222 | 10,001 | 954 | 1,862 |
| lark | 312,006 | 9.32 | 1,436 | 10,001 | 1,127 | 2,204 |
| reactor | 548,712 | 2.38 | 946 | 10,000 | 314 | 1,054 |
| pekko | 431,077 | 4.14 | 426 | 10,001 | 288 | 630 |

#### `fanout-10k-native`

| contestant | ops/s | CPU µs/op | alloc B/op | max outstanding | live B per in-flight | RSS peak MB |
|---|--:|--:|--:|--:|--:|--:|
| vt-plain | 803,594 | 4.11 | 892 | 10,001 | 1,160 | 998 |
| lark | 768,074 | 4.64 | 1,160 | 10,001 | 914 | 1,418 |
| reactor | 306,006 | 5.32 | 960 | 10,000 | 354 | 1,178 |
| pekko | 716,630 | 3.00 | 536 | 10,001 | 280 | 750 |

#### `inflight-100k`

| contestant | ops/s | CPU µs/op | alloc B/op | max outstanding | live B per in-flight | RSS peak MB |
|---|--:|--:|--:|--:|--:|--:|
| vt-plain | 207,706 | 15.07 | 1,232 | 100,001 | 1,042 | 3,333 |
| lark | 205,774 | 14.00 | 1,489 | 100,001 | 1,068 | 4,240 |
| reactor | 505,739 | 4.41 | 947 | 65,111 | nan | 2,600 |
| pekko | 415,825 | 4.31 | 426 | 100,001 | 194 | 1,022 |

#### `inflight-100k-native`

| contestant | ops/s | CPU µs/op | alloc B/op | max outstanding | live B per in-flight | RSS peak MB |
|---|--:|--:|--:|--:|--:|--:|
| vt-plain | 1,091,436 | 3.57 | 986 | 100,001 | 722 | 2,190 |
| lark | 944,512 | 4.13 | 1,264 | 100,001 | 716 | 2,796 |
| reactor | 12,224 | 84.77 | 934 | 100,000 | 342 | 2,356 |
| pekko | 1,075,609 | 3.19 | 536 | 100,001 | 150 | 1,277 |

#### JMH (ms per million elements, lower is better)

| benchmark | ms/op | ± | alloc B/op |
|---|--:|--:|--:|
| CpuChainBenchmark.lark | 23.81 | 1.44 | 56,007,895 |
| CpuChainBenchmark.loop | 0.83 | 0.04 | 0 |
| CpuChainBenchmark.pekko | 164.03 | 186.64 | 56,082,126 |
| CpuChainBenchmark.reactor | 15.29 | 0.52 | 27,998,291 |
| CpuChainBenchmark.sequence | 11.38 | 0.95 | 43,996,950 |
| HandOffBenchmark.lark | 520.07 | 195.48 | 46,663,131 |
| HandOffBenchmark.pekko | 1624.99 | 884.61 | 121,384,569 |
| HandOffBenchmark.reactor | 25.07 | 0.65 | 31,999,602 |
| HandOffBenchmark.vtQueue | 419.76 | 185.84 | 23,248,919 |
