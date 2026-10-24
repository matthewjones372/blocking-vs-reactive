
#### `backpressure`

| contestant | ops/s | CPU µs/op | alloc B/op | max outstanding | live B per in-flight | RSS peak MB |
|---|--:|--:|--:|--:|--:|--:|
| vt-plain | 11,544 | 34.00 | 1,182 | 17 | – | 216 |
| lark | 11,698 | 35.75 | 1,386 | 17 | – | 234 |
| lark-unordered | 11,452 | 39.25 | 1,460 | 17 | – | 241 |
| lark-async | 11,762 | 25.25 | 250 | 17 | – | 169 |
| lark-async-unordered | 11,608 | 25.00 | 373 | 17 | – | 173 |
| reactor | 13,355 | 11.00 | 383 | 16 | – | 184 |
| pekko | 12,222 | 26.00 | 431 | 17 | – | 252 |
| vt-naive | 215,064 | 16.00 | 1,220 | 19,997 | – | 260 |

#### `backpressure-native`

| contestant | ops/s | CPU µs/op | alloc B/op | max outstanding | live B per in-flight | RSS peak MB |
|---|--:|--:|--:|--:|--:|--:|
| vt-plain | 11,300 | 31.75 | 946 | 17 | – | 196 |
| lark | 12,070 | 27.25 | 1,062 | 17 | – | 220 |
| lark-unordered | 11,986 | 31.50 | 1,228 | 17 | – | 234 |
| lark-async | 11,658 | 27.50 | 256 | 17 | – | 172 |
| lark-async-unordered | 11,740 | 27.75 | 378 | 17 | – | 176 |
| reactor | 13,460 | 30.00 | 410 | 16 | – | 191 |
| pekko | 4,720 | 36.00 | 536 | 17 | – | 252 |
| vt-naive | 462,576 | 8.50 | 943 | 19,058 | – | 234 |

#### `fanout-1k`

| contestant | ops/s | CPU µs/op | alloc B/op | max outstanding | live B per in-flight | RSS peak MB |
|---|--:|--:|--:|--:|--:|--:|
| vt-plain | 94,472 | 11.18 | 1,302 | 1,001 | 1,320 | 396 |
| lark | 93,068 | 10.05 | 1,274 | 1,001 | 1,064 | 417 |
| lark-unordered | 92,814 | 9.35 | 1,534 | 1,001 | 1,306 | 447 |
| lark-async | 95,106 | 4.08 | 233 | 1,001 | 226 | 248 |
| lark-async-unordered | 94,161 | 4.70 | 360 | 1,001 | 309 | 299 |
| reactor | 97,097 | 6.35 | 868 | 1,000 | 348 | 351 |
| pekko | 96,306 | 8.60 | 428 | 1,001 | 309 | 389 |

#### `fanout-1k-native`

| contestant | ops/s | CPU µs/op | alloc B/op | max outstanding | live B per in-flight | RSS peak MB |
|---|--:|--:|--:|--:|--:|--:|
| vt-plain | 94,710 | 6.45 | 920 | 1,001 | 793 | 343 |
| lark | 94,364 | 5.88 | 1,036 | 1,001 | 856 | 387 |
| lark-unordered | 93,842 | 7.38 | 1,194 | 1,001 | 1,137 | 454 |
| lark-async | 94,423 | 4.25 | 231 | 1,001 | 225 | 252 |
| lark-async-unordered | 93,665 | 4.75 | 359 | 1,001 | 309 | 298 |
| reactor | 97,418 | 10.38 | 822 | 1,000 | 381 | 367 |
| pekko | 80,175 | 4.45 | 536 | 1,001 | 310 | 391 |

#### `fanout-10k`

| contestant | ops/s | CPU µs/op | alloc B/op | max outstanding | live B per in-flight | RSS peak MB |
|---|--:|--:|--:|--:|--:|--:|
| vt-plain | 242,838 | 12.10 | 1,282 | 10,001 | 1,342 | 1,890 |
| lark | 246,727 | 11.24 | 1,350 | 10,001 | 900 | 2,120 |
| lark-unordered | 328,693 | 9.66 | 1,556 | 10,001 | 1,318 | 2,146 |
| lark-async | 669,873 | 3.35 | 237 | 10,001 | 205 | 396 |
| lark-async-unordered | 593,916 | 3.96 | 364 | 10,001 | 286 | 493 |
| reactor | 360,994 | 3.79 | 948 | 10,000 | 335 | 1,053 |
| pekko | 364,775 | 4.75 | 426 | 10,001 | 299 | 629 |

#### `fanout-10k-native`

| contestant | ops/s | CPU µs/op | alloc B/op | max outstanding | live B per in-flight | RSS peak MB |
|---|--:|--:|--:|--:|--:|--:|
| vt-plain | 759,183 | 4.67 | 893 | 10,001 | 811 | 1,085 |
| lark | 773,396 | 4.52 | 1,050 | 10,001 | 742 | 1,370 |
| lark-unordered | 650,304 | 5.70 | 1,272 | 10,001 | 992 | 1,871 |
| lark-async | 595,733 | 2.55 | 232 | 10,001 | 210 | 390 |
| lark-async-unordered | 569,064 | 2.88 | 359 | 10,001 | 276 | 494 |
| reactor | 274,986 | 5.75 | 960 | 10,000 | 362 | 1,086 |
| pekko | 744,572 | 2.93 | 548 | 10,001 | 287 | 716 |

#### `inflight-100k`

| contestant | ops/s | CPU µs/op | alloc B/op | max outstanding | live B per in-flight | RSS peak MB |
|---|--:|--:|--:|--:|--:|--:|
| vt-plain | 180,934 | 15.58 | 1,210 | 100,001 | 852 | 4,067 |
| lark | 194,444 | 14.28 | 1,363 | 100,001 | 904 | 3,176 |
| lark-unordered | 247,142 | 12.79 | 1,588 | 100,001 | 1,066 | 3,862 |
| lark-async | 512,598 | 4.87 | 246 | 100,001 | 192 | 626 |
| lark-async-unordered | 422,272 | 6.07 | 374 | 100,001 | 270 | 1,020 |
| reactor | 364,312 | 5.88 | 949 | 100,000 | nan | 2,625 |
| pekko | 368,862 | 4.67 | 426 | 100,001 | 149 | 1,007 |

#### `inflight-100k-native`

| contestant | ops/s | CPU µs/op | alloc B/op | max outstanding | live B per in-flight | RSS peak MB |
|---|--:|--:|--:|--:|--:|--:|
| vt-plain | 982,250 | 3.96 | 922 | 100,001 | 804 | 2,164 |
| lark | 983,618 | 3.94 | 1,058 | 100,001 | 588 | 2,471 |
| lark-unordered | 748,322 | 5.19 | 1,339 | 100,001 | 590 | 2,832 |
| lark-async | 838,538 | 2.88 | 236 | 100,000 | nan | 606 |
| lark-async-unordered | 757,075 | 3.36 | 364 | 100,000 | nan | 848 |
| reactor | 10,495 | 99.41 | 935 | 100,000 | 345 | 2,595 |
| pekko | 948,354 | 3.12 | 536 | 100,001 | 202 | 1,300 |

#### `overload`: capacity drops from 500 to 100 halfway

| contestant | seconds | ops/s | p50 before ms | p99 before ms | p50 after ms | p99 after ms |
|---|--:|--:|--:|--:|--:|--:|
| reactor-1000 | 6.28 | 31,864 | 10.4 | 13.2 | 52.2 | 56.4 |
| reactor-tuned | 6.29 | 31,783 | 5.1 | 6.2 | 26.1 | 28.5 |
| lark-1000 | 6.31 | 31,690 | 10.1 | 12.9 | 52.0 | 55.8 |
| lark-adaptive | 7.96 | 25,134 | 5.1 | 6.9 | 5.1 | 10.1 |

#### `net-16`: real sockets, HTTP/1.1 over loopback

| contestant | ops/s | CPU µs/op | alloc B/op | max outstanding | live B per in-flight | RSS peak MB | server CPU % |
|---|--:|--:|--:|--:|--:|--:|--:|
| vt-plain | 1,440 | 410.00 | 15,625 | 17 | – | 355 | 14 |
| lark | 1,412 | 400.00 | 15,564 | 17 | – | 368 | 12 |
| lark-async | 1,360 | 381.25 | 14,730 | 17 | – | 348 | 12 |
| reactor-netty | 1,492 | 192.50 | 8,254 | 16 | – | 298 | 16 |
| reactor-jdk | 1,453 | 356.25 | 14,917 | 16 | – | 350 | 14 |
| pekko | 1,392 | 525.00 | 15,005 | 17 | – | 352 | 15 |
| vt-socket | 1,451 | 121.25 | 2,070 | 17 | – | 188 | 12 |
| lark-socket | 1,450 | 107.50 | 2,166 | 16 | – | 206 | 12 |

#### `net-1k`: real sockets, HTTP/1.1 over loopback

| contestant | ops/s | CPU µs/op | alloc B/op | max outstanding | live B per in-flight | RSS peak MB | server CPU % |
|---|--:|--:|--:|--:|--:|--:|--:|
| vt-plain | 19,891 | 123.80 | 15,342 | 1,001 | 13,638 | 1,733 | 42 |
| lark | 15,570 | 158.85 | 15,785 | 1,001 | 469 | 1,888 | 34 |
| lark-async | 14,000 | 183.95 | 14,718 | 1,001 | 4,698 | 2,408 | 32 |
| reactor-netty | 46,198 | 45.25 | 8,436 | 1,000 | 3,052 | 1,268 | 72 |
| reactor-jdk | 17,472 | 145.80 | 15,060 | 1,000 | 4,864 | 1,906 | 40 |
| pekko | 17,705 | 142.85 | 14,881 | 1,001 | 4,158 | 2,112 | 37 |
| vt-socket | 73,941 | 26.00 | 1,958 | 1,001 | 998 | 358 | 88 |
| lark-socket | 75,350 | 23.25 | 2,116 | 1,001 | 668 | 386 | 84 |

#### `net-10k`: real sockets, HTTP/1.1 over loopback

| contestant | ops/s | CPU µs/op | alloc B/op | max outstanding | live B per in-flight | RSS peak MB | server CPU % |
|---|--:|--:|--:|--:|--:|--:|--:|
| vt-plain | 10,492 | 211.45 | 15,688 | 10,001 | 733 | 2,436 | 30 |
| lark | 2,554 | 637.60 | 17,197 | 10,001 | 703 | 2,582 | 8 |
| lark-async | 1,365 | 1,371.50 | 15,081 | 10,001 | 3,572 | 2,998 | 6 |
| reactor-netty | 39,554 | 50.80 | 8,653 | 10,000 | 2,788 | 1,116 | 68 |
| reactor-jdk | 4,109 | 590.65 | 14,926 | 10,000 | 3,428 | 2,584 | 20 |
| pekko | 3,545 | 690.30 | 14,577 | 10,001 | 3,291 | 2,614 | 17 |
| vt-socket | 53,150 | 31.85 | 1,930 | 10,001 | 510 | 422 | 92 |
| lark-socket | 47,038 | 35.20 | 1,992 | 10,001 | 475 | 407 | 92 |

#### Real network, blocking contestants under other scheduler settings (CPU µs/op, and ops/s)

| scenario | contestant | default | parallelism-2 | poller-1 | poller-2 |
|---|---|--:|--:|--:|--:|
| net-1k | vt-plain | 123.80 (19,891) | 148.35 (16,532) | 119.15 (21,468) | 82.60 (30,594) |
| net-1k | lark | 158.85 (15,570) | 171.50 (14,294) | 102.35 (24,566) | 97.60 (25,570) |
| net-1k | vt-socket | 26.00 (73,941) | 29.35 (62,732) | 19.95 (85,332) | 19.30 (91,268) |
| net-1k | lark-socket | 23.25 (75,350) | 23.25 (62,820) | 20.10 (80,202) | 17.40 (86,750) |
| net-10k | vt-plain | 211.45 (10,492) | 245.65 (8,958) | 163.15 (13,688) | 148.80 (14,820) |
| net-10k | lark | 637.60 (2,554) | 506.55 (3,293) | 443.75 (3,960) | 429.75 (3,910) |
| net-10k | vt-socket | 31.85 (53,150) | 27.00 (69,330) | 23.05 (81,286) | 21.05 (85,586) |
| net-10k | lark-socket | 35.20 (47,038) | 23.00 (70,756) | 22.70 (74,704) | 22.85 (77,801) |

#### JMH (ms per million elements, lower is better)

| benchmark | ms/op | ± | alloc B/op |
|---|--:|--:|--:|
| CpuChainBenchmark.lark | 19.52 | 2.96 | 56,007,455 |
| CpuChainBenchmark.loop | 0.93 | 0.22 | 0 |
| CpuChainBenchmark.pekko | 115.10 | 15.07 | 56,082,304 |
| CpuChainBenchmark.reactor | 15.88 | 0.71 | 12,000,339 |
| CpuChainBenchmark.sequence | 13.75 | 1.63 | 43,996,951 |
| HandOffBenchmark.lark | 45.31 | 6.79 | 40,164,559 |
| HandOffBenchmark.pekko | 1118.04 | 125.29 | 121,261,019 |
| HandOffBenchmark.reactor | 30.29 | 1.79 | 31,999,786 |
| HandOffBenchmark.vtQueue | 282.77 | 67.40 | 23,346,111 |
