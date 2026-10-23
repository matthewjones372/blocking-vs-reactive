
#### `backpressure`

| contestant | ops/s | CPU µs/op | alloc B/op | max outstanding | live B per in-flight | RSS peak MB |
|---|--:|--:|--:|--:|--:|--:|
| vt-plain | 12,325 | 34.50 | 1,234 | 17 | – | 216 |
| lark | 12,605 | 31.75 | 1,216 | 17 | – | 231 |
| lark-unordered | 12,504 | 29.00 | 1,402 | 17 | – | 242 |
| lark-async | 12,556 | 23.75 | 256 | 17 | – | 168 |
| lark-async-unordered | 12,367 | 23.50 | 374 | 17 | – | 169 |
| reactor | 13,888 | 10.00 | 388 | 16 | – | 174 |
| pekko | 12,918 | 25.25 | 431 | 17 | – | 248 |
| vt-naive | 225,030 | 15.75 | 1,258 | 18,289 | – | 257 |

#### `backpressure-native`

| contestant | ops/s | CPU µs/op | alloc B/op | max outstanding | live B per in-flight | RSS peak MB |
|---|--:|--:|--:|--:|--:|--:|
| vt-plain | 12,332 | 33.25 | 947 | 17 | – | 196 |
| lark | 12,676 | 27.50 | 1,086 | 17 | – | 216 |
| lark-unordered | 12,466 | 31.00 | 1,240 | 17 | – | 224 |
| lark-async | 12,322 | 26.50 | 257 | 17 | – | 169 |
| lark-async-unordered | 12,304 | 27.25 | 378 | 17 | – | 174 |
| reactor | 14,354 | 23.00 | 456 | 16 | – | 191 |
| pekko | 4,862 | 34.75 | 536 | 17 | – | 260 |
| vt-naive | 439,553 | 8.25 | 919 | 19,289 | – | 239 |

#### `fanout-1k`

| contestant | ops/s | CPU µs/op | alloc B/op | max outstanding | live B per in-flight | RSS peak MB |
|---|--:|--:|--:|--:|--:|--:|
| vt-plain | 95,548 | 13.20 | 1,178 | 1,001 | 2,106 | 407 |
| lark | 92,589 | 10.45 | 1,298 | 1,001 | 1,214 | 440 |
| lark-unordered | 93,588 | 9.85 | 1,554 | 1,001 | 1,338 | 468 |
| lark-async | 95,338 | 4.45 | 237 | 1,001 | 225 | 246 |
| lark-async-unordered | 95,114 | 5.15 | 366 | 1,001 | 308 | 300 |
| reactor | 96,481 | 6.30 | 893 | 1,000 | 348 | 349 |
| pekko | 96,282 | 9.95 | 428 | 1,001 | 310 | 389 |

#### `fanout-1k-native`

| contestant | ops/s | CPU µs/op | alloc B/op | max outstanding | live B per in-flight | RSS peak MB |
|---|--:|--:|--:|--:|--:|--:|
| vt-plain | 95,621 | 5.82 | 957 | 1,001 | 614 | 343 |
| lark | 95,083 | 5.22 | 1,024 | 1,001 | 970 | 376 |
| lark-unordered | 94,702 | 6.00 | 1,235 | 1,001 | 865 | 430 |
| lark-async | 95,317 | 4.40 | 234 | 1,001 | 225 | 248 |
| lark-async-unordered | 94,406 | 4.82 | 363 | 1,001 | 309 | 294 |
| reactor | 97,809 | 11.20 | 842 | 1,000 | 362 | 362 |
| pekko | 80,500 | 4.22 | 536 | 1,001 | 309 | 386 |

#### `fanout-10k`

| contestant | ops/s | CPU µs/op | alloc B/op | max outstanding | live B per in-flight | RSS peak MB |
|---|--:|--:|--:|--:|--:|--:|
| vt-plain | 212,432 | 14.28 | 1,254 | 10,001 | 1,236 | 1,766 |
| lark | 249,592 | 11.64 | 1,310 | 10,001 | 720 | 2,040 |
| lark-unordered | 290,188 | 10.79 | 1,510 | 10,001 | 1,086 | 2,350 |
| lark-async | 671,929 | 3.49 | 241 | 10,001 | 200 | 398 |
| lark-async-unordered | 619,410 | 4.12 | 374 | 10,001 | 268 | 506 |
| reactor | 433,936 | 3.06 | 946 | 10,000 | 322 | 1,052 |
| pekko | 359,936 | 4.90 | 426 | 10,001 | 282 | 607 |

#### `fanout-10k-native`

| contestant | ops/s | CPU µs/op | alloc B/op | max outstanding | live B per in-flight | RSS peak MB |
|---|--:|--:|--:|--:|--:|--:|
| vt-plain | 828,816 | 4.14 | 981 | 10,001 | 712 | 1,192 |
| lark | 770,764 | 4.69 | 1,058 | 10,001 | 778 | 1,430 |
| lark-unordered | 699,105 | 5.21 | 1,235 | 10,001 | 958 | 1,516 |
| lark-async | 664,458 | 2.48 | 238 | 10,000 | 205 | 398 |
| lark-async-unordered | 618,510 | 2.62 | 364 | 10,000 | 290 | 496 |
| reactor | 269,256 | 5.87 | 959 | 10,000 | 359 | 1,176 |
| pekko | 742,058 | 2.79 | 536 | 10,001 | 294 | 722 |

#### `inflight-100k`

| contestant | ops/s | CPU µs/op | alloc B/op | max outstanding | live B per in-flight | RSS peak MB |
|---|--:|--:|--:|--:|--:|--:|
| vt-plain | 171,790 | 17.40 | 1,262 | 100,001 | 886 | 3,365 |
| lark | 184,392 | 14.91 | 1,424 | 100,001 | 909 | 3,209 |
| lark-unordered | 208,905 | 14.77 | 1,526 | 100,001 | 1,258 | 3,784 |
| lark-async | 451,544 | 5.57 | 252 | 100,001 | 197 | 634 |
| lark-async-unordered | 409,355 | 6.63 | 385 | 100,001 | 268 | 980 |
| reactor | 379,386 | 5.94 | 948 | 64,593 | nan | 2,604 |
| pekko | 377,962 | 4.75 | 426 | 100,001 | 161 | 1,023 |

#### `inflight-100k-native`

| contestant | ops/s | CPU µs/op | alloc B/op | max outstanding | live B per in-flight | RSS peak MB |
|---|--:|--:|--:|--:|--:|--:|
| vt-plain | 1,080,366 | 3.53 | 1,019 | 100,001 | 868 | 2,646 |
| lark | 991,374 | 3.92 | 1,036 | 100,001 | 492 | 2,514 |
| lark-unordered | 913,150 | 4.26 | 1,263 | 100,001 | 633 | 2,738 |
| lark-async | 915,878 | 2.78 | 241 | 100,000 | nan | 627 |
| lark-async-unordered | 828,328 | 3.14 | 368 | 100,000 | nan | 900 |
| reactor | 8,876 | 116.56 | 934 | 100,000 | 376 | 2,246 |
| pekko | 1,102,044 | 3.00 | 536 | 100,001 | 185 | 1,357 |

#### `overload`: capacity drops from 500 to 100 halfway

| contestant | seconds | ops/s | p50 before ms | p99 before ms | p50 after ms | p99 after ms |
|---|--:|--:|--:|--:|--:|--:|
| reactor-1000 | 6.23 | 32,088 | 10.3 | 11.6 | 52.0 | 54.4 |
| reactor-tuned | 6.26 | 31,948 | 5.1 | 6.2 | 25.9 | 29.6 |
| lark-1000 | 6.21 | 32,168 | 10.1 | 11.9 | 51.6 | 53.8 |
| lark-adaptive | 7.23 | 27,656 | 5.1 | 8.1 | 5.1 | 10.1 |

#### `net-16`: real sockets, HTTP/1.1 over loopback

| contestant | ops/s | CPU µs/op | alloc B/op | max outstanding | live B per in-flight | RSS peak MB | server CPU % |
|---|--:|--:|--:|--:|--:|--:|--:|
| vt-plain | 1,421 | 373.75 | 15,374 | 17 | – | 354 | 12 |
| lark | 1,380 | 356.25 | 15,670 | 17 | – | 364 | 12 |
| lark-async | 1,374 | 356.25 | 14,553 | 17 | – | 346 | 12 |
| reactor-netty | 1,494 | 147.50 | 8,283 | 16 | – | 304 | 14 |
| reactor-jdk | 1,433 | 336.25 | 14,592 | 16 | – | 344 | 14 |
| pekko | 1,420 | 416.25 | 14,929 | 17 | – | 346 | 14 |
| vt-socket | 1,480 | 86.25 | 1,874 | 17 | – | 189 | 12 |
| lark-socket | 1,474 | 92.50 | 2,092 | 16 | – | 204 | 11 |

#### `net-1k`: real sockets, HTTP/1.1 over loopback

| contestant | ops/s | CPU µs/op | alloc B/op | max outstanding | live B per in-flight | RSS peak MB | server CPU % |
|---|--:|--:|--:|--:|--:|--:|--:|
| vt-plain | 20,541 | 118.35 | 15,207 | 1,001 | 1,468 | 1,766 | 44 |
| lark | 17,846 | 139.55 | 15,732 | 1,001 | 2,500 | 2,165 | 34 |
| lark-async | 15,949 | 160.35 | 14,662 | 1,001 | 4,263 | 1,986 | 33 |
| reactor-netty | 43,723 | 41.65 | 8,408 | 1,000 | 3,054 | 1,152 | 70 |
| reactor-jdk | 17,674 | 143.45 | 15,176 | 1,000 | 15,442 | 1,746 | 40 |
| pekko | 17,886 | 142.35 | 14,630 | 1,001 | 3,945 | 1,730 | 36 |
| vt-socket | 68,290 | 28.45 | 1,854 | 1,001 | 1,793 | 354 | 90 |
| lark-socket | 69,564 | 26.50 | 2,050 | 1,001 | 596 | 390 | 88 |

#### `net-10k`: real sockets, HTTP/1.1 over loopback

| contestant | ops/s | CPU µs/op | alloc B/op | max outstanding | live B per in-flight | RSS peak MB | server CPU % |
|---|--:|--:|--:|--:|--:|--:|--:|
| vt-plain | 10,512 | 208.90 | 15,728 | 10,001 | 1,343 | 2,434 | 30 |
| lark | 2,685 | 609.60 | 16,821 | 10,001 | 740 | 2,550 | 9 |
| lark-async | 1,686 | 1,146.00 | 15,209 | 10,001 | 3,639 | 3,108 | 8 |
| reactor-netty | 53,994 | 42.85 | 8,458 | 10,000 | 3,045 | 1,114 | 66 |
| reactor-jdk | 4,509 | 541.60 | 15,047 | 10,000 | 3,532 | 2,500 | 23 |
| pekko | 4,600 | 521.55 | 14,607 | 10,001 | 3,284 | 2,586 | 20 |
| vt-socket | 72,170 | 26.60 | 2,040 | 10,001 | 611 | 424 | 91 |
| lark-socket | 66,210 | 26.65 | 1,930 | 10,001 | 471 | 404 | 90 |

#### Real network, blocking contestants under other scheduler settings (CPU µs/op, and ops/s)

| scenario | contestant | default | parallelism-2 | poller-1 | poller-2 |
|---|---|--:|--:|--:|--:|
| net-1k | vt-plain | 118.35 (20,541) | 119.55 (20,362) | 120.10 (20,446) | 123.95 (19,756) |
| net-1k | lark | 139.55 (17,846) | 140.75 (17,769) | 140.20 (17,835) | 137.60 (18,161) |
| net-1k | vt-socket | 28.45 (68,290) | 26.00 (68,992) | 29.85 (62,384) | 27.70 (70,134) |
| net-1k | lark-socket | 26.50 (69,564) | 22.20 (68,846) | 28.65 (57,138) | 26.70 (69,568) |
| net-10k | vt-plain | 208.90 (10,512) | 193.85 (11,510) | 211.55 (10,484) | 197.35 (11,212) |
| net-10k | lark | 609.60 (2,685) | 566.70 (2,890) | 589.35 (2,767) | 614.90 (2,760) |
| net-10k | vt-socket | 26.60 (72,170) | 28.75 (69,792) | 30.25 (63,674) | 27.15 (68,162) |
| net-10k | lark-socket | 26.65 (66,210) | 25.00 (63,084) | 31.70 (53,688) | 28.80 (63,012) |

#### JMH (ms per million elements, lower is better)

| benchmark | ms/op | ± | alloc B/op |
|---|--:|--:|--:|
| CpuChainBenchmark.lark | 25.61 | 1.70 | 56,007,569 |
| CpuChainBenchmark.loop | 0.90 | 0.09 | 0 |
| CpuChainBenchmark.pekko | 107.75 | 11.06 | 56,082,079 |
| CpuChainBenchmark.reactor | 16.02 | 1.32 | 27,998,291 |
| CpuChainBenchmark.sequence | 12.19 | 1.07 | 43,996,938 |
| HandOffBenchmark.lark | 47.85 | 1.50 | 40,217,847 |
| HandOffBenchmark.pekko | 1309.54 | 200.06 | 121,263,483 |
| HandOffBenchmark.reactor | 26.09 | 1.10 | 31,999,658 |
| HandOffBenchmark.vtQueue | 440.69 | 80.54 | 24,092,836 |
