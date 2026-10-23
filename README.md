# Blocking vs reactive

Do virtual threads make reactive programming unnecessary? This repo puts blocking code on virtual threads (plain JDK,
and [Lark](https://github.com/matthewjones372/lark)'s `lark-stream`) against Project Reactor and Pekko Streams, on
workloads chosen to favour the reactive side.

The author prefers the blocking side. So the predictions were written down and committed in
[`HYPOTHESES.md`](HYPOTHESES.md) before anything was measured. Every result is reported, losses included.

There have been two rounds:

1. **Lark 0.10.0**, as released. Its results are kept in [`results/lark-0.10.0`](results/lark-0.10.0).
2. **Lark with specs 0169–0171**, built from the stacked pull requests
   [#479–#487](https://github.com/matthewjones372/lark/pulls). Round 1 showed where Lark lost; these specs set out
   to fix those losses and add an adaptive limit. Its results are in [`results`](results). Nothing from round 2 is
   released yet.

## The verdict in six lines

1. **Backpressure is as easy with virtual threads as with Reactor, but you still have to bound the work.**
   - Every bounded contestant held 16–17 elements in flight.
   - The deliberately unbounded virtual-thread version let 19,845 pile up.
2. **At 1,000 calls in flight, every model ties on throughput:** 94–98k ops/s against a ceiling of 100k.
3. **At 10k–100k in flight, Lark's `mapAsync` now beats Reactor in this harness.**
   - Throughput: 817k vs 522k ops/s at 10k, and 859k vs 455k at 100k, on the same fake I/O device.
   - CPU: about the same per call at 10k, and less at 100k.
   - Memory: 60% of Reactor's live heap per pending call. In round 1 Lark lost all three.
4. **A blocking body still costs a parked virtual thread.**
   - `mapPar` and plain virtual threads hold about 850–1,000 B per pending call, against about 320 B for Reactor.
   - Reactor and Pekko still use less CPU per call there.
   - For an async client, `mapAsync` is the right tool, and it is the one that wins.
5. **Lark's adaptive `Limit` cuts the tail when a dependency loses capacity.**
   - p99 after the drop: 10 ms, against 27 ms for Reactor at its best fixed concurrency and 54–59 ms at a typical
     setting.
   - It costs 13% of throughput while it ramps up. Reactor and Pekko have nothing comparable.
6. **Reactor still wins on raw CPU work and hand-offs.**
   - It moves elements between threads 5.5× faster than Lark's improved `buffer`.
   - It runs a CPU-only operator chain 1.6× faster.

**Claims this repo supports:**

- With virtual threads, backpressure is a semaphore or a bounded window, not a protocol.
- At typical concurrency, blocking code matches reactive throughput and is far easier to debug.
- Lark's `mapAsync` beats Reactor's `flatMap` on throughput and memory in this harness.
- An adaptive limit protects a degraded dependency better than any fixed number.

**Claims it does not support:**

- "Virtual threads beat reactive everywhere."
- "A parked thread is as light as a callback."

## Scorecard against the pre-registered hypotheses

The hypotheses were written before round 1; H7 is spec 0171's own acceptance test, also written before it was run.

| # | Prediction | Round 1 (0.10.0) | Round 2 (specs 0169–0171) |
|---|---|---|---|
| H1 | Every bounded contestant keeps in-flight work below a few hundred; `vt-naive` doesn't; throughput within 10% | Confirmed | **Confirmed.** Max outstanding was 16–17 for every bounded contestant and 19,845 for `vt-naive`. Throughput was within 10% on the shared device. |
| H2 | At 1k and 10k in flight, virtual threads within ±15% of Reactor, using more CPU | Mixed: a tie at 1k; at 10k, Reactor 2.2× faster on the shared device | **Mixed, now in Lark's favour.** A tie at 1k. At 10k, `lark-async` 817k vs Reactor 522k, at the same CPU (2.62 vs 2.57 µs per call). `vt-plain` and `mapPar` were 358k and 414k, still behind Reactor on the shared device. |
| H3 | At 100k in flight, Reactor needs less heap per pending call | Confirmed | **Refuted for `mapAsync`** (191 B vs about 340 B for Reactor, and 672 MB peak RSS vs 2,226 MB). **Still true for blocking bodies:** `vt-plain` and `mapPar` hold about 1,000 B. |
| H4 | On a CPU-only chain a plain loop wins, and Reactor beats Lark and Pekko | Confirmed | **Confirmed.** Unchanged, since no spec touched it: Reactor 15.1 ms, Lark 24.1 ms. |
| H5 | Reactor's `publishOn` beats a blocking queue between virtual threads | Confirmed: 17× | **Confirmed, by less.** Lark's `buffer` went from 520 to 137 ms per million elements; Reactor takes 25 ms. Spec 0169 aimed for within 2× and missed. |
| H6 | Blocking code is shorter, and its stack trace names the caller | Half refuted | **Unchanged.** The traces are as predicted. On lines of code, Reactor 6 and Pekko 5 beat Lark's 8 and `vt-plain`'s 18. |
| H7 | When capacity drops 5×, the adaptive limit matches fixed concurrency on throughput and has a lower p99 | — | **Half met.** p99 after the drop 10 ms vs 27 ms (Reactor tuned) and 54–59 ms (fixed 1,000). Throughput 13% lower: the limit ramps up from 20 during a first phase that lasts one second. |

## What was measured

Every I/O scenario sends `items` elements from a source as fast as the source can make them, through one I/O call
each, with at most `concurrency` calls in flight, and sums the answers.
[`IoScenario.kt`](src/main/kotlin/bvr/IoScenario.kt) has each contestant's code between `region` markers.
[`Harness.kt`](src/main/kotlin/bvr/Harness.kt) measures:
- throughput, process CPU and allocation;
- the most elements ever outstanding, sampled every millisecond;
- live heap per in-flight element, from a full GC taken mid-run once at least 90% of the target concurrency is
  reached.

There are two I/O devices, because the device changes the result:

- **Shared device**: one `CompletableFuture` per call, completed by a shared two-thread
  `ScheduledThreadPoolExecutor`, as a socket poller or a Netty event loop would complete it.
  - Reactor, Pekko and `lark-async` compose the future.
  - The blocking contestants call `join()`.
- **Native timers (`-native`)**: each model waits on its own timer.
  - Virtual threads `Thread.sleep`.
  - Reactor uses `Mono.delay`.
  - Pekko *and `lark-async`* use Pekko's `Patterns.after`. So `lark-async`'s native rows measure Pekko's scheduler,
    which is why they match Pekko's at low concurrency.

Neither device is a real socket. See [Not yet tested](#not-yet-tested).

### Lark contestants

| name | code |
|---|---|
| `lark` | `mapPar(n) { blockingCall(it) }` |
| `lark-unordered` | `mapParUnordered(n) { … }` (spec 0170) |
| `lark-async` | `mapAsync(n) { asyncCall(it) }` |
| `lark-async-unordered` | `mapAsyncUnordered(n) { … }` (spec 0170) |
| `lark-adaptive` | `mapPar(Limit.adaptive(…)) { … }` (spec 0171), in the overload scenario only |

## Results, round 2

Medians over 2 JVM forks × 4 measured iterations, after 2 warm-up iterations, on one 4-core VM. Every table is in
[`results/summary.md`](results/summary.md).

**10k in flight, 10 ms calls, shared device (ceiling: 1M ops/s)**

| contestant | ops/s | CPU µs/op | live B per in-flight | RSS peak MB |
|---|--:|--:|--:|--:|
| **lark-async** | **816,882** | 2.62 | **192** | **413** |
| lark-async-unordered | 748,560 | 3.08 | 276 | 494 |
| reactor | 521,981 | 2.57 | 320 | 1,088 |
| lark-unordered | 517,124 | 6.66 | 1,320 | 2,392 |
| pekko | 445,352 | 3.85 | 278 | 594 |
| lark | 413,962 | 7.49 | 876 | 2,014 |
| vt-plain | 358,350 | 8.71 | 846 | 1,556 |

**100k in flight, 10 ms calls, shared device (ceiling: 10M ops/s)**

| contestant | ops/s | CPU µs/op | live B per in-flight | RSS peak MB |
|---|--:|--:|--:|--:|
| **lark-async** | **858,568** | **3.00** | **191** | **672** |
| lark-async-unordered | 726,444 | 3.72 | 266 | 850 |
| pekko | 479,580 | 3.63 | 230 | 1,022 |
| reactor | 454,594 | 5.07 | – (never reached 90k outstanding) | 2,226 |
| lark-unordered | 347,651 | 9.70 | 1,172 | 3,726 |
| lark | 300,966 | 10.34 | 993 | 3,486 |
| vt-plain | 284,762 | 11.19 | 1,002 | 3,340 |

With native timers at 100k, `lark-async` (on Pekko's scheduler) did 2.5M ops/s and `vt-plain` 1.25M. Reactor's
`flatMap` over `Mono.delay` collapsed to 11k ops/s, as it did in round 1: thread dumps show the whole pipeline on one
`parallel` worker inside `FluxFlatMap.drainLoop`. Treat that one as Reactor at an extreme, not Reactor in general.

**A dependency that loses capacity.** It serves 500 calls at once, 5 ms each; after 100k of the 200k calls, its
capacity drops to 100. Latency is measured per call, including the dependency's own queue, which is what a caller's
timeout sees. Medians of 2 rounds.

| contestant | run time | p99 before drop | p50 after drop | p99 after drop |
|---|--:|--:|--:|--:|
| Reactor `flatMap(…, 1000)` | 6.22 s | 11.6 ms | 51.7 ms | 58.7 ms |
| Reactor `flatMap(…, 500)`, tuned to the starting capacity | 6.20 s | 6.1 ms | 25.8 ms | 26.8 ms |
| Lark `mapPar(Limit.fixed(1000))` | 6.20 s | 12.2 ms | 51.4 ms | 53.9 ms |
| **Lark `mapPar(Limit.adaptive(…))`** | 7.25 s | 6.6 ms | **5.1 ms** | **10.1 ms** |

Reactor with Netflix's `concurrency-limits` library would close this gap. The difference is that Lark has the
adaptive limit built in.

**CPU-only and hand-off (JMH, ms per million elements)**

| | Lark 0.10.0 | Lark, round 2 | Reactor | Pekko | `ArrayBlockingQueue` between virtual threads |
|---|--:|--:|--:|--:|--:|
| hand-off across threads | 520 | **137** (±73) | 25 | 1,618 | 428 |
| map, filter, map, sum | 23.8 | 24.1 | 15.1 | 156 | plain loop 0.8, `Sequence` 10.8 |

### Stack traces

[`results/stacktraces.txt`](results/stacktraces.txt) shows what each contestant hands the person on call when one
order of ten fails to load ([`StackTraces.kt`](src/main/kotlin/bvr/StackTraces.kt)):
- The `vt-plain` and Lark traces start at `fetchOrderBlocking`, inside the handler.
- Reactor's starts on the timer thread. It reaches the handler only through `block()`'s suppressed exception, and a
  WebFlux handler never calls `block()`.
- Pekko's, like Reactor's, names the handler but not the fetch.

## What changed in Lark between the rounds

| spec | change | effect here |
|---|---|---|
| [0169](https://github.com/matthewjones372/lark/pull/477) | A `Ring` that wakes either side only in batches. `buffer`, `mapAsync` and `mapPar` move onto it, and a pull yields a few times before it parks on a result that isn't ready. | Hand-off 520 → 137 ms. `mapAsync` at 100k on the shared device: 320k → 500k ops/s comparing `main` with this stack in one session, and 859k in this full run. |
| [0170](https://github.com/matthewjones372/lark/pull/483) | `mapParUnordered` and `mapAsyncUnordered`. | On the shared device, `mapParUnordered` beats ordered `mapPar` by 15–25%; with native timers it's 6–20% slower. `mapAsyncUnordered` is slower than ordered `mapAsync` everywhere here, because every call takes the same time, so nothing waits behind a slow head. |
| [0171](https://github.com/matthewjones372/lark/pull/486) | `Limit.fixed` and `Limit.adaptive`, a Vegas-style limit; `mapPar(limit)`. | The overload table above. |

## Where Lark still loses

- **Hand-off across threads.** Lark's `buffer` is 5.5× behind Reactor's `publishOn`. A standalone prototype of the
  ring did 50 ms per million elements, so the overhead is in Lark's pull layers, not the ring itself.
- **CPU-only chains.** Lark is 1.6× behind Reactor and 2.2× behind a Kotlin `Sequence`.
- **Blocking bodies at high concurrency.**
  - `mapPar` holds about 1 KB per pending call; that's the parked virtual thread.
  - On the shared device it uses 2–3× the CPU per call of Reactor or Pekko.
- **Adaptive ramp-up.** The limit starts at 20, and climbing to 500 cost 13% of a short run's throughput.

## Caveats

- One 4-core cloud VM, which was restarted twice during round 2. The run resumed per case, so no case mixes two
  machines' halves. Treat differences under about 20% as noise.
- Fake I/O, not sockets.
- Neither the Reactor nor the Pekko code has been reviewed by someone who writes it for a living. Pull requests that
  make either faster are welcome, and the results will be re-run with them.
- Round 2 uses Lark built from unmerged pull requests, published locally as `0.0.0-cand-SNAPSHOT`.
- Versions: JDK 25.0.5 (Temurin), Kotlin 2.4.10, reactor-core 3.8.8, pekko-stream_2.13 1.2.1.

## Not yet tested

- **Real network I/O.** Reactor Netty against a virtual-thread server, driven by an open-model load generator at
  and beyond saturation.
- Kotlin coroutines and `Flow`.
- `mapAsync` under a `Limit`, which needs a permit that can be waited for without blocking.

## Running it

```bash
# JDK 25 on the toolchain path
scripts/run-jmh.sh                 # H4, H5: results/jmh.txt, results/jmh.json
scripts/run-io.sh                  # H1–H3 and H7: results/io.txt (FORKS=2 by default)
build/install/blocking-vs-reactive/bin/blocking-vs-reactive stacktraces   # H6
python3 scripts/summarise.py       # results/summary.md
python3 scripts/loc.py             # lines per contestant

# Against a Lark snapshot: in the lark checkout, on the branch to measure
./gradlew :lark:publishToMavenLocal :lark-stream:publishToMavenLocal :lark-stream-forks:publishToMavenLocal \
    -Prelease.forceVersion=0.0.0-cand
# then here
LARK_VERSION=0.0.0-cand-SNAPSHOT scripts/run-io.sh
```
