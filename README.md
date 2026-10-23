# Blocking vs reactive

Do virtual threads make reactive programming unnecessary? This repo puts blocking code on virtual threads (plain JDK,
and [Lark](https://github.com/matthewjones372/lark)'s `lark-stream`) against Project Reactor and Pekko Streams, on
workloads chosen to favour the reactive side.

The author prefers the blocking side. So the predictions were written down and committed in
[`HYPOTHESES.md`](HYPOTHESES.md) before anything was measured, including the predictions the author expected to
lose. Every result below is reported, wins and losses alike.

## The verdict in five lines

1. **Backpressure is just as easy with virtual threads.** A `Semaphore` held every bounded contestant to 16–17
   elements in flight, the same as Reactor's `flatMap(concurrency)`. It is not free: the naive version with no
   bound let 19,989 pile up. Backpressure is a design decision in both models.
2. **At ordinary concurrency (1,000 in flight), the models tie on throughput:** every contestant reached 96–98k ops/s
   against a ceiling of 100k. Performance is not a reason to choose reactive for a typical service.
3. **At 10k–100k in flight, the I/O path decides the winner, not the programming model.** When completions arrive on
   a foreign thread (like a socket poller), Reactor did 2.2–2.4× the throughput of virtual threads, on about a
   quarter of the CPU per operation. With each model's own timer, virtual threads did 2.6× the throughput of Reactor,
   and Reactor's `flatMap` collapsed at 100k in flight.
4. **Reactive is lighter.** A pending operation held about 300 B of live heap in Reactor and 150–300 B in Pekko,
   against 640–1,160 B for a parked virtual thread. Reactor hands elements between threads about 17× faster than a
   blocking queue between virtual threads.
5. **"Easier to write" holds for debugging, not for length.** The blocking version's stack trace names the failing
   function, and the reactive ones don't. But the Reactor pipeline is 6 lines, Pekko's is 5, Lark's is 8, and the
   plain virtual-thread version is 18.

**Claims these results support:**

- With virtual threads, backpressure is a semaphore or a bounded queue, not a protocol.
- At the concurrency most services run at, blocking code matches reactive throughput and is far easier to debug.

**Claims they do not support:**

- "Virtual threads perform better than reactive."
- "Virtual threads are lighter than reactive."

## Scorecard against the pre-registered hypotheses

| # | Prediction | Result |
|---|---|---|
| H1 | Every bounded contestant holds in-flight work to a few hundred, `vt-naive` does not, and throughput is within 10% | **Confirmed.** Max outstanding was 16–17 for every bounded contestant and 19,989 for `vt-naive`. Throughput was within 10% on the shared device. With native timers Pekko fell to 4.9k ops/s, an outlier. Reactor used about ¼ of the CPU per operation on the shared device. |
| H2 | At 1k and 10k in flight, `vt-plain` and Reactor within ±15%, and virtual threads use more CPU | **Mixed.** 1k: a tie, all 96–98k ops/s. 10k on the shared device: Reactor 549k vs `vt-plain` 247k ops/s, past the 30% "change my mind" line. 10k on native timers: `vt-plain` 804k vs Reactor 306k. CPU per operation is higher for virtual threads on the shared device and lower on native timers. |
| H3 | At 100k in flight, Reactor needs less heap per pending operation | **Confirmed.** About 340 B for Reactor, 150–200 B for Pekko, and 720–1,040 B for `vt-plain`. In absolute terms, 100k pending operations cost about 100 MB on virtual threads. |
| H4 | On a CPU-only chain a plain loop wins, and Reactor beats Lark and Pekko | **Confirmed.** Loop 0.8 ms, `Sequence` 11.4 ms, Reactor 15.3 ms, Lark 23.8 ms, Pekko 164 ms (very noisy). |
| H5 | Reactor's `publishOn` beats a blocking queue between virtual threads | **Confirmed, by more than predicted.** Reactor 25 ms per million elements, `ArrayBlockingQueue` 420 ms, Lark `buffer` 520 ms, Pekko `async` 1,625 ms. |
| H6 | Blocking code is shorter, and its trace names the caller | **Half refuted.** The traces are as predicted. The line counts are not: Reactor 6, Pekko 5, Lark 8, `vt-plain` 18. |

## What was measured

Every I/O scenario sends `items` elements from a source as fast as the source can make them, through one I/O call
each, with at most `concurrency` calls in flight, and sums the answers.
[`IoScenario.kt`](src/main/kotlin/bvr/IoScenario.kt) has each contestant's code between `region` markers.
[`Harness.kt`](src/main/kotlin/bvr/Harness.kt) measures throughput, process CPU, allocation, the most elements ever
outstanding (sampled every millisecond), and live heap per in-flight element (a full GC taken mid-run, at 90% or more
of the target concurrency).

Each scenario runs against two I/O devices, because the first smoke run showed the device changes the result:

- **Shared device**: one `CompletableFuture` per call, completed by a shared two-thread `ScheduledThreadPoolExecutor`.
  Reactor and Pekko compose the future; virtual threads `join()` it. Completions arrive on a thread the model does
  not own, as they do from a socket poller or a Netty event loop. Reactor runs its whole downstream on that thread
  with no hand-off; a virtual thread has to be unparked and mounted again.
- **Native timers (`-native`)**: each model waits on its own timer. Virtual threads `Thread.sleep`, Reactor uses
  `Mono.delay` on `Schedulers.parallel()`, and Pekko uses `Patterns.after` with a 1 ms scheduler tick.

Neither device is a real socket, so neither settles which model wins on real network I/O. See
[Not yet tested](#not-yet-tested).

### I/O results

Medians over 2 JVM forks × 4 measured iterations, after 2 warm-up iterations. The full tables, including the
1k-in-flight and backpressure scenarios, are in [`results/summary.md`](results/summary.md).

**10k in flight, 10 ms calls (ceiling: 1M ops/s)**

| contestant | shared: ops/s | shared: CPU µs/op | native: ops/s | native: CPU µs/op | live B per in-flight |
|---|--:|--:|--:|--:|--:|
| vt-plain | 246,988 | 12.33 | 803,594 | 4.11 | 954–1,160 |
| lark | 312,006 | 9.32 | 768,074 | 4.64 | 914–1,127 |
| reactor | 548,712 | 2.38 | 306,006 | 5.32 | 314–354 |
| pekko | 431,077 | 4.14 | 716,630 | 3.00 | 280–288 |

**100k in flight, 10 ms calls (ceiling: 10M ops/s)**

| contestant | shared: ops/s | shared: CPU µs/op | native: ops/s | native: CPU µs/op | RSS peak MB |
|---|--:|--:|--:|--:|--:|
| vt-plain | 207,706 | 15.07 | 1,091,436 | 3.57 | 2,190–3,333 |
| lark | 205,774 | 14.00 | 944,512 | 4.13 | 2,796–4,240 |
| reactor | 505,739 | 4.41 | 12,224 | 84.77 | 2,356–2,600 |
| pekko | 415,825 | 4.31 | 1,075,609 | 3.19 | 1,022–1,277 |

Reactor at 100k with `Mono.delay` is not a typo. Thread dumps show every callback on one `parallel` worker inside
`FluxFlatMap$FlatMapMain.drainLoop`, so the whole pipeline runs on one core while the others sit idle. One
iteration recovered to 217k ops/s, so the collapse depends on state. On the shared device Reactor never reached
100k outstanding (at most 65k), so its live-heap probe could not fire. A Reactor expert should review both before
anyone quotes them.

### Stack traces

[`results/stacktraces.txt`](results/stacktraces.txt) shows what each contestant hands the person on call when one
order of ten fails to load ([`StackTraces.kt`](src/main/kotlin/bvr/StackTraces.kt)):

- **`vt-plain` and Lark**: the trace starts at `fetchOrderBlocking` and then the handler's lambda.
- **Reactor**: the trace starts in the timer thread's `FakeIo.failing` lambda. It reaches `handleRequestReactor`
  only through the suppressed `#block terminated with an error`, and a WebFlux handler never calls `block()`.
  `checkpoint()` or `ReactorDebugAgent` would add operator assembly sites, at a cost.
- **Pekko**: like Reactor. It names neither the handler nor the fetch.

To keep this fair, each side fails the way its own clients fail. A blocking client throws on the caller's stack, and
an async client completes a future from its event loop.

## Where Lark itself loses (notes for its author)

- `mapPar` held 1.9–2.3× the live heap per in-flight element of hand-written `vt-plain` at 1k in flight (about
  even at 10k and 100k). On native timers at 100k in flight, it reached 86% of `vt-plain`'s throughput.
- On the CPU-only chain Lark was 1.6× slower than Reactor and 2× slower than a Kotlin `Sequence`.
- `buffer` on `Forks()` was slower than a bare `ArrayBlockingQueue` between two virtual threads (520 vs 420 ms),
  and both were about 17–20× slower than Reactor's `publishOn`.

## Caveats

- One 4-core cloud VM, shared with the load. JMH error bars on Pekko, Lark `buffer` and `vtQueue` are wide. Treat
  differences under about 20% as noise.
- Fake I/O, not sockets.
- Neither the Reactor nor the Pekko code has been reviewed by someone who writes it for a living. Pull requests
  that make either faster are welcome, and the results will be re-run with them.
- Versions: JDK 25.0.5 (Temurin), Kotlin 2.4.10, lark-stream-forks 0.10.0, reactor-core 3.8.8,
  pekko-stream_2.13 1.2.1.

## Not yet tested

- **Real network I/O.** Reactor Netty against a virtual-thread server (Helidon SE 4 or Jetty 12), each calling a
  downstream over loopback, driven by an open-model load generator at and beyond saturation. This is the test that
  decides finding 3.
- Kotlin coroutines and `Flow`, which someone will ask about.
- Timed batching (`bufferTimeout` against `groupedWithin`), and cancellation under load.

## Running it

```bash
# JDK 25 on the toolchain path
scripts/run-jmh.sh                 # H4, H5:          results/jmh.txt, results/jmh.json
scripts/run-io.sh                  # H1–H3:           results/io.txt (FORKS=2 by default)
build/install/blocking-vs-reactive/bin/blocking-vs-reactive stacktraces   # H6
python3 scripts/summarise.py       # results/summary.md
python3 scripts/loc.py             # lines per contestant
```
