# Blocking vs reactive

Do virtual threads make reactive programming unnecessary? This repo puts blocking code on virtual threads (plain JDK,
and [Lark](https://github.com/matthewjones372/lark)'s `lark-stream`) against Project Reactor and Pekko Streams, on
workloads chosen to favour the reactive side.

The author prefers the blocking side. So the predictions were written down and committed in
[`HYPOTHESES.md`](HYPOTHESES.md) before anything was measured, and every result is reported, losses included.

## In short

- **Backpressure is as easy with virtual threads as with Reactor, but you still have to bound the work.** Every
  bounded contestant held 16–17 elements in flight; the unbounded virtual-thread version let 18,289 pile up.
- **At 1,000 calls in flight, every model ties:** 93–96k ops/s against a ceiling of 100k.
- **From 10,000 calls in flight up, Lark's `mapAsync` beats Reactor.** It does 1.2–1.5× Reactor's throughput, at
  similar CPU per call, with 60% of the heap per pending call.
- **On real sockets, blocking code is no more expensive than Reactor Netty.** Virtual threads blocked in a plain
  `Socket` used about two-thirds of Reactor Netty's CPU per call. The 2.5–4× gap on the shared device comes from how
  its virtual threads are woken (by a platform thread), not from blocking itself. The same gap reappears on the JDK's
  `HttpClient`, which wakes them the same way, and Reactor pays it too on that client.
- **Lark's adaptive `Limit` protects a dependency that loses capacity.** p99 after the drop is 10 ms, against 30 ms
  for Reactor at its best fixed concurrency. It costs 13% of throughput while it ramps up.
- **Reactor still wins on CPU-only work.** It hands elements between threads 1.8× faster than Lark, and runs a
  CPU-only operator chain 1.6× faster.

## Throughput as concurrency grows

![Throughput at 1k, 10k and 100k calls in flight for Lark mapAsync, Reactor, Pekko, Lark mapPar and plain virtual threads](docs/charts/throughput.svg)

All four models tie at 1,000 calls in flight, where the 100k ops/s ceiling is the limit. From 10,000 calls in flight
up, Lark's `mapAsync` pulls away. The models that park a thread per call (`mapPar`, plain virtual threads) fall
behind Reactor and Pekko.

## The tests

Each I/O test sends elements from a source as fast as it can make them, through one 10 ms I/O call each, with at most
*N* calls in flight, and sums the answers. Each contestant's code is in
[`IoScenario.kt`](src/main/kotlin/bvr/IoScenario.kt) between `region` markers.

| contestant | what it is |
|---|---|
| Lark `mapAsync` | `mapAsync(n) { asyncCall(it) }`: composes a `CompletionStage` and parks no thread per call |
| Lark `mapAsyncUnordered` | the same, passing answers on in completion order |
| Lark `mapPar` | `mapPar(n) { blockingCall(it) }`: one virtual thread per call in flight |
| Lark `mapParUnordered` | the same, in completion order |
| Reactor `flatMap` | `Flux.flatMap({ Mono.fromFuture(asyncCall(it)) }, n)` |
| Pekko `mapAsyncUnordered` | `Source.mapAsyncUnordered(n) { asyncCall(it) }` |
| Virtual threads + `Semaphore` | plain JDK: a `Semaphore(n)` and one virtual thread per call |

There are two I/O devices, because the device changes the result:

- **Shared device.** Every contestant calls the same fake client: a `CompletableFuture` completed by a two-thread
  timer, as a socket poller or a Netty event loop would complete it. The async contestants compose the future; the
  blocking ones `join()` it. This is the fairer comparison, and the one the headline numbers use.
- **Each model's own timer.** Virtual threads `Thread.sleep`, Reactor uses `Mono.delay` on its parallel scheduler,
  and Lark's `mapAsync` uses a JDK `ScheduledThreadPoolExecutor` with a thread per core, as Reactor's scheduler has.
  Pekko uses its own scheduler, which ticks every millisecond: on its own that caps 16 calls in flight at about
  4,900 ops/s, so Pekko's low-concurrency rows here measure the tick.

[`Harness.kt`](src/main/kotlin/bvr/Harness.kt) measures:
- throughput, process CPU and allocation;
- the most elements ever outstanding, sampled every millisecond;
- live heap per pending call, from a full GC taken mid-run once at least 90% of the target concurrency is reached.

Medians over 2 JVM forks × 4 measured iterations, after 2 warm-up iterations. Every number is in
[`results/summary.md`](results/summary.md).

### Backpressure

![Most elements in flight, and throughput, with at most 16 calls allowed](docs/charts/backpressure.svg)

There's a fast producer and a slow consumer (1 ms calls), with at most 16 in flight. Every bounded contestant held
16–17 elements in flight, at the same throughput. The virtual-thread version with no bound held 18,289. Virtual
threads don't give you backpressure for free, but a `Semaphore` or a bounded window is all it takes.

### 1,000 calls in flight

![Throughput, CPU and memory with 1,000 calls in flight](docs/charts/fanout-1k.svg)

Every model reaches 93–96k ops/s against a ceiling of 100k. At typical service concurrency, throughput is no reason
to choose a model. Lark's `mapAsync` uses the least CPU per call and the least heap per pending call.

### 10,000 calls in flight

![Throughput, CPU and memory with 10,000 calls in flight](docs/charts/fanout-10k.svg)

- **Shared device.** Lark's `mapAsync` does 672k ops/s against Reactor's 434k, at a little more CPU per call (3.49
  against 3.06 µs). It holds 200 B per pending call against Reactor's 322 B.
- **Blocking bodies.** `mapPar` (250k ops/s, 11.6 µs per call) and plain virtual threads trail Reactor here, and hold
  0.7–1.2 KB per pending call.
- **Own timers.** Blocking bodies win: `mapPar` does 771k ops/s and plain virtual threads 829k, against Reactor's
  269k.

### 100,000 calls in flight

![Throughput, CPU and memory with 100,000 calls in flight](docs/charts/inflight-100k.svg)

- **Shared device.** Lark's `mapAsync` does 452k ops/s against Reactor's 379k and Pekko's 378k. Pekko uses the least
  CPU per call (4.75 µs, against Lark's 5.57 and Reactor's 5.94). Lark peaks at 634 MB RSS, against Pekko's 1,023 MB
  and Reactor's 2,604 MB.
- **Reactor's heap.** Reactor never reached 90% of its 100,000-call target, so its live heap per call couldn't be
  measured.
- **Own timers.** Reactor's `flatMap` over `Mono.delay` collapsed to 9k ops/s, against about 1M for the others. Thread dumps show the whole pipeline
  on one `parallel` worker inside `FluxFlatMap.drainLoop`, so treat that bar as Reactor at an extreme, not Reactor in
  general.

### Real sockets

![CPU per call and throughput at 16, 1,000 and 10,000 HTTP calls in flight over loopback](docs/charts/net.svg)

Each call is an HTTP/1.1 `GET` over loopback, with keep-alive, to a server in its own JVM (Netty, one event-loop
thread pinned to its own core) that answers after 10 ms. The client gets the other 3 cores. Code:
[`NetScenario.kt`](src/main/kotlin/bvr/NetScenario.kt), [`NetServer.kt`](src/main/kotlin/bvr/NetServer.kt).

CPU per call, µs:

| contestant | 16 | 1k | 10k |
|---|--:|--:|--:|
| Virtual threads over a plain `Socket` | 86 | 28.5 | 26.6 |
| Lark `mapPar` over a plain `Socket` | 93 | 26.5 | 26.7 |
| Reactor Netty `HttpClient` | 148 | 41.7 | 42.9 |
| Virtual threads on the JDK `HttpClient` | 374 | 118 | 209 |
| Lark `mapPar` on the JDK `HttpClient` | 356 | 140 | 610 |
| Lark `mapAsync` on the JDK `HttpClient` | 356 | 160 | 1,146 |
| Reactor on the JDK `HttpClient` | 336 | 143 | 542 |
| Pekko on the JDK `HttpClient` | 416 | 142 | 522 |

- **Blocking sockets.** A virtual thread blocked in a socket read is woken by the JDK's own poller. The same
  `Semaphore` loop and the same `mapPar` used 0.6–0.7× Reactor Netty's CPU per call, and a quarter of its allocation.
  The socket client is about 40 lines written for this test, and does less than Reactor Netty's full HTTP codec, so
  read this as "blocking sockets are not more expensive", not as a margin to bank. At 1k and 10k both socket
  contestants hit the one-core server's ceiling of about 70k calls a second.
- **The JDK `HttpClient`.** It is asynchronous inside: `send` is `sendAsync(...).get()`, so a virtual thread in `send`
  is woken by a platform thread, as on the shared device. Everything on it costs 2.4–3.4× Reactor Netty's CPU at 16
  and 1k, Reactor included. At 10k connections it collapses for everyone (1.7–10.5k ops/s): its pool walks a list of
  every idle connection under one lock, and its default executor grew to about 8,800 platform threads.
- **Scheduler settings** ([chart](docs/charts/net-scheduler.svg)). JDK 25 accepts `-Djdk.pollerMode=1` (platform
  poller threads) and `2` (virtual-thread pollers, Linux's default). Mode 1 cost the socket contestants 5–19% more CPU.
  `-Djdk.virtualThreadScheduler.parallelism=2` was within the noise. The defaults are right.

### A dependency that loses capacity

![p99 latency before and after a dependency's capacity drops, and each run's length](docs/charts/overload.svg)

The dependency serves 500 calls at once, 5 ms each. After 100,000 of the 200,000 calls, its capacity drops to 100, as
if a replica failed. Latency is measured per call, including the dependency's own queue, which is what a caller's
timeout sees.

- **Fixed concurrency.** Every fixed setting sees its p99 grow 4–5× when capacity drops: 54 ms at 1,000, and
  30 ms even at the best fixed number.
- **The adaptive limit.** `mapPar(Limit.adaptive(…))` settles at about 105 against the new capacity of 100, and holds
  p99 at 10 ms.
- **The cost.** It finishes 13% later, because it ramps up from a starting limit of 20.
- **Reactor.** It has nothing built in that adapts. Netflix's `concurrency-limits` library would close the gap.

### CPU-only work and hand-offs

![Milliseconds per million elements for a hand-off between threads and for a CPU-only chain](docs/charts/jmh.svg)

These are JMH runs, measured in ms per million elements.
- **Hand-off between threads.** Reactor's `publishOn` takes 26 ms and Lark's `buffer` 48 ms, a ninth of what a plain
  `ArrayBlockingQueue` between virtual threads takes (441 ms). Reactor's number barely crosses threads: after the
  first batch, 99.96% of its elements are emitted on the consumer's own thread, because `publishOn` requests more from
  its drain loop and `range` emits on whichever thread asks. Lark's `buffer` runs upstream on a thread of its own.
- **CPU-only chain.** For map, filter, map and sum, Reactor's operator fusion beats Lark by 1.6×, and a plain loop
  beats everything.

### Code and stack traces

![Lines of code each contestant needs](docs/charts/code.svg)

The stream DSLs are short; plain virtual threads with a `Semaphore` take 18 lines.

Where blocking code wins clearly is the stack trace when a call fails
([`results/stacktraces.txt`](results/stacktraces.txt)):

| contestant | names the handler | names the failing fetch |
|---|---|---|
| Virtual threads + `Semaphore` | yes | yes |
| Lark | yes | yes |
| Reactor | only through `block()`, which a WebFlux handler never calls | no |
| Pekko | yes | no |

## Against the pre-registered hypotheses

| # | Prediction | Result |
|---|---|---|
| H1 | Every bounded contestant keeps in-flight work below a few hundred; the unbounded one doesn't; throughput within 10% | **Confirmed** |
| H2 | At 1k and 10k in flight, virtual threads within ±15% of Reactor, using more CPU | **Mixed.** A tie at 1k. At 10k Lark's `mapAsync` is 1.5× Reactor at similar CPU. Blocking bodies use more CPU on the shared device, but less than Reactor Netty on real sockets |
| H3 | At 100k in flight, Reactor needs less heap per pending call | **Refuted for `mapAsync`** (197 B vs about 350 B); **true for blocking bodies** (about 900 B) |
| H4 | On a CPU-only chain a plain loop wins, and Reactor beats Lark and Pekko | **Confirmed** |
| H5 | Reactor's `publishOn` beats a blocking queue between virtual threads | **Confirmed**, by 1.8× over Lark's `buffer`, though Reactor's run barely crosses threads |
| H6 | Blocking code is shorter, and its trace names the caller | **Half refuted.** The traces are as predicted; the line counts are not |
| H7 | When a dependency loses capacity, an adaptive limit matches fixed concurrency on throughput and has a lower p99 | **Half met.** p99 2.9× lower; throughput 13% lower |

## Caveats

- One 4-core cloud VM. Treat differences under about 20% as noise.
- The I/O tests use fake I/O. The shared device completes every call on a platform thread, which is the most
  expensive way to wake a virtual thread. The real-socket test is loopback only: no network latency, loss or
  bandwidth, and a one-core server that caps at about 70k calls a second.
- Neither the Reactor nor the Pekko code has been reviewed by someone who writes it for a living. Pull requests that
  make either faster are welcome, and the results will be re-run with them.
- Lark is built from its `main` branch, which holds specs 0170–0178 (unordered windows, the adaptive `Limit`, and the
  faster hand-off and window) but isn't released yet, and published locally.
- Versions: JDK 25.0.5 (Temurin), Kotlin 2.4.10, reactor-core 3.8.8, reactor-netty-http 1.3.7, pekko-stream_2.13
  1.2.1.

## Not yet tested

- **A real network**, between machines, and servers (Reactor Netty against a virtual-thread server) driven by an
  open-model load generator at and beyond saturation.
- Kotlin coroutines and `Flow`.
- `mapAsync` under a `Limit`.

## Running it

```bash
# JDK 25 on the toolchain path. Build Lark from its main branch first:
# in the lark checkout,
./gradlew :lark:publishToMavenLocal :lark-stream:publishToMavenLocal :lark-stream-forks:publishToMavenLocal \
    -Prelease.forceVersion=0.0.0-cand

# then here
export LARK_VERSION=0.0.0-cand-SNAPSHOT
scripts/run-jmh.sh                 # results/jmh.json
scripts/run-io.sh                  # results/io.txt
scripts/run-net.sh                 # results/net.txt; about 3 hours
build/install/blocking-vs-reactive/bin/blocking-vs-reactive stacktraces > results/stacktraces.txt
python3 scripts/summarise.py > results/summary.md
python3 scripts/charts.py          # docs/charts/*.svg
```

## Licence

[Apache 2.0](LICENSE), as Lark is.
