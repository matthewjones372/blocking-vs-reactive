# Blocking vs reactive

Do virtual threads make reactive programming unnecessary? This repo puts blocking code on virtual threads (plain JDK,
and [Lark](https://github.com/matthewjones372/lark)'s `lark-stream`) against Project Reactor and Pekko Streams, on
workloads chosen to favour the reactive side.

The author prefers the blocking side. So the predictions were written down and committed in
[`HYPOTHESES.md`](HYPOTHESES.md) before anything was measured, and every result is reported, losses included.

## In short

- **Backpressure is as easy with virtual threads as with Reactor, but you still have to bound the work.** Every
  bounded contestant held 16–17 elements in flight; the unbounded virtual-thread version let 19,845 pile up.
- **At 1,000 calls in flight, every model ties:** 94–98k ops/s against a ceiling of 100k.
- **From 10,000 calls in flight up, Lark's `mapAsync` beats Reactor.** It does 1.6–1.9× Reactor's throughput, at about
  the same CPU per call at 10k and less at 100k, with 60% of the heap per pending call.
- **A blocking body still costs a parked virtual thread.** `mapPar` and plain virtual threads hold about 1 KB per
  pending call and use 2–3× the CPU of Reactor or Pekko. For an async client, use `mapAsync`.
- **Lark's adaptive `Limit` protects a dependency that loses capacity.** p99 after the drop is 10 ms, against 27 ms
  for Reactor at its best fixed concurrency. It costs 13% of throughput while it ramps up.
- **Reactor still wins on CPU-only work.** It hands elements between threads 5.5× faster than Lark, and runs a
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
- **Each model's own timer.** Virtual threads `Thread.sleep`, Reactor uses `Mono.delay`, and Pekko and Lark's
  `mapAsync` use Pekko's scheduler. So Lark `mapAsync`'s numbers here are partly Pekko's scheduler.

[`Harness.kt`](src/main/kotlin/bvr/Harness.kt) measures:
- throughput, process CPU and allocation;
- the most elements ever outstanding, sampled every millisecond;
- live heap per pending call, from a full GC taken mid-run once at least 90% of the target concurrency is reached.

Medians over 2 JVM forks × 4 measured iterations, after 2 warm-up iterations. Every number is in
[`results/summary.md`](results/summary.md).

### Backpressure

![Most elements in flight, and throughput, with at most 16 calls allowed](docs/charts/backpressure.svg)

There's a fast producer and a slow consumer (1 ms calls), with at most 16 in flight. Every bounded contestant held
16–17 elements in flight, at the same throughput. The virtual-thread version with no bound held 19,845. Virtual
threads don't give you backpressure for free, but a `Semaphore` or a bounded window is all it takes.

### 1,000 calls in flight

![Throughput, CPU and memory with 1,000 calls in flight](docs/charts/fanout-1k.svg)

Every model reaches 94–98k ops/s against a ceiling of 100k. At typical service concurrency, throughput is no reason
to choose a model. Lark's `mapAsync` uses the least CPU per call and the least heap per pending call.

### 10,000 calls in flight

![Throughput, CPU and memory with 10,000 calls in flight](docs/charts/fanout-10k.svg)

- **Shared device.** Lark's `mapAsync` does 817k ops/s against Reactor's 522k, at the same CPU per call (2.62 vs
  2.57 µs). It holds 192 B per pending call against Reactor's 320 B.
- **Blocking bodies.** `mapPar` and plain virtual threads trail Reactor here, and hold 850–1,300 B per pending call.

### 100,000 calls in flight

![Throughput, CPU and memory with 100,000 calls in flight](docs/charts/inflight-100k.svg)

- **Shared device.** Lark's `mapAsync` does 859k ops/s against Reactor's 455k and Pekko's 480k. It uses the least CPU
  per call of any contestant (3.00 µs, against Reactor's 5.07), and peaks at 672 MB RSS against Reactor's 2,226 MB.
- **Reactor's heap.** Reactor never reached 90% of its 100,000-call target, so its live heap per call couldn't be
  measured.
- **Own timers.** Reactor's `flatMap` over `Mono.delay` collapsed to 11k ops/s. Thread dumps show the whole pipeline
  on one `parallel` worker inside `FluxFlatMap.drainLoop`, so treat that bar as Reactor at an extreme, not Reactor in
  general.

### A dependency that loses capacity

![p99 latency before and after a dependency's capacity drops, and each run's length](docs/charts/overload.svg)

The dependency serves 500 calls at once, 5 ms each. After 100,000 of the 200,000 calls, its capacity drops to 100, as
if a replica failed. Latency is measured per call, including the dependency's own queue, which is what a caller's
timeout sees.

- **Fixed concurrency.** Every fixed setting sees its p99 grow 4–5× when capacity drops: 54–59 ms at 1,000, and
  27 ms even at the best fixed number.
- **The adaptive limit.** `mapPar(Limit.adaptive(…))` settles at about 105 against the new capacity of 100, and holds
  p99 at 10 ms.
- **The cost.** It finishes 13% later, because it ramps up from a starting limit of 20.
- **Reactor.** It has nothing built in that adapts. Netflix's `concurrency-limits` library would close the gap.

### CPU-only work and hand-offs

![Milliseconds per million elements for a hand-off between threads and for a CPU-only chain](docs/charts/jmh.svg)

These are JMH runs, measured in ms per million elements.
- **Hand-off between threads.** Reactor's lock-free `publishOn` takes 25 ms. Lark's `buffer` takes 137 ms, a third
  of what a plain `ArrayBlockingQueue` between virtual threads takes (428 ms), but still 5.5× behind Reactor.
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
| H2 | At 1k and 10k in flight, virtual threads within ±15% of Reactor, using more CPU | **Mixed.** A tie at 1k. At 10k Lark's `mapAsync` is 1.6× Reactor at equal CPU; blocking bodies trail Reactor and use more CPU |
| H3 | At 100k in flight, Reactor needs less heap per pending call | **Refuted for `mapAsync`** (191 B vs about 340 B); **true for blocking bodies** (about 1,000 B) |
| H4 | On a CPU-only chain a plain loop wins, and Reactor beats Lark and Pekko | **Confirmed** |
| H5 | Reactor's `publishOn` beats a blocking queue between virtual threads | **Confirmed**, by 5.5× over Lark's `buffer` |
| H6 | Blocking code is shorter, and its trace names the caller | **Half refuted.** The traces are as predicted; the line counts are not |
| H7 | When a dependency loses capacity, an adaptive limit matches fixed concurrency on throughput and has a lower p99 | **Half met.** p99 2.7× lower; throughput 13% lower |

## Caveats

- One 4-core cloud VM. Treat differences under about 20% as noise.
- Fake I/O, not sockets.
- Neither the Reactor nor the Pekko code has been reviewed by someone who writes it for a living. Pull requests that
  make either faster are welcome, and the results will be re-run with them.
- Lark is built from its open pull requests
  ([#479–#487](https://github.com/matthewjones372/lark/pulls)), published locally.
- Versions: JDK 25.0.5 (Temurin), Kotlin 2.4.10, reactor-core 3.8.8, pekko-stream_2.13 1.2.1.

## Not yet tested

- **Real network I/O.** Reactor Netty against a virtual-thread server, driven by an open-model load generator at
  and beyond saturation.
- Kotlin coroutines and `Flow`.
- `mapAsync` under a `Limit`.

## Running it

```bash
# JDK 25 on the toolchain path. Build Lark from its pull requests first:
# in the lark checkout,
./gradlew :lark:publishToMavenLocal :lark-stream:publishToMavenLocal :lark-stream-forks:publishToMavenLocal \
    -Prelease.forceVersion=0.0.0-cand

# then here
export LARK_VERSION=0.0.0-cand-SNAPSHOT
scripts/run-jmh.sh                 # results/jmh.json
scripts/run-io.sh                  # results/io.txt
build/install/blocking-vs-reactive/bin/blocking-vs-reactive stacktraces > results/stacktraces.txt
python3 scripts/summarise.py > results/summary.md
python3 scripts/charts.py          # docs/charts/*.svg
```

## Licence

[Apache 2.0](LICENSE), as Lark is.
