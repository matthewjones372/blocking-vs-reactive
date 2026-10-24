# Blocking vs reactive on the JVM

An investigation of what blocking on virtual threads costs against composing asynchronous operations, and how
[Lark](https://github.com/matthewjones372/lark), Project Reactor and Pekko Streams compare at the same work. Every
number here comes from `results/`, produced by the commands under [Reproducing](#reproducing) on one 4-core
cloud VM; each table names the commit it ran at. How the experiments are built is in
[`docs/METHODOLOGY.md`](docs/METHODOLOGY.md); what was wrong with the earlier version of this benchmark, and what
changed, is in [`docs/AUDIT.md`](docs/AUDIT.md).

## 1. The questions

| | Question | Where it is answered |
|---|---|---|
| Q1 | Does Project Loom remove the need for asynchronous or reactive code? | Not directly measurable. Q2–Q4 and the semantics checks answer the parts that are. |
| Q2 | What does waiting by parking a virtual thread cost, against composing a future, for the same I/O? | I/O, workload, socket and memory suites; per-element JMH |
| Q3 | How do Lark, Reactor and Pekko compare at the same job? | The same suites, comparing within one model (async with async, ordered with ordered) |
| Q4 | How do fixed and adaptive concurrency limits behave when a dependency loses capacity? | The overload suite, a factorial of model × policy |

Lark is on both sides of Q2: `mapPar` parks a virtual thread per element, `mapAsync` composes a
`CompletionStage` and parks nothing per element. Lark `mapAsync` beating Reactor is a result about two
asynchronous implementations (Q3), not about blocking against reactive.

## 2. The models

| model | contestants | waits by | order |
|---|---|---|---|
| plain blocking | `vt-plain`, `vt-ordered` | a parked virtual thread per call, `Semaphore` bound | completion; start order |
| structured blocking | `vt-structured` | the same, inside a `StructuredTaskScope` (JDK 25 preview) | completion |
| blocking in a stream | `lark-mappar`, `lark-mappar-unordered` | a virtual thread per element in Lark's window | start order; completion |
| async composition | `cf-plain`, `lark-mapasync`, `lark-mapasync-unordered` | a callback on a `CompletableFuture`, or Lark's pull loop polling the head | completion; start order; completion |
| reactive streams | `reactor-flatmap`, `reactor-flatmapsequential`, `pekko-mapasync`, `pekko-mapasync-unordered` | `Mono.fromFuture` / Pekko's async callback, demand-driven | completion; start order; start order; completion |

All of them bound the calls in flight and stop reading their source at the bound. They differ in where the next
step runs (Reactor's drain runs on the completing thread, Pekko's on its stream actor, a blocked virtual thread on
its carrier), in whether demand crosses an asynchronous boundary (Reactive Streams) or is implicit in a loop that
does not call `next()`, and in what a stop does (section 7). Adaptive concurrency is a policy layered on any of
them, and is measured separately (Q4).

## 3. Experimental design

- **One contract for every contestant.** The same counted source, I/O call, bound and sink; every iteration's sum
  and count are checked, and a run with the wrong answer is excluded and listed. Code per contestant is between
  `region` markers in [`Contestants.kt`](src/main/kotlin/bvr/Contestants.kt).
- **Four synthetic devices**, because the device decides who wins at high concurrency (finding 1): the original
  two-thread `ScheduledThreadPoolExecutor` (`shared`); a timing wheel completing on its own thread (`wheel`); the
  same wheel handing completions to a separate pool (`dispatched`, the default); and each model's own timer
  (`native`). Every run records the device's lag against its deadlines.
- **Real sockets**: a separate-JVM NIO HTTP server pinned to one core; blocking-socket and async-NIO clients with
  the same protocol code, and the JDK `HttpClient`'s `send` and `sendAsync`.
- **Memory** at a held, steady state: exactly `n` calls pending and nothing moving, live bytes from a class
  histogram at two bounds, and the slope between them.
- **Overload**: capacity changes on a wall clock; latency per element from its arrival, phases by arrival time.
- **Statistics**: each cell in 3–5 fresh JVMs, run in an order shuffled afresh each round; a JVM fork is one sample;
  medians with bootstrap 95% intervals; a difference is reported only when its interval clears the table's
  measured noise floor (at least 5%). Otherwise the tables say *tie*.

## 4. Reproducing

```bash
export JAVA_HOME=/path/to/jdk-25            # Temurin 25.0.5 was used
scripts/build-lark.sh                       # Lark at lark.commit, into ~/.m2 as 0.0.0-cand-SNAPSHOT
./gradlew test                              # correctness and semantics: 175 tests, about two minutes
scripts/run-all.sh                          # every suite (about seven hours on 4 cores), then the summaries
python3 scripts/charts.py                   # docs/charts/*.svg from the summaries
```

Single suites: `scripts/run-suite.py <io|workload|memory|overload|net> --forks N`, then
`scripts/summarise.py <suite>`. `scripts/run-jmh.sh` runs the JMH benchmarks. CI compiles, runs the tests and
drives every harness at smoke size; it does not measure. Lark comes from a commit on its `main` branch
(`lark.commit`), because the overload experiment needs Lark's `Limit`, which no release has yet and which the one
published snapshot (`0.10.2-20261022.045955-6`) predates.

## 5. Main findings

Medians across forks, with the 95% interval where it matters. "Ceiling" is concurrency ÷ latency.

**1. The original 10k and 100k results measured the fake device, not the contestants.** The two-thread
`ScheduledThreadPoolExecutor` saturates near 550k calls/s, and runs contestants' callbacks on its own threads, so
the contestant that puts least work on the completing thread looked fastest. At 10,000 calls of 10 ms in flight
(ceiling 1M/s) on that device, Lark `mapAsync` led at 752k against Reactor `flatMap`'s 485k; on a device that keeps
up (`dispatched`), they are 953k and 935k, a tie, and every contestant is within 83–96% of the ceiling. Rankings at
10k correlate weakly across devices (Spearman ρ against `dispatched`: 0.76 `wheel`, 0.61 `shared`, −0.17 `native`).
→ [`results/io/summary.md`](results/io/summary.md), ![](docs/charts/c10k-devices.svg)

**2. At ordinary concurrency every model is limited by the I/O, not by itself.** 1,000 calls of 10 ms (ceiling
100k/s): all twelve bounded contestants reach 94–97.4k ops/s, every one a tie with the best. 10,000 calls of 100 ms,
calls failing 1 in 100 and recovered, and lognormal latency for unordered contestants: ties within 5%.

**3. Parking a virtual thread costs more CPU per call than composing a future, and it shows when completions are
frequent.** On the dispatched device at 10,000 in flight: 3.5–4.4 µs of CPU per call for the parked contestants
against 0.9–2.3 µs for the asynchronous ones; throughput 0.87–0.96 of the best against 0.98–1.0. Pekko
`mapAsyncUnordered` is the exception on the async side: 3.3 µs and 0.87, with 1,378 answers on average waiting for
its one stage. With 1 ms calls at 1,000 in flight (a completion
rate near 800k/s): 4.0–5.0 µs and 0.80–0.89 of the best for blocking, 1.1–3.6 µs and 0.89–1.0 for async. With
socket clients that do real work per call the gap disappears (finding 6).

**4. Retained memory per pending call: about 0.9–1.3 KB for a parked virtual thread, 80–200 B for a pending
future.** Held at 100,000 pending, the slope between 10k and 100k is 79 B for Lark's ordered `mapAsync`, 146–158 B
for `cf-plain`, Lark's unordered `mapAsync` and Pekko, 187–200 B for Pekko's ordered `mapAsync` and both Reactor
operators, and 904–1,273 B for every blocking contestant, about half of it the virtual thread's stack chunk. An
answered-but-unconsumed element behind a held head costs 60–141 B in the stream libraries and 353 B in
`vt-ordered`. → [`results/memory/summary.md`](results/memory/summary.md)

**5. At 100,000 in flight the machine runs out of CPU, and the most frugal async code wins.** On the dispatched
device (ceiling 10M/s): Lark `mapAsync` 3.83M ops/s at 0.73 µs/call, `cf-plain` 3.30M (a tie), Reactor `flatMap`
2.56M (0.67 [0.57–0.76] of the best), parked virtual threads 1.08–1.43M (0.28–0.37), Pekko 0.86–0.95M (0.22–0.25).
Here the device lags by up to 14 ms at p99 (381 ms for Pekko's ordered `mapAsync`): this is a CPU-saturation
result, which a machine with more cores would move.

**6. Over real sockets the client library matters more than the concurrency model, and Lark is the slowest.** With
the raw clients (same protocol code, blocking socket against `AsynchronousSocketChannel`), plain virtual threads,
`cf-plain`, Reactor and Pekko tie at 43.4–46.9k requests/s in every scenario; both Lark contestants are 9–19%
behind, resolved in five of six comparisons. With the JDK `HttpClient`, everyone spends 78–115 µs of CPU per
request in the client and reaches 21–31k/s; blocking `send` on virtual threads is the fastest in all three
scenarios and Lark `mapAsync` or Pekko the slowest. The server stayed under 0.8 of its core.
→ [`results/net/summary.md`](results/net/summary.md)

**7. Ordering costs far more than the choice of model when latency varies.** Lognormal latency (median 5 ms, 1,000
in flight): every unordered contestant reaches 109–113k ops/s, every ordered one 16.1–16.2k, blocking or not, Lark,
Reactor or Pekko, because the window waits behind its slowest head. → [`results/workload/summary.md`](results/workload/summary.md)

**8. Reactor's `flatMap` has a cliff when completions arrive one at a time on one thread.** Its drain visits every
inner subscriber each pass. With completions serialised on one thread (`wheel` device; `Mono.delay` at 100k in
flight) it falls to 16.5k and 11.5k ops/s at 62–90 µs of CPU per call. `flatMapSequential` does not, and with
completions spread over threads (`dispatched`) `flatMap` is in the leading group. The original README's 11k ops/s
"Reactor at an extreme" is this.

**9. A slow consumer favours models with one downstream.** A serial consumer at about 10 µs per element: Reactor,
Pekko and Lark streams reach 44.7–49.6k ops/s; `vt-ordered` 39k; `cf-plain`, `vt-plain` and `vt-structured`, whose
sink runs inside each call's task, 24–29k, because their threads convoy on the consumer's lock (and `cf-plain`'s
convoy forms on the device's dispatch threads, lagging it by 36 ms).

**10. CPU work per element does not separate the models cleanly.** A CPU step of about 30 µs on each answer at
1,000 in flight: Reactor `flatMapSequential` 62k ops/s (best), everyone else 42.6–56k, with Reactor `flatMap`,
`cf-plain`, Pekko, `vt-structured`, `vt-plain` and Lark `mapAsync` at 50–56k and the ordered blocking contestants and
Lark's unordered operators at 42.6–44.7k. Asynchronous contestants run the step on the four dispatch threads,
blocking ones on the four carriers.

**11. Adaptive concurrency moves the queue rather than removing it, and pays for it in throughput; its clear win
is against a dependency that rejects.** The overload factorial (dependency capacity 500, then 100; 5 ms service; 4
forks) gives the same result for a fixed bound whatever the model: plain virtual threads, Lark and Reactor are
within a few percent of each other in every pattern, so the policy is what matters.

- *Sudden loss, closed loop.* After the drop every policy serves 17.8–19.2k calls/s of a possible 20k. A fixed bound
  of 500 gives an end-to-end p99 of 27.5–29.5 ms; 1,000 gives 58–69 ms; Lark's adaptive `Limit` keeps the
  dependency's own p99 at 9–10 ms but the caller's end-to-end p99 at 173–247 ms, because elements now wait for a
  permit in the caller's window instead of in the dependency's queue. Before the drop the adaptive limit costs
  15–18% of throughput (78k against 92–95k), from ramping up from 20 (0.6–1.1 s to reach 90% of capacity).
- *Rejecting dependency* (rejects beyond 200 queued). A fixed bound of 1,000 collapses to near zero after the drop
  (1.2–1.4M rejections: the closed loop retries at once), 500 to 6–15k/s; every adaptive policy keeps 18.0–18.5k/s
  with 6–8 rejections in the whole run, because a rejected call backs the limit off.
- *Recovery.* When capacity returns, fixed bounds reclaim it within milliseconds; adaptive limits take 190–250 ms.
- *Open loop with a 50 ms deadline.* Arrivals at 60k/s exceed the degraded capacity threefold. Fixed bounds answer
  almost nothing within the deadline (about 400/s); adaptive policies 0.4–8.5k/s, with runs of the same algorithm
  differing by an order of magnitude (`vt-adaptive` 0.7k, `lark-adaptive` 8.5k, both Lark's `Limit`). Treat the
  *good/s* columns as sensitive: with the post-drop median latency close to the 50 ms threshold, small shifts move
  many calls across it.
- The ported limit Reactor needs (`AsyncLimit`, Lark's Vegas arithmetic with a non-blocking acquire) matches Lark's
  `Limit` on throughput and dependency latency in closed loop, but not exactly in its tail: `vt-adaptive-port` has
  a lower end-to-end p99 than `vt-adaptive` (173 against 247 ms after a sudden drop). Reactor with the port
  collapses in both open-loop patterns (94 calls/s): see open questions.
→ [`results/overload/summary.md`](results/overload/summary.md), ![](docs/charts/overload-sudden.svg)

**12. Operational behaviour differs in ways a benchmark score does not show.** From
[`docs/semantics.md`](docs/semantics.md): a blocking contestant's pending calls are threads in a thread dump (100
of 100 with the call site on the stack), an async one's are invisible (0 of 100). Reactor cancels every pending
future on `take`, on a failure and on an interrupt; blocking contestants interrupt their threads; Lark
`mapAsyncUnordered`, Pekko and `cf-plain` leave callbacks on calls still in flight. Interrupting a caller waiting on
a Lark run with Lark's `await()` returns control but leaves the run going ([`docs/lark-issues.md`](docs/lark-issues.md)).
Ordered Lark and `vt-ordered` report a failure only after earlier calls answer; Reactor and Pekko fail at once. A
blocking failure names the handler and the failed fetch in its stack trace; Lark `mapAsync`, `cf-plain` and Pekko
name neither; Reactor names the handler through `block()`, and the fetch only with `checkpoint()`.

**13. Without I/O, per-element machinery separates the models by an order of magnitude, and Reactor leads the
CPU-only cases.** JMH, five forks, 99.9% intervals in [`results/jmh/summary.md`](results/jmh/summary.md):

- *Per element with already-completed I/O* (100,000 elements, bound 64): a bare `CompletableFuture` loop 44 ns;
  Reactor and Pekko 124–140 ns; Lark `mapAsync` 227 ns, unordered 387 ns; blocking contestants, which still start a
  virtual thread per element, 906–1,483 ns, and `vt-ordered`, which parks the consumer on every future, 7.8 µs. This
  is what each model adds to whatever the I/O costs.
- *Hand-off between threads* (a million ints, buffer 256): Reactor `publishOn` 26 ms, Lark `buffer` 41 ms, an
  `ArrayBlockingQueue` between two virtual threads 227 ms, Pekko's async boundary 1.1 s.
- *CPU-only chain* (map, filter, map, sum over a million ints): a plain loop 0.66 ms, `Sequence` 9.7 ms, Reactor
  17.5 ms, Lark 20.6 ms, Pekko 87 ms.
→ ![](docs/charts/per-element.svg)

## 6. Important caveats

- **One 4-core VM, shared hardware, no frequency control.** The calibration loop drifted with a coefficient of
  variation of 4–6% across JVMs; differences under the tables' noise floors (5–11%) are reported as ties.
- **Warm-up.** Two warm-up iterations left JIT compilation in measured iterations at a median of about 60 ms; the
  last measured iteration of a fork was within 0.92–1.11 of the first for 90% of cells. The socket suite compiled
  more (90th percentile 0.58 s per measured iteration, mostly the JDK `HttpClient`), so its first iterations are
  slightly pessimistic.
- **Four cores flatter serial designs.** A single drain loop or stream actor competes with three other cores here,
  not thirty; the dispatch pool and the contestant also share those cores.
- **Synthetic I/O is a model.** Findings 1, 8 and the `native` rows show how much a result depends on which thread
  completes a call; the dispatched device is a choice, documented in the methodology, not neutral ground.
- **Loopback sockets** exercise the kernel and the clients, not a network.
- **Not covered**: Reactor Netty, Pekko HTTP, Kotlin coroutines, JDBC or any blocking client inside a reactive
  pipeline (`subscribeOn(boundedElastic())`), and machines with many cores.

## 7. Detailed results

| suite | summary | raw |
|---|---|---|
| I/O sweep: 1k, 10k, 100k in flight on four devices; backpressure | [`results/io/summary.md`](results/io/summary.md) | `results/io/*.jsonl` |
| workload matrix: short, long, CPU, lognormal, errors, slow consumer | [`results/workload/summary.md`](results/workload/summary.md) | `results/workload/` |
| sockets | [`results/net/summary.md`](results/net/summary.md) | `results/net/` |
| memory | [`results/memory/summary.md`](results/memory/summary.md) | `results/memory/` |
| overload | [`results/overload/summary.md`](results/overload/summary.md) | `results/overload/` |
| JMH | [`results/jmh/summary.md`](results/jmh/summary.md) | `results/jmh/jmh.json` |
| semantics, thread dumps, stack traces | [`docs/semantics.md`](docs/semantics.md) | `results/semantics/` |

Lines of code per contestant (`scripts/loc.py`, including the failure, stop and interrupt handling the semantics
tests require): the stream DSLs 7 each; `cf-plain` 32; `vt-plain` 35; `vt-ordered` 39; `vt-structured` 40.

## 8. Limitations and open questions

- Whether the CPU-per-call gap between parked threads and futures (finding 3) matters in a service depends on what
  else a call costs; with a real HTTP client (finding 6) it was lost in the client's own cost.
- The socket suite's Lark deficit (finding 6) is not explained yet; Lark's pull loop and window are the first
  suspects.
- Reactor with the ported adaptive limit collapses under open-loop load with a deadline: the limit falls to 3–9
  while the dependency is idle, so Vegas is reading inflated call times. Pulling the source on its own worker
  (`subscribeOn`) and letting arrivals take free permits ahead of older waiters, as Lark's `Limit` does, did not
  change it, and the process is not CPU-bound. The cause is not isolated; the row says nothing about Reactor
  itself, and Netflix's `concurrency-limits` or another production limiter would be the fairer Reactor contestant.
- Pekko's async hand-off (JMH) and its single-stage completion processing (finding 5) were not tuned beyond
  documented settings; a Pekko maintainer may know better.
- A 4-core result is not a 64-core result; the suites are ready to rerun on a larger machine.

## Licence

[Apache 2.0](LICENSE), as Lark is.
