# Methodology

How each experiment is built, what each number means, and where it stops meaning anything. The README gives the
results; this document is what a reviewer needs to check them.

## 1. The questions, kept apart

| | Question | Experiment | What it cannot answer |
|---|---|---|---|
| Q1 | Does Loom remove the *need* for asynchronous or reactive code? | None directly. Q2–Q4 and the semantics checks give evidence on the parts of the question that are measurable. | Team skills, ecosystem, existing code, libraries that only offer one style. |
| Q2 | What does waiting by parking a virtual thread cost, against composing a future, for the same I/O? | `io`, `workload`, `net`, `memory`, `PerElementBenchmark`: blocking contestants against async ones *with the library held fixed* where possible (plain JDK against plain JDK, Lark `mapPar` against Lark `mapAsync`). | Costs inside a real client library's own event loop, beyond the two client families in `net`. |
| Q3 | How do Lark, Reactor and Pekko compare at the same job? | The same suites, comparing libraries within one model (async against async, ordered against ordered). | Workloads the operators here do not represent (fan-in, windows, joins, hot sources). |
| Q4 | How do fixed and adaptive concurrency limits behave when a dependency loses capacity? | `overload`: a factorial of model × policy. | Policies other than Vegas; dependencies whose latency does not rise with load. |

Lark appears on both sides of Q2: `mapPar` blocks a virtual thread per element, `mapAsync` composes a
`CompletionStage` and parks nothing per element. A result in which Lark `mapAsync` beats Reactor is evidence about
two asynchronous implementations (Q3), not about blocking against reactive (Q2).

## 2. The models

| model | waits by | bounds concurrency with | order | who runs the next step |
|---|---|---|---|---|
| Plain blocking (`vt-plain`, `vt-ordered`) | parking a virtual thread on `Future.get` or a socket read | a `Semaphore` | completion order, or the caller taking futures in start order | the parked thread, once its carrier picks it up |
| Structured blocking (`vt-structured`) | the same | a `Semaphore`, inside a `StructuredTaskScope` (JDK 25 preview) | completion | the same; the scope owns every fork, cancels the rest on failure, and outlives none |
| Blocking in a stream (`lark-mappar*`) | the same, one virtual thread per element in a window | the window | ordered, or unordered | the same; Lark's pull loop takes answers from the window |
| Async composition (`cf-plain`, `lark-mapasync*`) | registering on a `CompletableFuture` (or, for ordered Lark, polling the head stage) | a `Semaphore` released by the callback, or Lark's window | completion, or ordered | the thread that completes the future (callbacks), or Lark's pull loop (ordered `mapAsync`) |
| Reactive streams (`reactor-*`, `pekko-*`) | `Mono.fromFuture` / Pekko's async callback | `flatMap`'s `maxConcurrency`, Pekko's `parallelism` | `flatMap`, `mapAsyncUnordered`: completion; `flatMapSequential`, `mapAsync`: source order | Reactor: the completing thread runs the drain; Pekko: the stream's actor on its dispatcher |
| Unbounded (`vt-naive`) | parking | nothing | completion | control only |

Where they overlap: every bounded contestant limits how many calls are in flight, and every one of them stops
reading its source while at the limit (`bound`, `slow consumer`, `paused consumer` in `semantics.md`). Where they
differ:

- **Demand is per element in Reactive Streams, and implicit elsewhere.** A Reactor or Pekko stage asks upstream for
  `n` more; a virtual-thread loop simply does not call `next()` until it has a permit. Both bound read-ahead; only
  the former carries demand across an asynchronous boundary to a producer that is not a loop the consumer controls.
- **Where the consumer runs.** In `vt-plain`, `vt-structured` and `cf-plain` the "sink" runs inside each call's
  task, so a slow sink holds back one task, not the source; the streams have one downstream, which a pause stops.
- **Stopping.** Interrupting a parked virtual thread stops it waiting; cancelling a future stops nothing unless the
  library cancels it and the client honours it. `semantics.md` reports what each actually does.

Adaptive concurrency control is a separate thing again: a policy for choosing the bound, which can wrap any of
these models. Q4 treats it that way.

## 3. Contestants and equal work

Every contestant is a function of one `Job` (`Contestants.kt`): the same counted source (`0 until n`), the same I/O
call (`Io`), the same bound, the same sink, and the same per-element extras when a workload asks for them (a CPU
step, recovery from a failed call, a per-call timeout). The CPU step and the recovery are composed only when used,
so no contestant pays for a stage that does nothing. Each contestant's code is between `// region` markers and is
what `scripts/loc.py` counts.

Checks that the work is equal:

- Every I/O iteration checks the sum of the answers and the count consumed against the expected values, and is
  marked invalid otherwise; the summary leaves invalid runs out and lists them.
- `ContestantSemanticsTest` checks every element is answered exactly once (and in order, for ordered contestants),
  that each contestant starts exactly its bound of calls when nothing answers, and never has more than its bound
  plus one between start and sink while flowing (the one being an answer that has left the operator and not yet
  reached the sink).
- Per-element latency and failure are functions of the element (`Workload.latencyNanos`, `Workload.fails`), so every
  contestant sees the same latency for the same element.

Choices made for every contestant alike:

- **Called from a virtual thread.** Each run is started on a fresh virtual thread, as a request handler on a
  virtual-thread server would start it. This matters for `vt-plain` and `cf-plain`, whose producer loop parks on a
  `Semaphore`: parking a platform thread costs more than parking a virtual one.
- **Reactor's inner prefetch is 1**, as its documentation advises for `Mono` inners; with the default of 32,
  `flatMapSequential` allocates a queue of 32 per inner.
- **Pekko waits with `get()`** on the materialised stage, and Lark with Lark's own `await()`, both interruptible.
- **Blocking calls use `Future.get`**, not `join`, so an interrupt can stop them (AUDIT A11).

What is not equal, and cannot be made so: Reactor's `flatMap` drain runs on whichever thread completes an inner;
Pekko processes completions on its stream actor; Lark's ordered `mapAsync` polls rather than registering a callback;
a virtual thread is resumed by its carrier pool. These are the models, and the device experiments (section 4) exist
to show how much results depend on them.

Variants tried and dropped (AUDIT A3): Reactor `parallel(4).flatMap(n/4)` and Pekko `Balance`/`Merge` over four
`mapAsyncUnordered` branches, both slower than the plain operator in exploratory runs.

## 4. The I/O devices

The synthetic device is the experiment's most consequential choice: its capacity caps every throughput, and its
threads run whatever contestants compose on its futures. Four are used, all in `Device.kt` / `Io.kt`:

| device | timer | completes futures on | purpose |
|---|---|---|---|
| `shared` | `ScheduledThreadPoolExecutor(2)` | its two timer threads | the original, kept for comparison; saturates near 550k calls/s |
| `wheel` | one-thread timing wheel, 100 µs ticks, lock-free submission | the wheel thread | the original's placement of continuations, without its capacity limit |
| `dispatched` | the same wheel | a separate pool of one platform thread per core, batches of up to 64 | the default: the timer's work per call is the same for every contestant |
| `native` | each model's own: `Thread.sleep`, `Mono.delay` on Reactor's parallel scheduler, Pekko's scheduler (1 ms ticks), a JDK `ScheduledExecutorService` with a thread per core for `CompletableFuture` and Lark `mapAsync` | the model's own timer threads | what each model does with its own primitives |

Every iteration records the device's lag: how late each call fired against its deadline. A healthy wheel fires
within a tick or two; a lag that grows means the device, or the CPU it shares, is the bottleneck, and the
summary prints it beside every throughput. `dispatched` also records how long a fired batch waited for a dispatch
thread.

Limits of the dispatched device: the dispatch pool competes with the contestant for the same four cores; batching
64 completions per task is a policy (it suits drain-loop designs, which see several completions per drain, and does
not hurt others measurably); and continuations still run on one kind of thread for every contestant, which is a
simplification of real clients, each of which has its own event-loop or poller design.

## 5. Real sockets (`net`)

`NetServer.kt` is a separate JVM serving HTTP/1.1 with keep-alive: `GET /?d=<µs>&s=<bytes>` is answered after `d`
microseconds with `s` bytes. No thread sleeps: each selector thread keeps its own queue of responses by due time,
and one timer thread wakes a selector when its earliest response is due. The server measures its own lag (response
written against due time) and CPU, and every client iteration reads them (`/__stats`, after `/__reset`), so each
result row says whether the server kept up. The server is pinned to one core and the client to the other three
(`taskset`), restarted every round, and runs one selector.

Clients, three families, so the library is held fixed while the waiting model changes, and the client a Reactor
service would actually use is present:

- `raw`: a blocking `java.net.Socket` client (virtual threads park on the JDK's poller) and an
  `AsynchronousSocketChannel` client (completion handlers on a pool of one thread per client core). Same request
  bytes and response parser; one connection per unit of concurrency, so the pool never queues.
- `jdk`: `java.net.http.HttpClient` (HTTP/1.1) with `send` for blocking contestants and `sendAsync` for the rest. Its
  idle-connection pool is unbounded and kept for ten minutes, set explicitly. `send` is `sendAsync(...).get()` inside,
  so a virtual thread in `send` is woken by the client's selector thread, not by the JDK's poller.
- `netty`: Reactor Netty's `HttpClient`, with a connection pool sized to the concurrency, run by the Reactor
  contestants only.

Scenarios: 64 in flight at 1 ms with 128 B (high rate, small calls), 1,000 in flight at 20 ms with 128 B (many
connections), 256 in flight at 5 ms with 16 KiB (bytes moved), and 10,000 in flight at 10 ms with 128 B (connection
count), each for every client family. Three more check the result's dependence on things outside the contestant:
one scenario against a second server (Netty, from main's network work: each delayed answer a task on its event
loop, several `SO_REUSEPORT` listeners), and two settings the JDK offers a virtual-thread service, platform poller
threads (`-Djdk.pollerMode=1`) and two scheduler carriers (`-Djdk.virtualThreadScheduler.parallelism=2`), for the
blocking contestants at 1,000 and 10,000 in flight. Pekko HTTP and Kotlin coroutines are not covered.

## 6. What is counted, and when

`Counters` tracks each element four times, with `LongAdder`s: taken from the source, handed to the I/O call,
answered by the device, consumed by the sink. Their differences are the queues: waiting for a permit, pending I/O,
answered-but-unconsumed. A platform thread samples them every millisecond, but only inside the steady window,
from the moment 10% of the elements have been consumed until 90% have, so ramp-up and drain are left out. From
the window:

- `steadyOpsPerSec`, the consumption rate inside it;
- mean and maximum of each queue;
- `impliedLatencyMs` = mean pending ÷ steady rate (Little's law): the mean time a call actually spent pending. Set
  against the configured latency plus the device's lag, it checks that the counters and the clock agree;
- RSS, sampled every 20 ms.

Per iteration: wall time, process CPU (all threads, so the device and the sampler too, equally for everyone),
allocated bytes (`ThreadMXBean.getTotalThreadAllocatedBytes`, which includes virtual threads' allocations on their
carriers and threads that have ended), GC pauses and concurrent GC time separately, and JIT compilation time
during the iteration (to show warm-up was enough: it should be near zero in measured iterations).

The instrumentation's cost is four increments per element and a 1 ms sampler; `-Dbvr.sampler=off` removes the
sampler for a check. It is the same code for every contestant.

## 7. Memory (`memory`)

Allocated bytes per element (section 6) and retained bytes per pending element are different quantities; only the
second is measured here.

- A `HeldDevice` answers nothing, so a contestant started with bound `n` and a source of `2n` settles with exactly
  `n` calls pending and nothing moving. The measurement waits until the device holds `n` calls and no counter has
  moved for 300 ms; if that does not happen within 60 s it is recorded as not settled.
- `GC.class_histogram` (via the diagnostic MBean) runs a full GC and lists live bytes by class. The baseline is
  taken the same way just before the run starts.
- This is done at bounds of 10,000 and 100,000 in the same JVM, three times, after an unmeasured round that loads
  and compiles everything. The slope `(live(100k) − live(10k)) / 90,000` is the retained cost per pending element:
  fixed costs (pools, operators, loaded classes) cancel. The largest growing classes are listed, so the figure can
  be traced to objects.
- `hol` mode holds only element 0 and answers the rest at once, so an ordered contestant fills its window with
  `n − 1` answered results behind it: the cost of buffered, completed elements.

What it includes: futures, callbacks, subscribers, window slots, virtual-thread objects and their stack chunks (a
parked virtual thread's frames live on the heap), the boxed elements, and the held device's own 8 bytes per call
(an array slot for the future and one for the element). What it leaves out: carrier and platform thread stacks
(native, and not growing with `n` here: NMT's `Thread` category is reported beside it), and anything that exists
only while elements flow, such as queues between operators under load. RSS growth at 100k is reported but is
dominated by heap pages touched, not by live objects.

The figures are per element in this state, on this JVM, with compressed oops; they are not a claim about a real
client, whose per-connection buffers would usually dominate.

## 8. Overload and adaptive concurrency (`overload`)

The dependency (`Overload.kt`) serves as many calls at once as its capacity allows, each in 5 ms, and queues the
rest FIFO; capacity follows wall-clock time. Full capacity is 500 (100k calls/s), degraded is 100 (20k/s).

Patterns:

| pattern | load | capacity | other |
|---|---|---|---|
| `sudden` | closed loop, 6 s | 500, then 100 from 3 s | |
| `gradual` | closed loop, 7 s | 500, falling linearly to 100 between 2 and 5 s | |
| `recovery` | closed loop, 7 s | 500, 100 from 2 to 4.5 s, then 500 | |
| `rejecting` | closed loop, 6 s | as `sudden` | the dependency rejects a call when 200 are already queued |
| `open-50ms` | open loop, 60k arrivals/s, 6 s | as `sudden` | a 50 ms deadline, enforced before the permit and again inside it |
| `open-500ms` | the same | as `sudden` | a 500 ms deadline |

In closed loop an element arrives when it is pulled; in open loop element `i` arrives at `i / rate` whether or not
the contestant is ready, and its latency counts from then (so time spent waiting in the caller counts, and
coordinated omission does not hide it). Every element records arrival, outcome (answered, rejected, expired before
the call), end-to-end latency, and time inside the dependency. Phases are cut by arrival time. A 10 ms series of
capacity, dependency queue, in-flight, client backlog and the adaptive limit gives two recovery figures per change:
*relief* (how long until the dependency's queue stays at or under half its capacity) and *reclaim* (how long until
it serves 90% of capacity for 200 ms).

The factorial:

| | fixed 1,000 | fixed 500 | fixed 100 | adaptive (Lark `Limit`) | adaptive (port) |
|---|---|---|---|---|---|
| plain virtual threads | ✓ | ✓ | ✓ | ✓ | ✓ |
| Lark `mapParUnordered(limit)` | ✓ | ✓ | | ✓ | |
| Reactor `flatMap` | ✓ | ✓ | | | ✓ |

Adaptive is TCP Vegas as Lark implements it (initial 20, max 1,000). Lark's `Vegas` is internal, and its `Limit`
only offers a blocking `withPermit`, so `AsyncLimit` ports the same arithmetic under the same lock and adds a
non-blocking acquire for Reactor. Running plain virtual threads on both is the check that the port behaves as the
original. Lark's `mapParUnordered(limit)` is written out as Lark defines it (`mapParUnordered(limit.max) {
limit.withPermit { … } }`) so an expired element can be caught outside the permit rather than fail the stream.
Under every adaptive policy the outer bound is 1,000, as Lark's window is `limit.max`. As in Lark's `Limit`, a
caller that finds a free place takes it even if others are waiting. Reactor's open-loop source, which parks until
each arrival is due, is pulled on a `boundedElastic` worker (`subscribeOn`), as Reactor's documentation does for a
blocking source.

## 9. Execution and statistics

`scripts/run-suite.py` runs every cell of a suite once per round in a fresh JVM, in an order shuffled afresh each
round (seeded, recorded), for 5 rounds (I/O, workload), 4 (overload) or 3 (sockets, memory). Before measuring it
stops Gradle's and Kotlin's daemons. Each JVM has a fixed, pre-touched heap; I/O cells run 2 warm-up and 3
measured iterations, each of a few hundred thousand to a million elements, with a full GC before each measured
iteration. A single-threaded calibration loop at the start of every JVM records how much the machine drifted.
Everything the summary needs to reproduce a number is in `results/<suite>/`: `results.jsonl` (every measurement,
with commit, Lark commit, JDK, JVM options, fork and order), `runs.jsonl` (start, end, exit, load average, stderr
tail of each JVM), and `metadata.json` (machine, kernel, CPU, seed, schedule).

`scripts/summarise.py` treats a fork as one sample (the median of its measured iterations), and reports the median
across forks, their range, and a bootstrap 95% interval; ratios to the best contestant carry their own interval
(forks of each resampled independently). A ratio is called a difference only when its interval excludes 1 by more
than the table's noise floor, the larger of 5% and twice the median coefficient of variation across forks in that
table; otherwise the table says *tie*. It refuses to summarise a suite whose records disagree on commit, JDK, CPU
count or JVM options.

JMH (`scripts/run-jmh.sh`) is used for CPU-bound microbenchmarks only: the CPU-only chain, the thread hand-off, and
the per-element machinery of every contestant against already-completed I/O. Each has five forks; warm-up and
measurement are set per class (10 × 2 s for the two whose operation takes up to a second). The end-to-end I/O tests
are not forced into JMH: they need devices, steady-state windows and per-element accounting that JMH does not
provide, and their iterations are seconds long.

## 10. The environment, and what it limits

All results come from one cloud VM: 4 vCPUs (Intel Xeon @ 2.1 GHz), 15 GiB, Linux 6.18, no CPU frequency control
(`scaling_governor` is not exposed), shared physical hardware. JDK 25.0.5 (Temurin), Kotlin 2.4.10, reactor-core
3.8.8, pekko-stream 1.2.1, Lark at `lark.commit`.

- Neighbours and frequency are not controlled; the randomised order and the calibration loop make drift visible and
  spread it across contestants, but do not remove it.
- Four cores is small. Contestants whose costs are serial (a single drain loop, a single stream actor) look better
  relative to parallel ones than they would on 32 cores, and the dispatch pool and the contestant share the same
  cores.
- The synthetic suites are not pinned; the socket suite pins server and client to separate cores, which leaves the
  client three.
- Loopback sockets have no network latency, loss or bandwidth limit; they exercise the kernel's socket path and the
  clients' code, not a network.

## 11. Semantics and operational checks

`Semantics.kt` runs each check against every contestant with held devices, so the checks are deterministic: answers
once and in order; calls started with nothing answering; most in flight; `take(10)` with 22 calls held; a failure
while 31 calls are held; per-call timeouts with 10% of calls never answering; a 2 ms-per-element consumer; a consumer
that pauses for 400 ms; a source that yields every 5 ms; and an interrupt of the caller while every call is held.
After a stop, *still awaited* counts held calls that are not done but have a thread parked on them or a callback
registered: calls the run abandoned rather than let go of. `ContestantSemanticsTest` asserts the observed behaviour
of each contestant with a stated reason per difference; `docs/semantics.md` is the table.

`StackTraces.kt` fails the third of ten order fetches and prints the trace each model hands the caller, with Reactor
also shown with `checkpoint()` and with `Hooks.onOperatorDebug()`. `threaddump` holds 100 calls in each contestant and
counts how many threads in a JSON thread dump (`HotSpotDiagnosticMXBean.dumpThreads`) have the contestant's call site
on their stack.
