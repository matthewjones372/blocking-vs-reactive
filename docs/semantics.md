# Semantics and operational behaviour

What each contestant does beyond producing its answer. Every row comes from `Semantics.kt`, run with held devices
so the outcome does not depend on timing, and `ContestantSemanticsTest` asserts each one, with the reason for every
difference between models written next to its expectation. The tables below are generated from
`results/semantics/` by `scripts/summarise.py semantics`.

These are observations, not scores. Some differences are design choices a library documents (Pekko streams are not
tied to the thread that waits for them; ordered windows report in order); others are worth knowing before relying
on a behaviour (`docs/lark-issues.md`).

## How to read the columns

- **calls started, nothing answering**: with every call held, how many the contestant starts. All hold their bound.
- **take(10): returns**: 32 calls held, then elements 0–9 answered: does the run return while 22 are still held?
- **calls still awaited**: after the run has returned, held calls that are not done and still have a parked thread
  or a registered callback on them. Zero means the contestant let go of them (interrupted its threads, or
  cancelled the futures); more than zero means it abandoned them to complete later.
- **failure surfaces while earlier calls held**: element 5 fails while 0–4 are held. Ordered windows that wait on
  their head report it only after 0–4 answer.
- **timed-out calls let go**: every tenth call never answers; with a 50 ms per-call timeout and recovery, the run
  completes and every held call has been cancelled or completed by its timeout.
- **slow consumer: most read ahead**: the furthest the source got ahead of a 2 ms-per-element consumer (bound 32).
- **paused consumer: source reads**: elements read from the source between 100 and 380 ms into a 400 ms pause of
  the consumer. Contestants whose sink runs inside each call's own task (`vt-plain`, `vt-structured`, `cf-plain`)
  have no single consumer to pause, and keep going.
- **caller interrupted**: the thread waiting for the run is interrupted while every call is held. *Returns*: it gets
  control back within 2 s. *Run abandoned*: the run's calls are still awaited afterwards.

## Results

| contestant | calls started, nothing answering | take(10): returns | take: calls still awaited | failure surfaces while earlier calls held | failure: calls still awaited | timed-out calls let go | slow consumer: most read ahead | paused consumer: source reads | caller interrupted: returns | interrupted: run abandoned |
|---|--:|---|--:|---|--:|---|--:|--:|---|---|
| vt-plain | 32 of 32 | yes | 0 | yes | 0 | yes | 33 | 0 | yes | no |
| vt-ordered | 32 of 32 | yes | 0 | no | 31 | yes | 34 | 0 | yes | no |
| vt-structured | 32 of 32 | yes | 0 | yes | 0 | yes | 33 | 0 | yes | no |
| lark-mappar | 32 of 32 | yes | 0 | no | 31 | yes | 33 | 0 | yes | yes |
| lark-mappar-unordered | 32 of 32 | yes | 0 | yes | 0 | yes | 33 | 0 | yes | yes |
| cf-plain | 32 of 32 | no | 23 | no | 31 | yes | 33 | 2032 | yes | yes |
| lark-mapasync | 32 of 32 | yes | 0 | no | 1 | yes | 33 | 0 | yes | yes |
| lark-mapasync-unordered | 32 of 32 | yes | 32 | yes | 31 | yes | 33 | 0 | yes | yes |
| reactor-flatmap | 32 of 32 | yes | 0 | yes | 0 | yes | 33 | 0 | yes | no |
| reactor-flatmapsequential | 32 of 32 | yes | 0 | yes | 0 | yes | 33 | 0 | yes | no |
| pekko-mapasync | 32 of 32 | yes | 32 | yes | 31 | yes | 33 | 0 | yes | yes |
| pekko-mapasync-unordered | 32 of 32 | yes | 32 | yes | 31 | yes | 33 | 0 | yes | yes |

### Thread dump with 100 calls pending

| contestant | model | threads in dump | threads at the call site |
|---|---|--:|--:|
| vt-plain | blocking | 113 | 100 |
| vt-ordered | blocking | 114 | 100 |
| vt-structured | structured | 113 | 100 |
| lark-mappar | blocking | 115 | 100 |
| lark-mappar-unordered | blocking | 115 | 100 |
| cf-plain | async | 13 | 0 |
| lark-mapasync | async | 15 | 0 |
| lark-mapasync-unordered | async | 15 | 0 |
| reactor-flatmap | reactive | 13 | 0 |
| reactor-flatmapsequential | reactive | 13 | 0 |
| pekko-mapasync | reactive | 21 | 0 |
| pekko-mapasync-unordered | reactive | 22 | 0 |

### Does the JMH hand-off cross threads?

| contestant | elements | produced on the consumer's thread | fraction |
|---|--:|--:|--:|
| reactor publishOn | 1,000,000 | 999,999 | 1.00 |
| lark buffer | 1,000,000 | 0 | 0.00 |
| pekko async boundary | 1,000,000 | 326,650 | 0.33 |
| virtual threads + ArrayBlockingQueue | 1,000,000 | 0 | 0.00 |

### Stack trace of a failed fetch, as the handler catches it

| contestant | exception caught | names `handleRequest` | names `fetchOrder` | lines |
|---|---|---|---|--:|
| vt-plain | `ExecutionException` | yes | yes | 15 |
| vt-structured | `IOException` | yes | yes | 6 |
| lark-mappar | `IOException` | yes | yes | 13 |
| lark-mapasync | `IoFailure` | no | no | 12 |
| cf-plain | `IoFailure` | no | no | 11 |
| reactor-flatmap | `IoFailure` | yes | no | 19 |
| reactor-flatmap + checkpoint() | `IoFailure` | yes | yes | 25 |
| reactor-flatmap + Hooks.onOperatorDebug() | `IoFailure` | yes | no | 29 |
| pekko-mapasync-unordered | `IoFailure` | no | no | 11 |

## What the differences mean

- **Letting go of calls.** Reactor cancels every pending future on `take`, on a failure and on an interrupt
  (`Mono.fromFuture` cancels the future it was given). Blocking contestants interrupt their threads, which stops
  them waiting; the future itself stays pending at the device, as an interrupted socket read would close its socket.
  Lark's unordered `mapAsync`, Pekko and `cf-plain` leave a callback on every call still in flight: nothing waits
  for it, but it runs when the call completes. Lark's ordered `mapAsync` registers no callbacks, so it leaves none.
- **Failures in ordered windows.** `vt-ordered`, `lark-mappar` and `lark-mapasync` report a failure only once every
  earlier element has answered; Reactor's `flatMapSequential` and Pekko's `mapAsync` fail at once. With no per-call
  timeout, a head that never answers hides the failure for ever (`docs/lark-issues.md`, issue 2).
- **Interrupts.** Every caller gets control back. Plain and structured virtual threads stop their calls, and so does
  Reactor's `block()`. A Lark run awaited with Lark's `await()`, and a Pekko stream awaited with `get()`, keep going
  after the caller has left; Pekko documents kill switches for this, and Lark's `Running.close()` stops a run
  (`docs/lark-issues.md`, issue 1). `cf-plain` has nothing to cancel.
- **`cf-plain` waits for its calls** on `take` and on failure, because nothing in it keeps the futures; it is the
  baseline for what a library adds, not a recommendation.
- **Observability.** A thread dump shows every pending blocking call as a thread with the call site on its stack, and
  no pending asynchronous call at all. Stack traces follow the same split: a blocking failure names the handler and
  the failed call; an asynchronous one carries the stack of the thread that completed the future, so Lark
  `mapAsync`, `cf-plain` and Pekko name neither. Reactor names the handler through `block()`, and the failing call
  only with `checkpoint()`; `Hooks.onOperatorDebug()` adds the assembly site (here a lambda in the handler) but
  not `fetchOrder`. Lark `mapAsync` does attach a suppressed `Defect` to the exception naming the operator, the
  element and the line that built the stage (`mapAsync died on 3, built at StackTraces.kt:89`); what it lacks is the
  waiting caller's own frames, which Reactor's `block()` adds
  ([lark#654](https://github.com/matthewjones372/lark/issues/654)).
- **Code.** The stream libraries express each contestant in seven lines; the plain JDK versions take 32–40 once
  they handle failure, stop and interrupt as the tests require (`scripts/loc.py`). Which is easier to read is a
  judgement this document does not make.

The full traces are in [`results/semantics/stacktraces.txt`](../results/semantics/stacktraces.txt).
