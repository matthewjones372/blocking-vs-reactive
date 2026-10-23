# Hypotheses, written before any benchmark ran

This file was committed before the first measurement. The results in
`README.md` are judged against it, including the predictions that go against
the author's own bias (the author prefers blocking code on virtual threads).

## The claim under test

> With virtual threads, blocking is cheap, so backpressure is a bounded queue
> or a semaphore. Reactive Streams' demand protocol (`request(n)`) exists to
> recreate that in a world where you may not block. Blocking code on virtual
> threads should match reactive libraries on I/O-bound work, while being
> simpler to write.

The stronger claim, "virtual threads beat reactive on performance
everywhere", is **not** expected to hold, and the scenarios below are chosen
so it can be proven false.

## Contestants

| Name | What it is |
|---|---|
| `vt-plain` | JDK only: virtual threads, `Semaphore`, `ArrayBlockingQueue` |
| `vt-naive` | a negative control: one virtual thread per element and no bound |
| `lark` | `lark-stream` on its `Forks()` backend (virtual threads) |
| `reactor` | Project Reactor `reactor-core`, idiomatic, with tuned prefetch |
| `pekko` | Pekko Streams `javadsl`, idiomatic, with default settings |

Every contestant uses the same fake I/O device: a `CompletableFuture`
completed by a shared `ScheduledThreadPoolExecutor`. Reactive contestants
compose the future without blocking. Virtual-thread contestants call `join()`
and park. So the device costs the same for everyone, and only the concurrency
model differs.

## Predictions

| # | Scenario | Prediction |
|---|---|---|
| H1 | **Backpressure**: an unbounded fast producer, and a slow I/O consumer with concurrency 16 | Every bounded contestant keeps the outstanding elements below a few hundred, and its heap stays flat. `vt-naive` does not, and that shows backpressure is a design choice in both worlds, not a gift of either. Throughput within 10% across the bounded contestants. |
| H2 | **I/O fan-out**: 10 ms calls at concurrency 1k and 10k | `vt-plain` and `reactor` within ±15% on throughput. Virtual threads use **more** CPU per operation than Reactor (unpark and remount against a callback that runs on the timer thread). |
| H3 | **100k in flight**: 10 ms calls at concurrency 100k | Reactor needs less live heap per pending operation than a parked virtual thread. **Reactor wins on memory.** |
| H4 | **CPU-only operator chain**: map, filter, scan over 10M ints | A plain Kotlin loop or `Sequence` wins by a wide margin. Among streaming libraries, Reactor's fusion beats `lark` and `pekko`. **Reactor wins** among the libraries. |
| H5 | **Hand-off across a thread boundary**: producer on one thread, consumer on another | Reactor's lock-free `publishOn` queue beats `ArrayBlockingQueue` between virtual threads. **Reactor wins**, perhaps by several times. |
| H6 | **Code**: lines per scenario, and the stack trace when the I/O call fails | Blocking code is shorter, and its stack trace names the calling function. Reactor's trace names operators, not the caller, unless checkpoints or `Hooks.onOperatorDebug()` are on. |

## What would change the author's mind

- H1 failing for `vt-plain` or `lark` (unbounded growth) would refute the
  "backpressure is easy" half of the claim.
- `reactor` beating `vt-plain` by more than 30% on throughput in H2 would
  refute "blocking matches reactive on I/O-bound work".
- Throughput in H2 is the result that matters most. H3–H5 are expected
  losses, and they are reported whatever they show.
