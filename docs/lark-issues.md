# Lark behaviours found by this benchmark

Two behaviours of Lark (at the commit in `lark.commit`) that the semantics checks exposed. Neither affects a
throughput or memory number; both affect what a caller can rely on. Each has a minimal reproduction in
[`src/test/kotlin/bvr/LarkIssuesTest.kt`](../src/test/kotlin/bvr/LarkIssuesTest.kt) that uses nothing of the harness
and asserts what Lark does today, so the test fails if Lark changes. Lark itself was not modified.

## 1. An interrupted `await()` abandons a Forks run instead of stopping it

Filed as [lark#647](https://github.com/matthewjones372/lark/issues/647); still present on Lark main at `6f2c3f91`.

```kotlin
val caller = Thread.ofVirtual().start {
    try {
        Stream.from(1..100).mapPar(4) { body(it) }.runFold(0) { s, x -> s + x }.run(Forks()).await()
    } catch (_: InterruptedException) { }
}
// once the four bodies are blocked:
caller.interrupt(); caller.join()
// the four bodies are still blocked, and stay so
```

`lark` core's `Future.await()` is documented as "an interrupt cancels the future on the way out", and it does: it
calls `cancel(true)` on the run's exit future. But `PullRun.exit` in `Forks.kt` is a plain `CompletableFuture`
that the run completes when it ends; nothing observes its cancellation, so the run keeps pulling and its bodies keep
waiting. `docs/stream.md` says of `lark-stream-pekko`'s `awaitExit` that "an interrupt cancels the run rather than
abandoning it"; on `Forks`, the general-purpose `await()` does the opposite.

Stopping works through the handle: `start(Forks())` returns a `Running`, whose `close()` stops the run and
interrupts its bodies (the contrast test). So the remedy is in Lark's hands: either have a cancelled exit stop the
run, or give `Run` a blocking wait that closes the `Running` on interrupt.

In this benchmark it shows as `interrupted: run abandoned = yes` for every Lark contestant in
[`semantics.md`](semantics.md), next to `no` for plain and structured virtual threads and for Reactor's `block()`,
which cancels its subscription.

## 2. An ordered `mapAsync` or `mapPar` reports a failure only after every earlier element has answered

Filed as [lark#648](https://github.com/matthewjones372/lark/issues/648); still present on Lark main at `6f2c3f91`.

```kotlin
Stream.from(1..10)
    .mapAsync(4) { x -> if (x == 1) never else if (x == 2) failedFuture(IllegalStateException()) else completedFuture(x) }
    .runFold(0) { s, x -> s + x }.start(Forks())
// element 2 failed at once; half a second later the run is still waiting on element 1
```

The ordered window passes answers on in source order, and a failure is treated as an answer: it waits behind the
head. If the head never answers (no timeout on the call), the failure is never reported. Pekko's `mapAsync`, whose
name and shape Lark follows, fails the stream as soon as any stage fails, and so does Reactor's
`flatMapSequential` (the contrast test, and `failure surfaces while earlier calls held` in
[`semantics.md`](semantics.md)). Plain virtual threads taking futures in order (`vt-ordered`) behave like Lark, so
this is a property of ordering by waiting on the head, not of blocking; but Lark's documentation of `mapAsync` does
not say that a failure can wait behind an unanswered element.

The unordered variants (`mapAsyncUnordered`, `mapParUnordered`) report a failure at once.

## Not issues, but worth knowing

- After a stop, `mapAsyncUnordered` leaves a `whenComplete` callback on every stage still in flight, and does not
  cancel the stages; the ordered `mapAsync` registers no callbacks, so it leaves nothing. Pekko's `mapAsync` and
  `mapAsyncUnordered` also leave their callbacks, and Reactor cancels each future. Whether a stop should cancel the
  caller's futures is a design choice; the table in `semantics.md` reports what each does. Filed as
  [lark#649](https://github.com/matthewjones372/lark/issues/649).
- `mapPar(limit)` is ordered, like `mapPar(n)`. Against a dependency whose latency varies, an ordered window holds
  answered elements behind a slow head; the overload experiment uses `mapParUnordered(limit)` for that reason.
- A full window wakes its waiting feeder only once a quarter of the window, at most 64 places, has freed
  (`Ring.kt`, `Arrivals.kt` in `lark-stream-forks`; spec 0172), so up to that many places stand idle while the
  feeder sleeps. Over loopback sockets at 64 in flight this costs every Lark operator, ordered or not, 15–17%
  against the best contestant: 56–58 calls in flight of 64, against 63 for the others. Plain virtual threads made
  to refill the same way (`vt-batched`, in `results/net-diag`) lose 10% against plain virtual threads, with the same
  shortfall. At 256 and 1,000 in flight the cost is mostly within the noise, and on the synthetic devices, where
  each call is cheap to issue, it does not show. It is a trade (fewer wake-ups for the feeder), not a defect;
  a smaller batch for small windows would remove most of it. Filed as [lark#589](https://github.com/matthewjones372/lark/issues/589).
