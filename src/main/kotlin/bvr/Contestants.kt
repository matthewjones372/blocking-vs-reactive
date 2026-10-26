package bvr

import com.typesafe.config.ConfigFactory
import io.github.matthewjones372.lark.await
import io.github.matthewjones372.lark.stream.Exit
import io.github.matthewjones372.lark.stream.Forks
import io.github.matthewjones372.lark.stream.Stream
import io.github.matthewjones372.lark.stream.from
import io.github.matthewjones372.lark.stream.mapAsync
import io.github.matthewjones372.lark.stream.mapAsyncUnordered
import io.github.matthewjones372.lark.stream.mapPar
import io.github.matthewjones372.lark.stream.mapParUnordered
import io.github.matthewjones372.lark.stream.run
import io.github.matthewjones372.lark.stream.runFold
import io.github.matthewjones372.lark.stream.take
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.javadsl.Sink
import org.apache.pekko.stream.javadsl.Source
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.Duration
import java.util.concurrent.Callable
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionException
import java.util.concurrent.CompletionStage
import java.util.concurrent.ExecutionException
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.Semaphore
import java.util.concurrent.StructuredTaskScope
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

/**
 * One run of a contestant: every element of [source] goes through one I/O call, at most [concurrency] elements
 * between being started and being consumed, and each answer goes to [sink].
 *
 * - [sink] is thread-safe. The unordered virtual-thread and `CompletableFuture` contestants call it from many
 *   threads at once; the stream libraries call it from one thread at a time.
 * - With [recover], a failed call answers 0 and the run goes on; without it the run fails with the call's
 *   [IoFailure], and should stop starting work.
 * - After [take] answers the run stops: it pulls nothing more from [source] and lets go of what is in flight, in
 *   whatever way its model offers.
 * - With [timeoutMillis], a call that has not answered in time fails with [IoTimeout] (recoverable as any
 *   [IoFailure]) using each model's own timeout: `get(timeout)`, `orTimeout`, `Mono.timeout`.
 */
class Job(
    val source: Iterator<Int>,
    val concurrency: Int,
    val io: Io,
    val recover: Boolean = false,
    val take: Long = Long.MAX_VALUE,
    val timeoutMillis: Long? = null,
    val sink: (Int) -> Unit,
)

/** The model a contestant represents; README.md explains each. */
enum class Model { BLOCKING, STRUCTURED, ASYNC, REACTIVE, UNBOUNDED }

class Contestant(
    val name: String,
    val model: Model,
    val ordered: Boolean,
    val library: String,
    val run: (Job) -> Unit,
)

// The per-element call, in each model's shape: I/O, then the workload's CPU step, then recovery if asked for.
// The CPU step and the recovery are only composed when the workload uses them, so no contestant pays for a stage
// that does nothing.

fun Job.callBlocking(x: Int): Int = try {
    val y = if (timeoutMillis == null) io.blocking(x) else io.blockingWithin(x, timeoutMillis * 1_000_000)
    if (io.workload.cpuIters == 0) y else io.workload.transform(y)
} catch (failed: IoFailure) {
    if (recover) 0 else throw failed
}

/** A future's own timeout, as [IoTimeout]; the future itself is completed exceptionally, which lets the device go. */
private fun CompletableFuture<Int>.within(x: Int, millis: Long?): CompletableFuture<Int> =
    if (millis == null) this else orTimeout(millis, TimeUnit.MILLISECONDS).exceptionally { e ->
        throw if (e.unwrap() is TimeoutException) IoTimeout(x) else e.unwrap()
    }

fun Job.callAsync(x: Int): CompletableFuture<Int> {
    var f = io.async(x).within(x, timeoutMillis)
    if (io.workload.cpuIters > 0) f = f.thenApply(io.workload::transform)
    if (recover) f = f.exceptionally { e -> if (e.unwrap() is IoFailure) 0 else throw e.unwrap() }
    return f
}

fun Job.callMono(x: Int): Mono<Int> {
    var m = io.mono(x)
    if (timeoutMillis != null) {
        m = m.timeout(Duration.ofMillis(timeoutMillis)).onErrorMap(TimeoutException::class.java) { IoTimeout(x) }
    }
    if (io.workload.cpuIters > 0) m = m.map(io.workload::transform)
    if (recover) m = m.onErrorReturn(IoFailure::class.java, 0)
    return m
}

fun Job.callPekko(x: Int): CompletionStage<Int> {
    var f = io.pekko(x).toCompletableFuture().within(x, timeoutMillis)
    if (io.workload.cpuIters > 0) f = f.thenApply(io.workload::transform)
    if (recover) f = f.exceptionally { e -> if (e.unwrap() is IoFailure) 0 else throw e.unwrap() }
    return f
}

fun Throwable.unwrap(): Throwable =
    if ((this is CompletionException || this is ExecutionException) && cause != null) cause!!.unwrap() else this

private fun <R : Any> Exit<*, R>.orThrow(): R = when (this) {
    is Exit.Done -> value
    is Exit.Died -> throw cause
    is Exit.Failed -> error("declared failure: $error")
}

// region vt-plain
/** A virtual thread per element, a [Semaphore] for the bound; answers reach the sink in completion order. */
fun vtPlain(job: Job) {
    val permits = Semaphore(job.concurrency)
    val failure = AtomicReference<Throwable>()
    val taken = AtomicLong()
    val stop = AtomicBoolean()
    Executors.newVirtualThreadPerTaskExecutor().use { threads ->
        for (x in job.source) {
            try {
                permits.acquire()
            } catch (interrupted: InterruptedException) {
                // The caller was interrupted: stop every call, or close() would wait for them all.
                threads.shutdownNow()
                throw interrupted
            }
            if (stop.get()) break
            try {
                threads.submit {
                    try {
                        val y = job.callBlocking(x)
                        val n = taken.incrementAndGet()
                        if (n <= job.take) job.sink(y)
                        if (n >= job.take && stop.compareAndSet(false, true)) threads.shutdownNow()
                    } catch (_: InterruptedException) {
                        // Let go of by shutdownNow.
                    } catch (failed: Throwable) {
                        if (failure.compareAndSet(null, failed) && stop.compareAndSet(false, true)) threads.shutdownNow()
                    } finally {
                        permits.release()
                    }
                }
            } catch (_: RejectedExecutionException) {
                break
            }
        }
    }
    failure.get()?.let { throw it }
}
// endregion

// region vt-batched (diagnostic)
/**
 * [vtPlain], but a finished call's permit is handed back only once a batch of them has finished: a quarter of the
 * bound, at most 64, a power of two, as Surefold's window wakes its parked feeder (`Ring`, `Arrivals`). Not a contestant:
 * it isolates what refilling in batches costs over a real socket, apart from everything else Surefold does.
 */
fun vtBatched(job: Job) {
    val batch = Integer.highestOneBit(maxOf(1, minOf(job.concurrency / 4, 64)))
    val permits = Semaphore(job.concurrency)
    val finished = AtomicLong()
    val failure = AtomicReference<Throwable>()
    val taken = AtomicLong()
    val stop = AtomicBoolean()
    Executors.newVirtualThreadPerTaskExecutor().use { threads ->
        for (x in job.source) {
            try {
                permits.acquire()
            } catch (interrupted: InterruptedException) {
                threads.shutdownNow()
                throw interrupted
            }
            if (stop.get()) break
            try {
                threads.submit {
                    try {
                        val y = job.callBlocking(x)
                        val n = taken.incrementAndGet()
                        if (n <= job.take) job.sink(y)
                        if (n >= job.take && stop.compareAndSet(false, true)) threads.shutdownNow()
                    } catch (_: InterruptedException) {
                        // Let go of by shutdownNow.
                    } catch (failed: Throwable) {
                        if (failure.compareAndSet(null, failed) && stop.compareAndSet(false, true)) threads.shutdownNow()
                    } finally {
                        // Once stopped, one permit each, so the caller wakes to see it however few calls are left.
                        if (stop.get()) permits.release() else if (finished.incrementAndGet() % batch == 0L) permits.release(batch)
                    }
                }
            } catch (_: RejectedExecutionException) {
                break
            }
        }
    }
    failure.get()?.let { throw it }
}
// endregion

// region vt-ordered
/** A virtual thread per element; the caller takes the futures in the order they were started. */
fun vtOrdered(job: Job) {
    val permits = Semaphore(job.concurrency)
    val started = LinkedBlockingQueue<Future<Int>>()
    val end = CompletableFuture.completedFuture(-1)
    Executors.newVirtualThreadPerTaskExecutor().use { threads ->
        val producer = threads.submit {
            try {
                for (x in job.source) {
                    permits.acquire()
                    started.put(threads.submit(Callable { job.callBlocking(x) }))
                }
            } finally {
                started.put(end)
            }
        }
        fun cancelAll() {
            producer.cancel(true)
            started.forEach { it.cancel(true) }
            threads.shutdownNow()
        }
        try {
            var taken = 0L
            while (taken < job.take) {
                // An interrupt here, or in get() below, ends the loop; the finally stops everything started.
                val next = started.take()
                if (next === end) break
                val y = try {
                    next.get()
                } catch (failed: ExecutionException) {
                    throw failed.cause!!
                }
                permits.release()
                job.sink(y)
                taken++
            }
        } finally {
            cancelAll()
        }
    }
}
// endregion

// region vt-structured
/**
 * As [vtPlain], inside a `StructuredTaskScope` (JDK 25 preview): the scope owns every fork, a failure cancels the
 * rest, and the scope does not close until every fork has finished.
 */
fun vtStructured(job: Job) {
    val permits = Semaphore(job.concurrency)
    val taken = AtomicLong()
    val joiner = StopOnFailureOrTake()
    StructuredTaskScope.open(joiner).use { scope ->
        for (x in job.source) {
            // A fork into a cancelled scope never runs, so its permit would not come back; the check after acquire
            // stops the loop, having lost at most the one permit taken before the stop was seen.
            permits.acquire()
            if (joiner.stopped) break
            scope.fork(Callable {
                try {
                    val y = job.callBlocking(x)
                    val n = taken.incrementAndGet()
                    if (n <= job.take) job.sink(y)
                    if (n >= job.take) joiner.stopped = true
                } finally {
                    permits.release()
                }
            })
        }
        try {
            scope.join()
        } catch (failed: StructuredTaskScope.FailedException) {
            throw failed.cause!!
        }
    }
}

/**
 * A permit, or false once [stopped]: after a scope is cancelled, a fork never runs, so a permit taken for it would
 * never come back, and a plain `acquire` could wait for ever on permits held by forks that will not run. Used by
 * the stack-trace demo, where a failure can come before every fork has been made.
 */
fun Semaphore.acquireUnless(stopped: () -> Boolean): Boolean {
    while (!tryAcquire(1, java.util.concurrent.TimeUnit.MILLISECONDS)) if (stopped()) return false
    if (stopped()) { release(); return false }
    return true
}

/** Cancels the scope on the first failure, which [result] then throws, or once the take is reached. */
class StopOnFailureOrTake : StructuredTaskScope.Joiner<Unit, Unit> {
    @Volatile var stopped = false
    @Volatile private var failure: Throwable? = null

    override fun onComplete(subtask: StructuredTaskScope.Subtask<out Unit>): Boolean {
        if (subtask.state() == StructuredTaskScope.Subtask.State.FAILED) {
            failure = failure ?: subtask.exception()
            stopped = true
        }
        return stopped
    }

    override fun result() {
        failure?.let { throw it }
    }
}
// endregion

// region vt-naive
/** The negative control: a virtual thread per element and no bound at all. */
fun vtNaive(job: Job) {
    Executors.newVirtualThreadPerTaskExecutor().use { threads ->
        for (x in job.source) threads.submit { job.sink(job.callBlocking(x)) }
    }
}
// endregion

// region surefold-mappar
fun surefoldMapPar(job: Job) {
    Stream.from(Iterable { job.source })
        .mapPar(job.concurrency) { x -> job.callBlocking(x) }
        .let { if (job.take < Long.MAX_VALUE) it.take(job.take) else it }
        .runFold(0L) { n, y -> job.sink(y); n + 1 }
        .run(Forks()).await().orThrow()
}
// endregion

// region surefold-mappar-unordered
fun surefoldMapParUnordered(job: Job) {
    Stream.from(Iterable { job.source })
        .mapParUnordered(job.concurrency) { x -> job.callBlocking(x) }
        .let { if (job.take < Long.MAX_VALUE) it.take(job.take) else it }
        .runFold(0L) { n, y -> job.sink(y); n + 1 }
        .run(Forks()).await().orThrow()
}
// endregion

// region cf-plain
/**
 * No library: a [Semaphore] for the bound, released by the callback that consumes the answer. The producer loop
 * blocks on the semaphore; at the end it takes every permit back, which waits for whatever is still in flight.
 * Nothing in flight is cancelled on a stop, because nothing here keeps the futures to cancel.
 */
fun cfPlain(job: Job) {
    val permits = Semaphore(job.concurrency)
    val failure = AtomicReference<Throwable>()
    val taken = AtomicLong()
    val stop = AtomicBoolean()
    for (x in job.source) {
        permits.acquire()
        if (stop.get()) {
            permits.release()
            break
        }
        job.callAsync(x).whenComplete { y, e ->
            try {
                if (e != null) {
                    failure.compareAndSet(null, e.unwrap())
                    stop.set(true)
                } else if (!stop.get()) {
                    val n = taken.incrementAndGet()
                    if (n <= job.take) job.sink(y)
                    if (n >= job.take) stop.set(true)
                }
            } catch (failed: Throwable) {
                failure.compareAndSet(null, failed)
                stop.set(true)
            } finally {
                permits.release()
            }
        }
    }
    permits.acquire(job.concurrency)
    failure.get()?.let { throw it }
}
// endregion

// region surefold-mapasync
fun surefoldMapAsync(job: Job) {
    Stream.from(Iterable { job.source })
        .mapAsync(job.concurrency) { x -> job.callAsync(x) }
        .let { if (job.take < Long.MAX_VALUE) it.take(job.take) else it }
        .runFold(0L) { n, y -> job.sink(y); n + 1 }
        .run(Forks()).await().orThrow()
}
// endregion

// region surefold-mapasync-unordered
fun surefoldMapAsyncUnordered(job: Job) {
    Stream.from(Iterable { job.source })
        .mapAsyncUnordered(job.concurrency) { x -> job.callAsync(x) }
        .let { if (job.take < Long.MAX_VALUE) it.take(job.take) else it }
        .runFold(0L) { n, y -> job.sink(y); n + 1 }
        .run(Forks()).await().orThrow()
}
// endregion

// region reactor-flatmap
/** `flatMap` answers in completion order. A `Mono` inner emits once, so its prefetch is 1, as Reactor's docs advise. */
fun reactorFlatMap(job: Job) {
    Flux.fromIterable(Iterable { job.source })
        .flatMap({ x -> job.callMono(x) }, job.concurrency, 1)
        .let { if (job.take < Long.MAX_VALUE) it.take(job.take) else it }
        .doOnNext(job.sink)
        .then().block()
}
// endregion

// region reactor-flatmapsequential
/** `flatMapSequential` subscribes to as many inners as `flatMap` does, and answers in source order. */
fun reactorFlatMapSequential(job: Job) {
    Flux.fromIterable(Iterable { job.source })
        .flatMapSequential({ x -> job.callMono(x) }, job.concurrency, 1)
        .let { if (job.take < Long.MAX_VALUE) it.take(job.take) else it }
        .doOnNext(job.sink)
        .then().block()
}
// endregion

val pekkoSystem: ActorSystem by lazy {
    ActorSystem.create(
        "bvr",
        ConfigFactory.parseString(
            """
            pekko.scheduler.tick-duration = 1ms
            pekko.loglevel = WARNING
            pekko.log-dead-letters = off
            """,
        ).withFallback(ConfigFactory.load()),
    )
}

// region pekko-mapasync
fun pekkoMapAsync(job: Job) {
    Source.fromIterator { job.source }
        .mapAsync(job.concurrency) { x -> job.callPekko(x) }
        .let { if (job.take < Long.MAX_VALUE) it.take(job.take) else it }
        .runWith(Sink.foreach { y -> job.sink(y) }, pekkoSystem)
        .toCompletableFuture().joinUnwrapped()
}
// endregion

// region pekko-mapasync-unordered
fun pekkoMapAsyncUnordered(job: Job) {
    Source.fromIterator { job.source }
        .mapAsyncUnordered(job.concurrency) { x -> job.callPekko(x) }
        .let { if (job.take < Long.MAX_VALUE) it.take(job.take) else it }
        .runWith(Sink.foreach { y -> job.sink(y) }, pekkoSystem)
        .toCompletableFuture().joinUnwrapped()
}
// endregion

/** Waits for a materialised Pekko stream, interruptibly; an interrupt stops the wait, not the stream. */
private fun <T> CompletableFuture<T>.joinUnwrapped(): T = try {
    get()
} catch (failed: ExecutionException) {
    throw failed.unwrap()
}

val contestants: Map<String, Contestant> = listOf(
    Contestant("vt-plain", Model.BLOCKING, ordered = false, "JDK", ::vtPlain),
    Contestant("vt-ordered", Model.BLOCKING, ordered = true, "JDK", ::vtOrdered),
    Contestant("vt-structured", Model.STRUCTURED, ordered = false, "JDK (preview)", ::vtStructured),
    Contestant("surefold-mappar", Model.BLOCKING, ordered = true, "Surefold", ::surefoldMapPar),
    Contestant("surefold-mappar-unordered", Model.BLOCKING, ordered = false, "Surefold", ::surefoldMapParUnordered),
    Contestant("cf-plain", Model.ASYNC, ordered = false, "JDK", ::cfPlain),
    Contestant("surefold-mapasync", Model.ASYNC, ordered = true, "Surefold", ::surefoldMapAsync),
    Contestant("surefold-mapasync-unordered", Model.ASYNC, ordered = false, "Surefold", ::surefoldMapAsyncUnordered),
    Contestant("reactor-flatmap", Model.REACTIVE, ordered = false, "Reactor", ::reactorFlatMap),
    Contestant("reactor-flatmapsequential", Model.REACTIVE, ordered = true, "Reactor", ::reactorFlatMapSequential),
    Contestant("pekko-mapasync", Model.REACTIVE, ordered = true, "Pekko", ::pekkoMapAsync),
    Contestant("pekko-mapasync-unordered", Model.REACTIVE, ordered = false, "Pekko", ::pekkoMapAsyncUnordered),
    Contestant("vt-naive", Model.UNBOUNDED, ordered = false, "JDK", ::vtNaive),
).associateBy { it.name }

/** Contestants that isolate one mechanism for a diagnostic suite; no published suite runs them. */
val diagnosticContestants: Map<String, Contestant> = listOf(
    Contestant("vt-batched", Model.BLOCKING, ordered = false, "JDK", ::vtBatched),
).associateBy { it.name }

fun contestant(name: String): Contestant = contestants[name] ?: diagnosticContestants.getValue(name)
