package bvr

import io.github.matthewjones372.lark.Limit
import io.github.matthewjones372.lark.stream.Exit
import io.github.matthewjones372.lark.stream.Forks
import io.github.matthewjones372.lark.stream.Stream
import io.github.matthewjones372.lark.stream.from
import io.github.matthewjones372.lark.stream.mapParUnordered
import io.github.matthewjones372.lark.stream.run
import io.github.matthewjones372.lark.stream.runFold
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import java.util.concurrent.CancellationException
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.ExecutionException
import java.util.concurrent.Executors
import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.LockSupport

/**
 * How a dependency's capacity moves over a run, and how load arrives.
 *
 * - Closed loop ([ratePerSec] null): the source hands out elements as fast as the contestant pulls them, for
 *   [durationMs]. An element's arrival is the moment it is pulled.
 * - Open loop: element `i` arrives at `i / ratePerSec` whether or not the contestant is ready for it. One that is
 *   pulled late has waited in the client, and its latency counts from its arrival, not from the pull.
 *
 * [deadlineMs], when set, is enforced: an element older than that when it reaches the call is dropped without calling
 * (checked both before and after waiting for a permit), as a caller that has already timed out would. [sloMs] only
 * classifies: a call answered later than it is a late answer. [queueLimit] makes the dependency reject a call when
 * that many already wait in its queue.
 */
data class Pattern(
    val name: String,
    val durationMs: Long,
    val changesMs: List<Long>,
    val ratePerSec: Int? = null,
    val deadlineMs: Long? = null,
    val sloMs: Long = 50,
    val queueLimit: Int? = null,
    val capacityAt: (Long) -> Int,
)

private const val FULL = 500
private const val DROPPED = 100
private const val SERVICE_MICROS = 5_000L

val patterns: Map<String, Pattern> = listOf(
    Pattern("sudden", 6_000, listOf(3_000)) { t -> if (t < 3_000) FULL else DROPPED },
    Pattern("gradual", 7_000, listOf(2_000, 5_000)) { t ->
        when {
            t < 2_000 -> FULL
            t < 5_000 -> FULL - ((FULL - DROPPED) * (t - 2_000) / 3_000).toInt()
            else -> DROPPED
        }
    },
    Pattern("recovery", 7_000, listOf(2_000, 4_500)) { t -> if (t in 2_000 until 4_500) DROPPED else FULL },
    Pattern("rejecting", 6_000, listOf(3_000), queueLimit = 200) { t -> if (t < 3_000) FULL else DROPPED },
    Pattern("open-50ms", 6_000, listOf(3_000), ratePerSec = 60_000, deadlineMs = 50) { t -> if (t < 3_000) FULL else DROPPED },
    Pattern("open-500ms", 6_000, listOf(3_000), ratePerSec = 60_000, deadlineMs = 500, sloMs = 500) { t ->
        if (t < 3_000) FULL else DROPPED
    },
    // The warm-up before every measured run: steady full capacity.
    Pattern("steady", 2_000, emptyList()) { FULL },
).associateBy { it.name }

class Rejected : RuntimeException("dependency queue full") {
    override fun fillInStackTrace() = this
}

/** Thrown inside a permit to give it back unmeasured: the call was never made because its caller had given up. */
class Expired : CancellationException("deadline passed before the call") {
    override fun fillInStackTrace() = this
}

/**
 * A dependency that serves as many calls at once as its capacity allows now, each in [SERVICE_MICROS]; the rest
 * wait in its own FIFO queue, as in a connection pool or a server's accept backlog. Service time is kept by a
 * dispatched [WheelDevice], so callers' continuations run on its dispatch pool, as in the I/O scenarios.
 */
class Dependency(private val pattern: Pattern, private val t0: Long) {
    private class Waiting(val i: Int, val answer: CompletableFuture<Int>)

    private val waiting = ConcurrentLinkedQueue<Waiting>()
    val queued = AtomicInteger()
    val serving = AtomicInteger()
    private val device = WheelDevice(inline = false)
    @Volatile private var running = true

    // Capacity can rise with nothing finishing to notice it: look every millisecond.
    private val ticker = Thread.ofPlatform().daemon().name("dependency-ticker").start {
        while (running) { admit(); LockSupport.parkNanos(1_000_000) }
    }

    fun capacity(): Int = pattern.capacityAt((System.nanoTime() - t0) / 1_000_000)

    fun call(i: Int): CompletableFuture<Int> {
        val limit = pattern.queueLimit
        if (limit != null && queued.get() >= limit) return CompletableFuture.failedFuture(Rejected())
        val w = Waiting(i, CompletableFuture())
        queued.incrementAndGet()
        waiting.offer(w)
        admit()
        return w.answer
    }

    private fun admit() {
        while (true) {
            val now = serving.get()
            if (now >= capacity() || waiting.isEmpty()) return
            if (!serving.compareAndSet(now, now + 1)) continue
            val next = waiting.poll()
            if (next == null) { serving.decrementAndGet(); return }
            queued.decrementAndGet()
            device.call(next.i, SERVICE_MICROS * 1_000, false).thenRun {
                serving.decrementAndGet()
                admit()
                next.answer.complete(next.i)
            }
        }
    }

    fun close() {
        running = false
        ticker.join()
        device.close()
    }
}

/** Everything one overload run records: per element by index, and a 10 ms time series. */
class OverloadRun(val pattern: Pattern, maxElements: Int = 1_500_000) {
    val t0 = System.nanoTime()
    val dependency = Dependency(pattern, t0)
    val arrival = LongArray(maxElements)
    val answeredAt = LongArray(maxElements)
    val depLatency = LongArray(maxElements)
    val outcome = ByteArray(maxElements)
    val pulled = AtomicInteger()
    val inBody = AtomicInteger()
    @Volatile var limitNow: () -> Int? = { null }
    private val end = t0 + pattern.durationMs * 1_000_000

    fun now() = System.nanoTime() - t0

    /** The source: closed or open loop, as the pattern says. */
    val source: Iterator<Int> = object : Iterator<Int> {
        var i = 0
        val gapNanos = pattern.ratePerSec?.let { 1_000_000_000L / it }

        override fun hasNext(): Boolean =
            if (gapNanos == null) System.nanoTime() < end && i < arrival.size else i * gapNanos < pattern.durationMs * 1_000_000

        override fun next(): Int {
            val idx = i++
            if (gapNanos == null) {
                arrival[idx] = now()
            } else {
                val due = idx * gapNanos
                while (now() < due) LockSupport.parkNanos(due - now())
                arrival[idx] = due
            }
            pulled.set(i)
            return idx
        }
    }

    private fun expired(i: Int) = pattern.deadlineMs?.let { now() - arrival[i] > it * 1_000_000 } ?: false

    private fun finish(i: Int, what: Int, dep: Long = 0) {
        answeredAt[i] = now()
        depLatency[i] = dep
        outcome[i] = what.toByte()
    }

    /**
     * One element on a thread that may block, inside whatever [permit] the contestant uses. The deadline is checked
     * before the permit and again inside it, where an expired element is given back unmeasured.
     */
    fun serveBlocking(i: Int, permit: (() -> Unit) -> Unit) {
        if (expired(i)) return finish(i, EXPIRED)
        inBody.incrementAndGet()
        try {
            permit {
                if (expired(i)) throw Expired()
                val started = now()
                try {
                    dependency.call(i).get()
                } catch (e: ExecutionException) {
                    throw e.cause!!
                }
                finish(i, OK, now() - started)
            }
        } catch (_: Expired) {
            finish(i, EXPIRED)
        } catch (_: Rejected) {
            finish(i, REJECTED)
        } finally {
            inBody.decrementAndGet()
        }
    }

    /** The same element without blocking: [permit], if any, wraps the call as `AsyncLimit.withPermitAsync` does. */
    fun serveAsync(i: Int, permit: ((() -> CompletableFuture<Unit>) -> CompletableFuture<Unit>)?): CompletableFuture<Unit> {
        if (expired(i)) return CompletableFuture.completedFuture(finish(i, EXPIRED))
        inBody.incrementAndGet()
        val call = {
            if (expired(i)) {
                CompletableFuture.failedFuture<Unit>(Expired())
            } else {
                val started = now()
                dependency.call(i).thenApply { finish(i, OK, now() - started) }
            }
        }
        return (permit?.invoke(call) ?: call()).handle { _, e ->
            inBody.decrementAndGet()
            when (e?.unwrap()) {
                null -> {}
                is Expired -> finish(i, EXPIRED)
                is Rejected -> finish(i, REJECTED)
                else -> throw e
            }
        }
    }

    companion object {
        const val OK = 1
        const val REJECTED = 2
        const val EXPIRED = 3
    }
}

// region overload-vt
/** Plain virtual threads: a [Semaphore] of [bound], and inside it, optionally, a second limit. */
fun overloadVt(run: OverloadRun, bound: Int, inner: ((() -> Unit) -> Unit)?) {
    val permits = Semaphore(bound)
    Executors.newVirtualThreadPerTaskExecutor().use { threads ->
        for (i in run.source) {
            permits.acquire()
            threads.submit {
                try {
                    run.serveBlocking(i) { call -> if (inner == null) call() else inner(call) }
                } finally {
                    permits.release()
                }
            }
        }
    }
}
// endregion

// region overload-surefold
/**
 * `mapParUnordered(limit)` written out as Surefold defines it (`mapParUnordered(limit.max) { limit.withPermit { … } }`),
 * so that an expired element can be caught outside the permit instead of failing the stream.
 */
fun overloadSurefold(run: OverloadRun, limit: Limit) {
    val exit = Stream.from(Iterable { run.source })
        .mapParUnordered(limit.max) { i -> run.serveBlocking(i) { call -> limit.withPermit(call) } }
        .runFold(0L) { n, _ -> n + 1 }
        .run(Forks()).toCompletableFuture().join()
    check(exit is Exit.Done) { "surefold overload run ended with $exit" }
}
// endregion

// region overload-reactor
/** `flatMap` with a bound of [bound] and, optionally, the ported adaptive limit inside it. */
fun overloadReactor(run: OverloadRun, bound: Int, limit: AsyncLimit?) {
    Flux.fromIterable(Iterable { run.source })
        // An open-loop source parks until each arrival is due. Reactor pulls a source on whichever thread requests,
        // which here is a completing thread; subscribeOn moves the pulling to a worker of its own, as Reactor's
        // documentation does for any blocking source.
        .let { if (run.pattern.ratePerSec != null) it.subscribeOn(Schedulers.boundedElastic()) else it }
        .flatMap({ i ->
            val permit = limit?.let { l -> { call: () -> CompletableFuture<Unit> -> l.withPermitAsync(call) } }
            Mono.fromFuture(run.serveAsync(i, permit))
        }, bound, 1)
        .then().block()
}
// endregion

private const val MAX_BOUND = 1_000

private fun adaptiveSurefold(run: OverloadRun) =
    Limit.adaptive("dependency", initial = 20, max = MAX_BOUND).also { l -> run.limitNow = { l.current } }

private fun adaptivePort(run: OverloadRun) =
    AsyncLimit.adaptive(initial = 20, max = MAX_BOUND).also { l -> run.limitNow = { l.current } }

/** Model × control policy. `fixed-N` is a bound somebody picked; `adaptive` is Surefold's Vegas, `adaptive-port` the port. */
val overloadContestants: Map<String, (OverloadRun) -> Unit> = linkedMapOf(
    "vt-fixed-1000" to { r -> overloadVt(r, 1_000, null) },
    "vt-fixed-500" to { r -> overloadVt(r, 500, null) },
    "vt-fixed-100" to { r -> overloadVt(r, 100, null) },
    "vt-adaptive" to { r -> val l = adaptiveSurefold(r); overloadVt(r, l.max) { call -> l.withPermit(call) } },
    "vt-adaptive-port" to { r -> val l = adaptivePort(r); overloadVt(r, l.max) { call -> l.withPermit(call) } },
    "surefold-fixed-1000" to { r -> overloadSurefold(r, Limit.fixed(1_000)) },
    "surefold-fixed-500" to { r -> overloadSurefold(r, Limit.fixed(500)) },
    "surefold-adaptive" to { r -> overloadSurefold(r, adaptiveSurefold(r)) },
    "reactor-fixed-1000" to { r -> overloadReactor(r, 1_000, null) },
    "reactor-fixed-500" to { r -> overloadReactor(r, 500, null) },
    "reactor-adaptive-port" to { r -> overloadReactor(r, MAX_BOUND, adaptivePort(r)) },
)

val overloadSuites: Map<String, List<Pair<String, String>>> = mapOf(
    "overload" to patterns.keys.filter { it != "steady" }.flatMap { p -> overloadContestants.keys.map { p to it } },
)

private fun quantile(sorted: LongArray, q: Double): Double =
    if (sorted.isEmpty()) Double.NaN else sorted[minOf(sorted.size - 1, (q * sorted.size).toInt())] / 1e6

/** Per phase, by arrival time: what happened to the elements that arrived in it. */
private fun phases(r: OverloadRun, n: Int): List<Map<String, Any?>> {
    val bounds = listOf(0L) + r.pattern.changesMs + r.pattern.durationMs
    val slo = r.pattern.sloMs * 1_000_000
    return bounds.zipWithNext().map { (from, until) ->
        val lo = from * 1_000_000
        val hi = until * 1_000_000
        val e2e = ArrayList<Long>()
        val dep = ArrayList<Long>()
        var arrivals = 0; var ok = 0; var rejected = 0; var expired = 0; var late = 0; var unanswered = 0
        var completedInPhase = 0
        for (i in 0 until n) {
            val a = r.outcome[i].toInt()
            if (a == OverloadRun.OK && r.answeredAt[i] in lo until hi) completedInPhase++
            if (r.arrival[i] !in lo until hi) continue
            arrivals++
            when (a) {
                OverloadRun.OK -> {
                    ok++
                    val l = r.answeredAt[i] - r.arrival[i]
                    e2e += l
                    dep += r.depLatency[i]
                    if (l > slo) late++
                }
                OverloadRun.REJECTED -> rejected++
                OverloadRun.EXPIRED -> expired++
                else -> unanswered++
            }
        }
        val seconds = (until - from) / 1000.0
        val e = e2e.toLongArray().also { it.sort() }
        val d = dep.toLongArray().also { it.sort() }
        linkedMapOf(
            "fromMs" to from, "untilMs" to until, "arrivals" to arrivals, "ok" to ok, "rejected" to rejected,
            "expired" to expired, "late" to late, "unanswered" to unanswered,
            "throughputPerSec" to completedInPhase / seconds,
            "goodputPerSec" to (ok - late) / seconds,
            "e2eP50Ms" to quantile(e, 0.5), "e2eP99Ms" to quantile(e, 0.99), "e2eP999Ms" to quantile(e, 0.999),
            "depP50Ms" to quantile(d, 0.5), "depP99Ms" to quantile(d, 0.99),
        )
    }
}

/**
 * From the 10 ms series: after each change, how long until the dependency's queue stayed at or under half its
 * capacity for the rest of that phase (relief, after a drop), and how long until it served at least 90% of its
 * capacity for 200 ms (reclaim, after a rise and from the start). Null when it never did.
 */
private fun recovery(p: Pattern, t: List<Long>, cap: List<Int>, serving: List<Int>, queued: List<Int>): List<Map<String, Any?>> {
    val bounds = listOf(0L) + p.changesMs + p.durationMs
    return bounds.zipWithNext().mapIndexed { k, (from, until) ->
        val idx = t.indices.filter { t[it] in from until until }
        val before = if (k == 0) 0 else p.capacityAt(from - 1)
        val after = p.capacityAt(from)
        val relief = idx.firstOrNull { s -> idx.filter { it >= s }.all { queued[it] <= cap[it] / 2 } }?.let { t[it] - from }
        val reclaim = idx.windowed(20).firstOrNull { w -> w.all { serving[it] >= cap[it] * 9 / 10 } }?.let { t[it.first()] - from }
        mapOf("fromMs" to from, "capacityBefore" to before, "capacityAfter" to after, "reliefMs" to relief, "reclaimMs" to reclaim)
    }
}

fun overloadRun(pattern: Pattern, name: String, measured: Boolean) {
    val contestant = overloadContestants.getValue(name)
    val r = OverloadRun(pattern)
    val t = ArrayList<Long>(); val cap = ArrayList<Int>(); val serving = ArrayList<Int>(); val queued = ArrayList<Int>()
    val inBody = ArrayList<Int>(); val clientQueue = ArrayList<Int>(); val limit = ArrayList<Int?>()
    val sampling = java.util.concurrent.atomic.AtomicBoolean(true)
    val gap = pattern.ratePerSec?.let { 1_000_000_000L / it }
    val sampler = Thread.ofPlatform().daemon().name("overload-sampler").start {
        while (sampling.get()) {
            val now = r.now()
            t += now / 1_000_000; cap += r.dependency.capacity(); serving += r.dependency.serving.get()
            queued += r.dependency.queued.get(); inBody += r.inBody.get(); limit += r.limitNow()
            clientQueue += gap?.let { g -> maxOf(0, (minOf(now, pattern.durationMs * 1_000_000) / g).toInt() - r.pulled.get()) } ?: 0
            LockSupport.parkNanos(10_000_000)
        }
    }
    // A run that has not drained well after its pattern ended is stuck: say where, and fail it rather than hang.
    val watchdog = Thread.ofPlatform().daemon().name("overload-watchdog").start {
        try {
            Thread.sleep(pattern.durationMs + 30_000)
            System.err.println("STUCK ${pattern.name} $name: pulled=${r.pulled.get()} inBody=${r.inBody.get()} " +
                "serving=${r.dependency.serving.get()} queued=${r.dependency.queued.get()} limit=${r.limitNow()}")
            System.exit(4)
        } catch (_: InterruptedException) {
            // Finished in time.
        }
    }
    val failure = runCatching { onVirtualThread { contestant(r) } }.exceptionOrNull()
    watchdog.interrupt()
    val wallMs = r.now() / 1e6
    sampling.set(false)
    sampler.join()
    r.dependency.close()
    val n = r.pulled.get()
    val unanswered = (0 until n).count { r.outcome[it].toInt() == 0 }
    if (!measured) return
    emit(
        linkedMapOf<String, Any?>(
            "kind" to "overload", "pattern" to pattern.name, "contestant" to name, "valid" to (failure == null && unanswered == 0),
            "error" to failure?.toString(), "wallMs" to wallMs, "elements" to n,
            "ok" to (0 until n).count { r.outcome[it].toInt() == OverloadRun.OK },
            "rejected" to (0 until n).count { r.outcome[it].toInt() == OverloadRun.REJECTED },
            "expired" to (0 until n).count { r.outcome[it].toInt() == OverloadRun.EXPIRED },
            "unanswered" to unanswered,
            "ratePerSec" to pattern.ratePerSec, "deadlineMs" to pattern.deadlineMs, "sloMs" to pattern.sloMs,
            "queueLimit" to pattern.queueLimit,
            "phases" to phases(r, n),
            "recovery" to recovery(pattern, t, cap, serving, queued),
            "series" to mapOf("tMs" to t, "capacity" to cap, "serving" to serving, "depQueue" to queued,
                "inBody" to inBody, "clientQueue" to clientQueue, "limit" to limit),
        ) + environment(),
    )
    if (failure != null || unanswered > 0) {
        System.err.println("INVALID: overload ${pattern.name} $name: $failure, $unanswered unanswered")
        System.exit(3)
    }
}

/** `overload <pattern> <contestant>`: a steady warm-up, then one measured run, in a fresh JVM. */
fun overloadMain(args: List<String>) {
    overloadRun(patterns.getValue("steady"), args[1], measured = false)
    overloadRun(patterns.getValue(args[0]), args[1], measured = true)
}
