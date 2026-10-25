package bvr

import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutionException
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.math.log10
import kotlin.math.max

/**
 * Surefold's `Limit` (surefold-core, `Limit.kt` and `Vegas.kt` as of Surefold `3ccde44`, spec 0171), ported so that a contestant
 * which may not block (Reactor) can use the same control policy. It does not have spec 0187's changes (slow start,
 * thresholds that scale with the limit, cuts to what the dependency is serving, the periodic probe); see
 * docs/adaptive-limit.md. Surefold's `Limit.withPermit` waits on a condition, so it
 * can only be used from a thread that may park. This class keeps the same state, under the same lock, with the same
 * Vegas arithmetic, and adds [acquire], which answers a future instead of waiting.
 *
 * The overload experiment also runs plain virtual threads on both this port and Surefold's own `Limit`, which checks
 * that the port behaves as the original does.
 */
class AsyncLimit private constructor(initial: Int, val max: Int, private val vegas: PortedVegas?) {
    private val lock = ReentrantLock()
    private var allowed = initial
    private var using = 0
    private val waiting = ArrayDeque<CompletableFuture<Unit>>()

    val current: Int get() = lock.withLock { allowed }

    /**
     * A future that completes once the limit has room for one more call, which then holds a place. A caller that
     * finds room takes it even if others are waiting, as a thread arriving at Surefold's `Limit` does: its `acquire`
     * waits on a condition, and a signalled waiter must win the lock again before it can take the place it was
     * woken for. Strict FIFO here would hand every place to the oldest waiter, which under a deadline is the one
     * most likely to have expired.
     */
    fun acquire(): CompletableFuture<Unit> = lock.withLock {
        if (using < allowed) {
            using++
            DONE
        } else {
            CompletableFuture<Unit>().also { waiting.addLast(it) }
        }
    }

    /**
     * Gives back a place. An answered call teaches an adaptive limit and a failed one backs it off; one that was
     * cancelled (a [java.util.concurrent.CancellationException], as Surefold's `Limit` also treats it) says nothing about
     * the dependency and teaches nothing.
     */
    fun release(tookNanos: Long, failed: Boolean, measured: Boolean = true) {
        val granted = ArrayList<CompletableFuture<Unit>>()
        lock.withLock {
            val inside = using
            using--
            if (vegas != null && measured) allowed = vegas.sample(tookNanos, inside, dropped = failed)
            while (using < allowed && waiting.isNotEmpty()) {
                using++
                granted += waiting.removeFirst()
            }
        }
        // Outside the lock: whatever waited on a place runs here, on the releasing thread. A granted call that
        // ends at once releases again from inside this loop; its grants join this thread's queue instead of
        // recursing, or a thousand waiters whose calls fail fast would overflow the stack.
        val running = trampoline.get()
        if (running != null) {
            running.addAll(granted)
            return
        }
        val queue = ArrayDeque(granted)
        trampoline.set(queue)
        try {
            while (queue.isNotEmpty()) queue.removeFirst().complete(Unit)
        } finally {
            trampoline.remove()
        }
    }

    private val trampoline = ThreadLocal<ArrayDeque<CompletableFuture<Unit>>>()

    /** [call] once a place is free, without blocking: the place is released when the call's future completes. */
    fun <A> withPermitAsync(call: () -> CompletableFuture<A>): CompletableFuture<A> =
        acquire().thenCompose {
            val started = System.nanoTime()
            val f = try { call() } catch (t: Throwable) { CompletableFuture.failedFuture(t) }
            f.whenComplete { _, e ->
                val cancelled = e?.unwrap() is java.util.concurrent.CancellationException
                release(System.nanoTime() - started, failed = e != null, measured = !cancelled)
            }
        }

    /** The blocking form, for a thread that may park; the same as Surefold's `Limit.withPermit`. */
    fun <A> withPermit(block: () -> A): A {
        try {
            acquire().get()
        } catch (e: ExecutionException) {
            throw e.cause ?: e
        }
        val started = System.nanoTime()
        var failed = true
        var measured = true
        try {
            return block().also { failed = false }
        } catch (cancelled: java.util.concurrent.CancellationException) {
            measured = false
            throw cancelled
        } finally {
            release(System.nanoTime() - started, failed, measured)
        }
    }

    companion object {
        private val DONE: CompletableFuture<Unit> = CompletableFuture.completedFuture(Unit)

        fun fixed(n: Int) = AsyncLimit(n, n, null)

        fun adaptive(initial: Int = 20, min: Int = 1, max: Int = 1_000) = AsyncLimit(initial, max, PortedVegas(initial, min, max))
    }
}

/** Surefold's `Vegas` as of `3ccde44`, unchanged but for its name and visibility; see its KDoc in surefold-core. */
class PortedVegas(initial: Int, private val min: Int, private val max: Int) {
    private var limit = initial.toDouble()
    private var unloaded = Double.MAX_VALUE
    private var windows = 0L

    private var windowCalls = 0
    private var windowTook = 0L
    private var windowMostInside = 0

    fun sample(tookNanos: Long, inFlight: Int, dropped: Boolean): Int {
        if (dropped) {
            limit = max(min.toDouble(), limit * BACKOFF)
            windowCalls = 0
            return limit.toInt()
        }
        windowCalls++
        windowTook += max(1L, tookNanos)
        windowMostInside = maxOf(windowMostInside, inFlight)
        if (windowCalls < max(MIN_WINDOW.toDouble(), limit)) return limit.toInt()

        val average = windowTook.toDouble() / windowCalls
        if (++windows % FORGET_EVERY == 0L) unloaded = average
        unloaded = minOf(unloaded, average)
        val busy = windowMostInside >= limit / 2
        windowCalls = 0
        windowTook = 0
        windowMostInside = 0

        val queued = limit * (1 - unloaded / average)
        val step = max(1.0, log10(limit))
        limit = when {
            queued < 1 && busy -> limit + max(step, limit * GROWTH)
            queued <= GROW_BELOW * step && busy -> limit + step
            queued >= SHRINK_ABOVE * step -> max(limit * unloaded / average + GROW_BELOW * step, limit * MOST_CUT)
            else -> limit
        }.coerceIn(min.toDouble(), max.toDouble())
        return limit.toInt()
    }

    private companion object {
        const val BACKOFF = 0.9
        const val GROWTH = 0.1
        const val MOST_CUT = 0.5
        const val GROW_BELOW = 3
        const val SHRINK_ABOVE = 6
        const val MIN_WINDOW = 16
        const val FORGET_EVERY = 100L
    }
}
