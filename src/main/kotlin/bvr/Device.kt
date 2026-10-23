package bvr

import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.ForkJoinPool
import java.util.concurrent.ForkJoinWorkerThread
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.LongAdder
import java.util.concurrent.locks.LockSupport

/** What an I/O call fails with when the workload says it fails, or when it took longer than its timeout. */
open class IoFailure(message: String) : RuntimeException(message) {
    constructor(x: Int) : this("connection reset on call $x")
}

class IoTimeout(x: Int) : IoFailure("call $x timed out")

/**
 * A synthetic I/O device: a call answers a future once its latency has passed. What differs between devices is
 * which thread completes the future, because whatever a contestant composed on that future runs there too.
 *
 * [lag] is how late each completion fired against its deadline. It is the device's own health: when it grows, the
 * device (or the CPU it shares with the contestant) is the bottleneck, and throughput measures that instead of
 * the contestant.
 */
interface Device : AutoCloseable {
    val name: String
    val lag: Histogram
    val firedAfterCancel: LongAdder

    /** Counters of the run in progress; the device bumps [Counters.ioDone] when a call's latency has passed. */
    var counters: Counters?

    fun call(x: Int, latencyNanos: Long, fails: Boolean): CompletableFuture<Int>

    override fun close() {}
}

/** Fires one call: records the lag, counts it, and completes (or fails) its future on the current thread. */
internal fun Device.fire(f: CompletableFuture<Int>, x: Int, fails: Boolean, deadline: Long, now: Long = System.nanoTime()) {
    lag.record(now - deadline)
    counters?.ioDone?.increment()
    if (f.isCancelled) firedAfterCancel.increment()
    if (fails) f.completeExceptionally(IoFailure(x)) else f.complete(x)
}

private fun daemon(name: String): (Runnable) -> Thread {
    val n = AtomicInteger()
    return { r -> Thread(r, "$name-${n.incrementAndGet()}").apply { isDaemon = true } }
}

/**
 * Mode A, the original device, kept for comparison: a two-thread [ScheduledThreadPoolExecutor] whose threads
 * complete each future, and so also run every callback composed on it. A contestant whose continuations do more
 * work on the completing thread slows the device for every other call in flight.
 */
class SharedDevice(threads: Int = 2) : Device {
    override val name = "shared"
    override val lag = Histogram()
    override val firedAfterCancel = LongAdder()
    @Volatile override var counters: Counters? = null
    private val timer = ScheduledThreadPoolExecutor(threads, daemon("fake-io"))

    override fun call(x: Int, latencyNanos: Long, fails: Boolean): CompletableFuture<Int> {
        val f = CompletableFuture<Int>()
        val deadline = System.nanoTime() + latencyNanos
        timer.schedule({ fire(f, x, fails, deadline) }, latencyNanos, TimeUnit.NANOSECONDS)
        return f
    }

    override fun close() { timer.shutdownNow() }
}

/**
 * A timing wheel on one platform thread, built so that the device is not what limits a run: a call is a lock-free
 * enqueue, and the wheel thread files it into a slot of [TICK_NANOS] and fires each slot once its tick has passed,
 * so a call fires between 0 and one tick (plus the thread's wake-up) after its deadline. Measured alone, it serves
 * several million calls a second on one core; [lag] says whether it kept up in a given run.
 *
 * - [inline] (mode A′, `wheel`): the wheel thread completes each future itself, as the shared device does, so a
 *   contestant's callbacks still run on the device's thread. Against `shared`, this isolates the timer's capacity.
 * - otherwise (mode B, `dispatched`): each fired batch of up to [BATCH] completions is handed to a pool of
 *   [dispatchThreads] platform threads, which complete the futures and so run whatever was composed on them. The
 *   wheel's work per call is then the same whatever the contestant. [dispatchLag] is how long a batch waited for a
 *   dispatch thread: the continuation queue's delay.
 */
class WheelDevice(
    private val inline: Boolean,
    dispatchThreads: Int = Runtime.getRuntime().availableProcessors(),
) : Device {
    override val name = if (inline) "wheel" else "dispatched"
    override val lag = Histogram()
    override val firedAfterCancel = LongAdder()
    val dispatchLag = Histogram()
    @Volatile override var counters: Counters? = null

    private class Call(val f: CompletableFuture<Int>, val x: Int, val fails: Boolean, val deadline: Long)

    private val incoming = ConcurrentLinkedQueue<Call>()
    private val slots = Array(SLOTS) { ArrayList<Call>() }
    private val overflow = ArrayList<Call>()
    private val origin = System.nanoTime()
    private var processed = 0L

    @Volatile private var running = true

    private val dispatch: ForkJoinPool? = if (inline) null else ForkJoinPool(
        dispatchThreads,
        { pool -> object : ForkJoinWorkerThread(null, pool, true) {}.apply { name = "io-dispatch-$poolIndex"; isDaemon = true } },
        null,
        true,
    )

    private val wheel = Thread.ofPlatform().daemon().name("io-wheel").start { turn() }

    override fun call(x: Int, latencyNanos: Long, fails: Boolean): CompletableFuture<Int> {
        val f = CompletableFuture<Int>()
        incoming.offer(Call(f, x, fails, System.nanoTime() + latencyNanos))
        return f
    }

    private fun tickOf(nanos: Long) = (nanos - origin) / TICK_NANOS

    private fun file(c: Call, due: ArrayList<Call>) {
        val t = tickOf(c.deadline)
        when {
            t < processed -> due.add(c)
            t - processed < SLOTS -> slots[(t % SLOTS).toInt()].add(c)
            else -> overflow.add(c)
        }
    }

    private fun turn() {
        var due = ArrayList<Call>()
        while (running) {
            while (true) file(incoming.poll() ?: break, due)
            val now = System.nanoTime()
            // A slot's tick has passed once the next one has begun: every deadline in it is due.
            val current = tickOf(now)
            while (processed < current) {
                val slot = slots[(processed % SLOTS).toInt()]
                due.addAll(slot)
                slot.clear()
                processed++
                if (processed % (SLOTS / 2) == 0L && overflow.isNotEmpty()) {
                    val again = ArrayList(overflow)
                    overflow.clear()
                    again.forEach { file(it, due) }
                }
            }
            if (due.isNotEmpty()) {
                fireAll(due, now)
                due = ArrayList()
            }
            LockSupport.parkNanos(TICK_NANOS)
        }
    }

    private fun fireAll(due: List<Call>, firedAt: Long) {
        if (dispatch == null) {
            for (c in due) fire(c.f, c.x, c.fails, c.deadline, firedAt)
            return
        }
        for (from in due.indices step BATCH) {
            val batch = due.subList(from, minOf(from + BATCH, due.size))
            dispatch.execute {
                val now = System.nanoTime()
                dispatchLag.record(now - firedAt)
                for (c in batch) fire(c.f, c.x, c.fails, c.deadline, now)
            }
        }
    }

    override fun close() {
        running = false
        wheel.join()
        dispatch?.shutdownNow()
    }

    companion object {
        const val TICK_NANOS = 100_000L
        const val SLOTS = 1 shl 16
        const val BATCH = 64
    }
}

/**
 * A device that answers nothing while it holds, so a pipeline settles with exactly its limit of calls pending and
 * nothing moving: the state the memory measurement inspects. Calls for which [holds] is false, and every call
 * once [release] has run, complete at once on the calling thread.
 */
class HeldDevice(private val holds: (Int) -> Boolean = { true }) : Device {
    override val name = "held"
    override val lag = Histogram()
    override val firedAfterCancel = LongAdder()
    @Volatile override var counters: Counters? = null

    // A future and its element per held call, in two growing arrays: the device's own bookkeeping is 8 bytes a
    // call beyond the future itself, which every contestant needs anyway.
    private var futures = arrayOfNulls<CompletableFuture<Int>>(1024)
    private var elements = IntArray(1024)
    private var size = 0
    private var released = false

    /** Calls held and not yet done: not answered here, and not cancelled or timed out by their caller. */
    val holding: Int get() = synchronized(this) { (0 until size).count { !futures[it]!!.isDone } }

    /** Answers the held calls whose element matches [which], now, on the calling thread. */
    fun answer(which: (Int) -> Boolean) = settle(which, fails = false)

    /** Fails the held calls whose element matches [which], now, on the calling thread. */
    fun fail(which: (Int) -> Boolean) = settle(which, fails = true)

    private fun settle(which: (Int) -> Boolean, fails: Boolean) {
        val chosen = synchronized(this) { (0 until size).filter { which(elements[it]) }.map { futures[it]!! to elements[it] } }
        for ((f, x) in chosen) fire(f, x, fails, System.nanoTime())
    }

    /**
     * Held calls that something still waits on: not done, and with a thread parked in `get` or a callback
     * registered on them. After a run has returned, these are calls it abandoned rather than let go of.
     */
    fun stillAwaited(): Int = synchronized(this) {
        (0 until size).count { !futures[it]!!.isDone && futures[it]!!.numberOfDependents > 0 }
    }

    override fun call(x: Int, latencyNanos: Long, fails: Boolean): CompletableFuture<Int> {
        val f = CompletableFuture<Int>()
        val held = holds(x) && synchronized(this) {
            if (released) return@synchronized false
            if (size == futures.size) {
                futures = futures.copyOf(size * 2)
                elements = elements.copyOf(size * 2)
            }
            futures[size] = f
            elements[size] = x
            size++
            true
        }
        if (!held) fire(f, x, fails, System.nanoTime())
        return f
    }

    /** Held calls their caller has cancelled, or completed some other way (a timeout), since they were held. */
    fun cancelled(): Int = synchronized(this) { (0 until size).count { futures[it]!!.isCancelled } }

    /** Whether every held call has been let go of by its caller: cancelled, or completed by its own timeout. */
    fun allHeldDone(): Boolean = synchronized(this) { (0 until size).all { futures[it]!!.isDone } }

    /** Answers every held call, and every later one at once. */
    fun release() {
        val (fs, xs, n) = synchronized(this) {
            released = true
            Triple(futures, elements, size).also { size = 0; futures = arrayOfNulls(0); elements = IntArray(0) }
        }
        for (i in 0 until n) fire(fs[i]!!, xs[i], false, System.nanoTime())
    }
}
