package bvr

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.LongAdder

/**
 * Observable behaviour of each contestant, beyond its answer: what a bound, a stop, a failure, a timeout, a slow
 * consumer and an interrupt actually do. Each check returns what it saw; ContestantSemanticsTest asserts on it, and
 * `Main semantics` prints it for docs/semantics.md. Nothing here is timed for performance; the waits are generous so
 * the checks are deterministic on a loaded machine.
 */
object Semantics {
    /** A run on its own virtual thread, waited for at most [waitMs]; [finished] false means it was still running. */
    class Ran(val finished: Boolean, val failure: Throwable?, val ms: Long, val thread: Thread)

    fun start(body: () -> Unit): Pair<Thread, () -> Throwable?> {
        var failure: Throwable? = null
        val t = Thread.ofVirtual().name("semantics-run").start { try { body() } catch (e: Throwable) { failure = e } }
        return t to { failure }
    }

    fun ran(waitMs: Long, body: () -> Unit): Ran {
        val t0 = System.nanoTime()
        val (t, failure) = start(body)
        val finished = t.join(java.time.Duration.ofMillis(waitMs))
        return Ran(finished, failure(), (System.nanoTime() - t0) / 1_000_000, t)
    }

    private fun Throwable.rootIoFailure(): Throwable? {
        var e: Throwable? = this
        while (e != null) { if (e is IoFailure) return e; e = e.cause }
        return null
    }

    private fun sumTo(n: Int, skip: (Int) -> Boolean = { false }) = (0 until n).sumOf { if (skip(it)) 0L else it.toLong() }

    /** Every element answered once; for an ordered contestant, in source order. */
    fun answers(c: Contestant): Map<String, Any?> {
        val n = 3_000
        val counters = Counters()
        val device = WheelDevice(inline = false)
        val seen = java.util.Collections.synchronizedList(ArrayList<Int>())
        val r = ran(30_000) {
            c.run(Job(counted(n, counters), 32, DeviceIo(device, Workload(300, Dist.LOGNORMAL), counters),
                sink = { y -> seen += y; counters.consumed.increment() }))
        }
        device.close()
        val sorted = seen.sorted()
        return mapOf(
            "finished" to r.finished, "failure" to r.failure?.toString(),
            "allOnce" to (sorted == (0 until n).toList()), "inOrder" to (seen == (0 until n).toList()),
        )
    }

    /** With nothing answering, how many calls the contestant starts, and how far ahead of them it reads its source. */
    fun bound(c: Contestant, n: Int = 32): Map<String, Any?> {
        val counters = Counters()
        val device = HeldDevice()
        val sum = LongAdder()
        val (t, failure) = start {
            c.run(Job(counted(1_000, counters), n, DeviceIo(device, Workload(0), counters),
                sink = { y -> sum.add(y.toLong()); counters.consumed.increment() }))
        }
        val settledAt = settleQuietly(counters)
        val pending = device.holding
        val readAhead = counters.produced.sum() - counters.started.sum()
        device.release()
        t.join()
        return mapOf(
            "settled" to settledAt, "pending" to pending, "readAhead" to readAhead,
            "correct" to (failure() == null && sum.sum() == sumTo(1_000)),
        )
    }

    /** Waits for the counters to stop moving; true if they did within ten seconds. */
    private fun settleQuietly(c: Counters): Boolean {
        var last = -1L
        var since = System.currentTimeMillis()
        val deadline = since + 10_000
        while (System.currentTimeMillis() < deadline) {
            Thread.sleep(20)
            val now = c.produced.sum() * 31 + c.started.sum() * 17 + c.consumed.sum()
            if (now != last) { last = now; since = System.currentTimeMillis() }
            if (System.currentTimeMillis() - since > 300) return true
        }
        return false
    }

    /** Flowing calls, counted at the source and the sink: the most ever between being started and being consumed. */
    fun mostInFlight(c: Contestant, n: Int = 32): Long {
        val counters = Counters()
        val device = WheelDevice(inline = false)
        val inFlight = AtomicLong()
        val most = AtomicLong()
        val base = DeviceIo(device, Workload(200, Dist.LOGNORMAL), counters)
        val io = object : Io {
            override val workload = base.workload
            override val counters = counters
            fun started() { most.accumulateAndGet(inFlight.incrementAndGet(), ::maxOf) }
            override fun async(x: Int) = started().let { base.async(x) }
            override fun blocking(x: Int) = started().let { base.blocking(x) }
        }
        c.run(Job(counted(5_000, counters), n, io, sink = { inFlight.decrementAndGet(); counters.consumed.increment() }))
        device.close()
        return most.get()
    }

    /**
     * Thirty-two calls start and are held; then elements 0 to 9 are answered, which satisfies `take(10)`. Does the
     * run return while the other 22 are still held, how much more of the source did it read, and what became of
     * the 22: cancelled at the device, or still awaited by a parked thread or a registered callback?
     */
    fun take(c: Contestant): Map<String, Any?> {
        val counters = Counters()
        val device = HeldDevice()
        val got = LongAdder()
        val (t, failure) = start {
            c.run(Job(counted(100_000, counters), 32, DeviceIo(device, Workload(0), counters), take = 10,
                sink = { got.increment(); counters.consumed.increment() }))
        }
        settleQuietly(counters)
        val t0 = System.nanoTime()
        device.answer { it < 10 }
        val returned = t.join(java.time.Duration.ofSeconds(3))
        val ms = (System.nanoTime() - t0) / 1_000_000
        Thread.sleep(200)
        val observed = mapOf(
            "returned" to returned, "ms" to ms, "answers" to got.sum(), "produced" to counters.produced.sum(),
            "stillHeld" to device.holding, "heldCancelled" to device.cancelled(), "stillAwaited" to device.stillAwaited(),
        )
        device.release()
        t.join()
        return observed + ("failure" to failure()?.toString())
    }

    /** Thirty-two calls start and are held; then element 5 fails. Does the failure surface while the rest are held? */
    fun failure(c: Contestant): Map<String, Any?> {
        val counters = Counters()
        val device = HeldDevice()
        val (t, failure) = start {
            c.run(Job(counted(100_000, counters), 32, DeviceIo(device, Workload(0), counters), sink = { counters.consumed.increment() }))
        }
        settleQuietly(counters)
        val t0 = System.nanoTime()
        device.fail { it == 5 }
        val returned = t.join(java.time.Duration.ofSeconds(3))
        val ms = (System.nanoTime() - t0) / 1_000_000
        Thread.sleep(200)
        val observed = mapOf(
            "returnedWhileHeld" to returned, "ms" to ms, "produced" to counters.produced.sum(),
            "stillHeld" to device.holding, "heldCancelled" to device.cancelled(), "stillAwaited" to device.stillAwaited(),
        )
        device.release()
        t.join()
        val e = failure()
        return observed + mapOf("failedWithIoFailure" to (e?.rootIoFailure() != null), "failure" to e?.let { it::class.simpleName })
    }

    /** Every tenth call never answers; a 50 ms timeout, recovered: the run completes and every held call is let go. */
    fun timeouts(c: Contestant): Map<String, Any?> {
        val counters = Counters()
        val device = HeldDevice { it % 10 == 3 }
        val sum = LongAdder()
        val n = 200
        val r = ran(20_000) {
            c.run(Job(counted(n, counters), 16, DeviceIo(device, Workload(0), counters), recover = true, timeoutMillis = 50,
                sink = { y -> sum.add(y.toLong()); counters.consumed.increment() }))
        }
        val heldDone = device.allHeldDone()
        device.release()
        r.thread.join()
        return mapOf(
            "finished" to r.finished, "failure" to r.failure?.toString(), "answers" to counters.consumed.sum(),
            "correct" to (sum.sum() == sumTo(n) { it % 10 == 3 }), "heldCallsReleased" to heldDone,
        )
    }

    /** A consumer that takes 2 ms an element: how far ahead of it does the source get? */
    fun slowConsumer(c: Contestant, n: Int = 32): Map<String, Any?> {
        val counters = Counters()
        val device = WheelDevice(inline = false)
        val most = AtomicLong()
        val source = object : Iterator<Int> {
            val inner = counted(1_000_000, counters)
            override fun hasNext() = inner.hasNext()
            override fun next(): Int = inner.next().also {
                most.accumulateAndGet(counters.produced.sum() - counters.consumed.sum(), ::maxOf)
            }
        }
        val lock = Any()
        val r = ran(60_000) {
            c.run(Job(source, n, DeviceIo(device, Workload(100), counters), take = 300,
                sink = { synchronized(lock) { Thread.sleep(2) }; counters.consumed.increment() }))
        }
        device.close()
        return mapOf("finished" to r.finished, "mostAheadOfConsumer" to most.get(), "bound" to n)
    }

    /** The consumer stops for 400 ms at its 100th element: is the source read between 100 and 380 ms into the pause? */
    fun demandPauseGrowth(c: Contestant, n: Int = 32): Map<String, Any?> {
        val counters = Counters()
        val device = WheelDevice(inline = false)
        val paused = CountDownLatch(1)
        val consumed = AtomicLong()
        val (t, failure) = start {
            c.run(Job(counted(3_000, counters), n, DeviceIo(device, Workload(100), counters),
                sink = {
                    if (consumed.incrementAndGet() == 100L) { paused.countDown(); Thread.sleep(400) }
                    counters.consumed.increment()
                }))
        }
        paused.await(10, TimeUnit.SECONDS)
        Thread.sleep(100)
        val a = counters.produced.sum()
        Thread.sleep(280)
        val b = counters.produced.sum()
        t.join()
        device.close()
        return mapOf("readDuringPause" to b - a, "aheadAtPause" to a - 100, "correct" to (failure() == null))
    }

    /** A source that yields one element every 5 ms: does each one reach the sink promptly, or wait for a batch? */
    fun slowProducer(c: Contestant): Map<String, Any?> {
        val counters = Counters()
        val device = WheelDevice(inline = false)
        val producedAt = LongArray(40)
        val worst = AtomicLong()
        val source = object : Iterator<Int> {
            var i = 0
            override fun hasNext() = i < 40
            override fun next(): Int {
                Thread.sleep(5)
                producedAt[i] = System.nanoTime()
                counters.produced.increment()
                return i++
            }
        }
        val r = ran(30_000) {
            c.run(Job(source, 16, DeviceIo(device, Workload(1_000), counters),
                sink = { y -> worst.accumulateAndGet(System.nanoTime() - producedAt[y], ::maxOf); counters.consumed.increment() }))
        }
        device.close()
        return mapOf("finished" to r.finished, "worstProducedToConsumedMs" to worst.get() / 1e6)
    }

    /** The caller is interrupted while every call is held: does the run stop, and are the calls cancelled? */
    fun interrupt(c: Contestant): Map<String, Any?> {
        val counters = Counters()
        val device = HeldDevice()
        val (t, failure) = start {
            c.run(Job(counted(1_000, counters), 32, DeviceIo(device, Workload(0), counters), sink = { counters.consumed.increment() }))
        }
        settleQuietly(counters)
        t.interrupt()
        val stopped = t.join(java.time.Duration.ofSeconds(2))
        Thread.sleep(200)
        val observed = mapOf(
            "callerReturned" to stopped, "heldCancelled" to device.cancelled(), "stillAwaited" to device.stillAwaited(),
            "produced" to counters.produced.sum(),
        )
        device.release()
        t.join()
        return observed + ("failure" to failure()?.let { it::class.simpleName })
    }

    val checks: Map<String, (Contestant) -> Map<String, Any?>> = linkedMapOf(
        "answers" to ::answers,
        "bound" to { c -> bound(c) },
        "mostInFlight" to { c -> mapOf("most" to mostInFlight(c), "bound" to 32) },
        "take" to ::take,
        "failure" to ::failure,
        "timeouts" to ::timeouts,
        "slowConsumer" to { c -> slowConsumer(c) },
        "demandPause" to { c -> demandPauseGrowth(c) },
        "slowProducer" to ::slowProducer,
        "interrupt" to ::interrupt,
    )
}

/** `semantics`: every check for every contestant, one `RESULT` line each. */
fun semanticsMain() {
    for (c in contestants.values) for ((check, f) in Semantics.checks) {
        if (c.model == Model.UNBOUNDED && check != "answers") continue
        emit(linkedMapOf<String, Any?>("kind" to "semantics", "check" to check, "contestant" to c.name, "model" to c.model) + f(c) + environment())
    }
}
