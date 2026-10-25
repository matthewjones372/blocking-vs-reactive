package bvr

import java.util.concurrent.atomic.LongAdder

/**
 * An I/O scenario: [items] elements, each through one call of [workload], at most [concurrency] in flight, on
 * [device] (`shared`, `dispatched` or `native`; see Device.kt and Io.kt).
 */
data class IoScenario(
    val name: String,
    val device: String,
    val items: Int,
    val concurrency: Int,
    val workload: Workload,
    val recover: Boolean = false,
    val contestants: List<String>,
) {
    /** The sum every contestant must reach: each element answers itself, or 0 if its call failed and was recovered. */
    val expectedSum: Long get() = (0 until items).sumOf { if (workload.fails(it)) 0L else it.toLong() }
}

/** A source of 0 until [n] that counts what it hands out, so backpressure is measured where a contestant pulls. */
fun counted(n: Int, counters: Counters): Iterator<Int> = object : Iterator<Int> {
    var i = 0
    override fun hasNext() = i < n
    override fun next(): Int {
        if (i >= n) throw NoSuchElementException()
        counters.produced.increment()
        return i++
    }
}

/** The I/O for one iteration, and a way to read the timer's lag afterwards. */
class IoSetup(val io: Io, val lag: Histogram, val dispatchLag: Histogram?, val firedAfterCancel: () -> Long, val close: () -> Unit)

fun ioSetup(device: String, workload: Workload, counters: Counters): IoSetup = when (device) {
    "native" -> NativeIo(workload, counters).let { IoSetup(it, it.lag, null, { 0 }, {}) }
    "shared" -> SharedDevice().let { d -> IoSetup(DeviceIo(d, workload, counters), d.lag, null, { d.firedAfterCancel.sum() }, d::close) }
    "dispatched", "wheel" -> WheelDevice(inline = device == "wheel").let { d ->
        IoSetup(DeviceIo(d, workload, counters), d.lag, d.dispatchLag.takeIf { device == "dispatched" },
            { d.firedAfterCancel.sum() }, d::close)
    }
    else -> error("no device $device")
}

/** The sink every I/O scenario uses: sums the answers, and with `sinkIters` is one slow consumer at a time. */
fun summingSink(workload: Workload, sum: LongAdder, counters: Counters): (Int) -> Unit {
    if (workload.sinkIters == 0) return { y -> sum.add(y.toLong()); counters.consumed.increment() }
    val lock = Any()
    return { y ->
        synchronized(lock) { cpuWork(y, workload.sinkIters) }
        sum.add(y.toLong())
        counters.consumed.increment()
    }
}

private fun us(nanos: Double) = nanos / 1000

/** One iteration of [s] for [c], measured, checked against the expected answer, and printed. */
fun ioIteration(s: IoScenario, c: Contestant, iteration: Int, warmup: Boolean, sampling: Boolean = true): Boolean {
    val counters = Counters()
    val setup = ioSetup(s.device, s.workload, counters)
    val sum = LongAdder()
    val job = Job(counted(s.items, counters), s.concurrency, setup.io, s.recover, sink = summingSink(s.workload, sum, counters))
    if (!warmup) { System.gc(); System.gc() }
    val sampler = if (sampling) Sampler(counters, s.items.toLong()) else null
    val before = Jvm.Snapshot()
    val failure = runCatching { onVirtualThread { c.run(job) } }.exceptionOrNull()
    val after = Jvm.Snapshot()
    sampler?.stop()
    setup.close()
    val valid = failure == null && sum.sum() == s.expectedSum && counters.consumed.sum() == s.items.toLong()
    val m = Jvm.delta(before, after, s.items.toLong())
    val steady = sampler?.steadyOpsPerSec ?: Double.NaN
    emit(
        linkedMapOf<String, Any?>(
            "kind" to "io", "scenario" to s.name, "contestant" to c.name, "model" to c.model, "ordered" to c.ordered,
            "library" to c.library, "device" to s.device, "items" to s.items, "concurrency" to s.concurrency,
            "latencyMicros" to s.workload.latencyMicros, "dist" to s.workload.dist, "failEvery" to s.workload.failEvery,
            "cpuIters" to s.workload.cpuIters, "sinkIters" to s.workload.sinkIters,
            "iteration" to iteration, "warmup" to warmup, "valid" to valid,
            "error" to failure?.let { "${it::class.simpleName}: ${it.message}" },
        ) + m + linkedMapOf(
            "steadyOpsPerSec" to steady,
            "pendingMean" to sampler?.pendingMean, "pendingMax" to sampler?.pendingMax,
            "bufferedMean" to sampler?.bufferedMean, "bufferedMax" to sampler?.bufferedMax,
            "queuedMean" to sampler?.queuedMean, "queuedMax" to sampler?.queuedMax,
            "outstandingMax" to sampler?.outstandingMax,
            // Little's law: what the mean latency must have been for this many pending calls to give this rate.
            "impliedLatencyMs" to sampler?.let { it.pendingMean / steady * 1000 },
            "lagP50Us" to us(setup.lag.quantile(0.5)), "lagP99Us" to us(setup.lag.quantile(0.99)),
            "lagMaxUs" to us(setup.lag.max()),
            "dispatchLagP99Us" to setup.dispatchLag?.let { us(it.quantile(0.99)) },
            "firedAfterCancel" to setup.firedAfterCancel(),
            "rssMeanMb" to sampler?.rssMeanMb, "rssMaxMb" to sampler?.rssMaxMb,
        ) + environment(),
    )
    return valid
}

/** `io <scenario> <contestant> [warmups] [iterations]`: one contestant in one fresh JVM. */
fun ioMain(args: List<String>) {
    val s = ioScenarios.getValue(args[0])
    val c = contestants.getValue(args[1])
    val warmups = args.getOrNull(2)?.toInt() ?: 2
    val iterations = args.getOrNull(3)?.toInt() ?: 3
    val sampling = System.getProperty("bvr.sampler") != "off"
    emit(linkedMapOf<String, Any?>("kind" to "calibration", "scenario" to s.name, "contestant" to c.name,
        "nsPerKiloRounds" to Jvm.calibrate()) + environment())
    var ok = true
    repeat(warmups) { ok = ioIteration(s, c, it, warmup = true, sampling) && ok }
    repeat(iterations) { ok = ioIteration(s, c, it, warmup = false, sampling) && ok }
    emit(linkedMapOf<String, Any?>("kind" to "rss", "scenario" to s.name, "contestant" to c.name,
        "rssHighWaterMb" to Jvm.rssHighWaterMb()) + environment())
    if (!ok) System.err.println("INVALID: ${s.name} ${c.name} did not produce the expected answer")
    System.exit(if (ok) 0 else 3)
}
