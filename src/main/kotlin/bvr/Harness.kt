package bvr

import com.sun.management.OperatingSystemMXBean
import com.sun.management.ThreadMXBean
import java.io.File
import java.lang.management.ManagementFactory
import java.util.concurrent.locks.LockSupport

/** Just enough JSON for result lines: maps, lists, strings, numbers, booleans and null. NaN becomes null. */
object Json {
    fun write(value: Any?): String = StringBuilder().also { append(it, value) }.toString()

    private fun append(out: StringBuilder, value: Any?) {
        when (value) {
            null -> out.append("null")
            is String -> {
                out.append('"')
                for (c in value) when {
                    c == '"' -> out.append("\\\"")
                    c == '\\' -> out.append("\\\\")
                    c == '\n' -> out.append("\\n")
                    c < ' ' -> out.append("\\u%04x".format(c.code))
                    else -> out.append(c)
                }
                out.append('"')
            }
            is Double -> if (value.isNaN() || value.isInfinite()) out.append("null") else out.append(value)
            is Float -> append(out, value.toDouble())
            is Number, is Boolean -> out.append(value)
            is Enum<*> -> append(out, value.name.lowercase())
            is Map<*, *> -> {
                out.append('{')
                value.entries.forEachIndexed { i, (k, v) ->
                    if (i > 0) out.append(',')
                    append(out, k.toString())
                    out.append(':')
                    append(out, v)
                }
                out.append('}')
            }
            is Iterable<*> -> {
                out.append('[')
                value.forEachIndexed { i, v -> if (i > 0) out.append(','); append(out, v) }
                out.append(']')
            }
            else -> append(out, value.toString())
        }
    }
}

/** One measurement, printed as `RESULT {json}` for the scripts to collect. */
fun emit(fields: Map<String, Any?>) {
    println("RESULT " + Json.write(fields))
    System.out.flush()
}

/**
 * Samples the queues between [Counters] every millisecond on a platform thread, but only inside the steady window:
 * from the moment 10% of [items] have been consumed until 90% have. Ramp-up and drain are left out, so the means
 * describe the concurrency a contestant actually sustains. The hot path pays only for the counters' increments.
 */
class Sampler(private val c: Counters, private val items: Long) {
    var samples = 0L; private set
    private var pendingSum = 0.0
    private var bufferedSum = 0.0
    private var queuedSum = 0.0
    var pendingMax = 0L; private set
    var bufferedMax = 0L; private set
    var queuedMax = 0L; private set
    var outstandingMax = 0L; private set
    private var rssSum = 0.0
    private var rssSamples = 0
    var rssMaxMb = 0.0; private set
    private var t10 = 0L
    private var c10 = 0L
    private var t90 = 0L
    private var c90 = 0L

    @Volatile private var running = true

    private val thread = Thread.ofPlatform().daemon().name("sampler").start {
        val lo = items / 10
        val hi = items - items / 10
        while (running) {
            val consumed = c.consumed.sum()
            val ioDone = c.ioDone.sum()
            val started = c.started.sum()
            val produced = c.produced.sum()
            outstandingMax = maxOf(outstandingMax, produced - consumed)
            if (consumed >= lo && t10 == 0L) { t10 = System.nanoTime(); c10 = consumed }
            if (consumed >= hi && t90 == 0L) { t90 = System.nanoTime(); c90 = consumed }
            if (consumed in lo until hi) {
                val pending = started - ioDone
                val buffered = ioDone - consumed
                val queued = produced - started
                samples++
                pendingSum += pending; bufferedSum += buffered; queuedSum += queued
                pendingMax = maxOf(pendingMax, pending)
                bufferedMax = maxOf(bufferedMax, buffered)
                queuedMax = maxOf(queuedMax, queued)
                if (samples % 20 == 1L) {
                    val rss = Jvm.rssMb()
                    rssSum += rss; rssSamples++
                    rssMaxMb = maxOf(rssMaxMb, rss)
                }
            }
            LockSupport.parkNanos(1_000_000)
        }
    }

    fun stop() { running = false; thread.join() }

    val pendingMean get() = if (samples == 0L) Double.NaN else pendingSum / samples
    val bufferedMean get() = if (samples == 0L) Double.NaN else bufferedSum / samples
    val queuedMean get() = if (samples == 0L) Double.NaN else queuedSum / samples
    val rssMeanMb get() = if (rssSamples == 0) Double.NaN else rssSum / rssSamples

    /** Elements consumed per second between the 10% and 90% marks; NaN if the run was too short to see both. */
    val steadyOpsPerSec get() = if (t90 <= t10 || t10 == 0L) Double.NaN else (c90 - c10) / ((t90 - t10) / 1e9)
}

/** The JVM's own counters, read before and after a measured body. */
object Jvm {
    private val os = ManagementFactory.getOperatingSystemMXBean() as OperatingSystemMXBean
    private val threads = ManagementFactory.getThreadMXBean() as ThreadMXBean
    private val gcs = ManagementFactory.getGarbageCollectorMXBeans()
    private val jit = ManagementFactory.getCompilationMXBean()

    // G1 reports its concurrent cycles as a collector too; only the others are pauses.
    private fun pauses() = gcs.filter { "Concurrent" !in it.name }
    private fun concurrent() = gcs.filter { "Concurrent" in it.name }

    class Snapshot(
        val nanos: Long = System.nanoTime(),
        val cpuNanos: Long = os.processCpuTime,
        val allocated: Long = threads.totalThreadAllocatedBytes,
        val pauseCount: Long = pauses().sumOf { it.collectionCount },
        val pauseMs: Long = pauses().sumOf { it.collectionTime },
        val concurrentMs: Long = concurrent().sumOf { it.collectionTime },
        val jitMs: Long = jit.totalCompilationTime,
    )

    /** What happened between [a] and [b], per element where that is how it is read. */
    fun delta(a: Snapshot, b: Snapshot, items: Long): Map<String, Any?> {
        val wallMs = (b.nanos - a.nanos) / 1e6
        val cpuMs = (b.cpuNanos - a.cpuNanos) / 1e6
        return linkedMapOf(
            "wallMs" to wallMs,
            "opsPerSec" to items / (wallMs / 1000),
            "cpuMs" to cpuMs,
            "cpuUsPerOp" to cpuMs * 1000 / items,
            "cpuUtilisation" to cpuMs / wallMs / Runtime.getRuntime().availableProcessors(),
            "allocBytesPerOp" to (b.allocated - a.allocated).toDouble() / items,
            "allocMbPerSec" to (b.allocated - a.allocated) / 1e6 / (wallMs / 1000),
            "gcPauses" to b.pauseCount - a.pauseCount,
            "gcPauseMs" to b.pauseMs - a.pauseMs,
            "gcConcurrentMs" to b.concurrentMs - a.concurrentMs,
            "jitMs" to b.jitMs - a.jitMs,
        )
    }

    private const val PAGE_KB = 4L

    /** Resident set size now, from /proc; NaN where there is no /proc. */
    fun rssMb(): Double = try {
        File("/proc/self/statm").readText().trim().split(" ")[1].toLong() * PAGE_KB / 1024.0
    } catch (_: Exception) {
        Double.NaN
    }

    /** The process's peak resident set size so far. */
    fun rssHighWaterMb(): Double = try {
        File("/proc/self/status").readLines().first { it.startsWith("VmHWM:") }.split(Regex("\\s+"))[1].toDouble() / 1024
    } catch (_: Exception) {
        Double.NaN
    }

    /**
     * A fixed amount of single-threaded CPU work, timed: ns per thousand rounds of the [cpuWork] loop. Printed at
     * the start of every JVM so the summary can show how much the machine itself drifted during a suite.
     */
    fun calibrate(): Double {
        repeat(5) { cpuWork(it, 2_000_000) }
        val runs = (1..5).map {
            val t0 = System.nanoTime()
            cpuWork(it, 10_000_000)
            (System.nanoTime() - t0) / 10_000.0
        }
        return runs.sorted()[2]
    }
}

/** What every result line carries so the summary can refuse to mix runs from different builds or machines. */
fun environment(): Map<String, Any?> = linkedMapOf(
    "jdk" to System.getProperty("java.vm.version"),
    "cpus" to Runtime.getRuntime().availableProcessors(),
    "maxHeapMb" to Runtime.getRuntime().maxMemory() / (1 shl 20),
    "jvmArgs" to ManagementFactory.getRuntimeMXBean().inputArguments
        .filter { it.startsWith("-X") && !it.startsWith("-Xlog") }.joinToString(" "),
    "commit" to System.getProperty("bvr.commit"),
    "surefoldVersion" to System.getProperty("bvr.surefoldVersion"),
    "run" to System.getProperty("bvr.run"),
    "fork" to System.getProperty("bvr.fork")?.toIntOrNull(),
    "order" to System.getProperty("bvr.order")?.toIntOrNull(),
)

/** Runs [body] on a fresh virtual thread and waits: every contestant is called the way a request handler would call it. */
fun <T> onVirtualThread(body: () -> T): T {
    var result: Result<T>? = null
    Thread.ofVirtual().name("caller").start { result = runCatching(body) }.join()
    return result!!.getOrThrow()
}
