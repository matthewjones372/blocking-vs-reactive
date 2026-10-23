package bvr

import com.sun.management.OperatingSystemMXBean
import com.sun.management.ThreadMXBean
import java.lang.management.ManagementFactory
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

/**
 * The one I/O device every contestant talks to: a call answers a future that a shared timer completes after
 * `latency`. Reactive contestants compose the future; virtual-thread contestants `join()` it and park. The device
 * therefore costs everyone the same, and only the concurrency model around it differs.
 */
object FakeIo {
    private val timer = ScheduledThreadPoolExecutor(2) { r -> Thread(r, "fake-io").apply { isDaemon = true } }

    fun call(x: Int, latencyMicros: Long): CompletableFuture<Int> {
        val f = CompletableFuture<Int>()
        timer.schedule({ f.complete(x) }, latencyMicros, TimeUnit.MICROSECONDS)
        return f
    }

    fun failing(latencyMicros: Long): CompletableFuture<Int> {
        val f = CompletableFuture<Int>()
        timer.schedule({ f.completeExceptionally(IllegalStateException("connection reset")) }, latencyMicros, TimeUnit.MICROSECONDS)
        return f
    }
}

/**
 * What a contestant reports while it runs: elements taken from the source, and elements that reached the sink.
 * A platform thread samples the difference every millisecond, so the hot path pays for two increments and no CAS loop.
 */
class Probe {
    private val produced = AtomicLong()
    private val completed = AtomicLong()
    @Volatile var maxOutstanding = 0L
        private set
    @Volatile private var running = true

    fun produced() { produced.incrementAndGet() }
    fun completed() { completed.incrementAndGet() }
    fun outstanding() = produced.get() - completed.get()

    private val sampler = Thread.ofPlatform().daemon().start {
        while (running) {
            val o = outstanding()
            if (o > maxOutstanding) maxOutstanding = o
            Thread.sleep(1)
        }
    }

    fun stop() { running = false; sampler.join() }
}

data class Measurement(
    val wallMs: Double,
    val cpuMs: Double,
    val allocatedMb: Double,
    val gcCount: Long,
    val gcMs: Long,
    val maxOutstanding: Long,
)

object Jvm {
    private val os = ManagementFactory.getOperatingSystemMXBean() as OperatingSystemMXBean
    private val threads = ManagementFactory.getThreadMXBean() as ThreadMXBean
    private val gcs = ManagementFactory.getGarbageCollectorMXBeans()
    private val memory = ManagementFactory.getMemoryMXBean()

    fun measure(body: (Probe) -> Unit): Measurement {
        val probe = Probe()
        val gcCount0 = gcs.sumOf { it.collectionCount }
        val gcMs0 = gcs.sumOf { it.collectionTime }
        val alloc0 = threads.totalThreadAllocatedBytes
        val cpu0 = os.processCpuTime
        val t0 = System.nanoTime()
        body(probe)
        val wall = System.nanoTime() - t0
        val cpu = os.processCpuTime - cpu0
        val alloc = threads.totalThreadAllocatedBytes - alloc0
        probe.stop()
        return Measurement(
            wallMs = wall / 1e6,
            cpuMs = cpu / 1e6,
            allocatedMb = alloc / 1e6,
            gcCount = gcs.sumOf { it.collectionCount } - gcCount0,
            gcMs = gcs.sumOf { it.collectionTime } - gcMs0,
            maxOutstanding = probe.maxOutstanding,
        )
    }

    /**
     * Live heap held per element in flight: the heap after a full GC taken while at least [inFlight] * 0.9
     * elements are outstanding, less the heap after a full GC before the run started.
     */
    fun liveBytesPerInFlight(inFlight: Int, body: (Probe) -> Unit): Double {
        System.gc(); System.gc()
        val before = memory.heapMemoryUsage.used
        val probe = Probe()
        var during = -1L
        var seen = 0L
        val watcher = Thread.ofPlatform().start {
            val deadline = System.nanoTime() + 10_000_000_000
            while (System.nanoTime() < deadline) {
                val o = probe.outstanding()
                if (o >= inFlight * 9L / 10) {
                    System.gc()
                    during = memory.heapMemoryUsage.used
                    seen = o
                    return@start
                }
                Thread.onSpinWait()
            }
        }
        body(probe)
        watcher.join()
        probe.stop()
        return if (during < 0) Double.NaN else (during - before).toDouble() / seen
    }

    fun rssHighWaterMb(): Double =
        java.io.File("/proc/self/status").readLines().first { it.startsWith("VmHWM:") }
            .split(Regex("\\s+"))[1].toDouble() / 1024
}
