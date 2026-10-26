package bvr

import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.BenchmarkMode
import org.openjdk.jmh.annotations.Measurement
import org.openjdk.jmh.annotations.Mode
import org.openjdk.jmh.annotations.OperationsPerInvocation
import org.openjdk.jmh.annotations.OutputTimeUnit
import org.openjdk.jmh.annotations.Param
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.annotations.Warmup
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.LongAdder

/**
 * What each contestant costs per element when the I/O costs nothing: 100,000 elements through the contestant's own
 * code (Contestants.kt), bound 64, against [ImmediateIo]. The blocking contestants still start a virtual thread per
 * element; the async ones still compose a future per element. No device, no timer, no waiting: the difference
 * between contestants here is their machinery, which the I/O scenarios add to whatever the I/O costs.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 8, time = 1)
open class PerElementBenchmark {
    @Param(
        "vt-plain", "vt-ordered", "vt-structured", "surefold-mappar", "surefold-mappar-unordered", "cf-plain", "surefold-mapasync",
        "surefold-mapasync-unordered", "reactor-flatmap", "reactor-flatmapsequential", "pekko-mapasync", "pekko-mapasync-unordered",
    )
    lateinit var contestant: String

    @Benchmark
    @OperationsPerInvocation(N)
    fun element(): Long {
        val counters = Counters()
        val sum = LongAdder()
        val c = contestants.getValue(contestant)
        onVirtualThread { c.run(Job(counted(N, counters), 64, ImmediateIo(counters), sink = { y -> sum.add(y.toLong()) })) }
        return sum.sum()
    }

    private companion object {
        const val N = 100_000
    }
}
