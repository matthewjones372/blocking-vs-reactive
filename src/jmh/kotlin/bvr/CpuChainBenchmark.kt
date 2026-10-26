package bvr

import io.github.matthewjones372.surefold.stream.Exit
import io.github.matthewjones372.surefold.stream.Forks
import io.github.matthewjones372.surefold.stream.Stream
import io.github.matthewjones372.surefold.stream.filter
import io.github.matthewjones372.surefold.stream.from
import io.github.matthewjones372.surefold.stream.map
import io.github.matthewjones372.surefold.stream.run
import io.github.matthewjones372.surefold.stream.runFold
import org.apache.pekko.stream.javadsl.Sink
import org.apache.pekko.stream.javadsl.Source
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.BenchmarkMode
import org.openjdk.jmh.annotations.Measurement
import org.openjdk.jmh.annotations.Mode
import org.openjdk.jmh.annotations.OutputTimeUnit
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.annotations.Warmup
import reactor.core.publisher.Flux
import java.util.concurrent.TimeUnit

/**
 * H4: map, filter, map and sum over a million ints, with no I/O and no thread boundary. This is where Reactor's
 * operator fusion should shine. The plain loop and `Sequence` are what blocking code would write instead of a stream.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
// One operation takes up to a second for the slower contestants: longer iterations, so each sees enough of them.
@Warmup(iterations = 10, time = 2)
@Measurement(iterations = 8, time = 2)
open class CpuChainBenchmark {
    private val n = 1_000_000

    @Benchmark
    fun loop(): Long {
        var sum = 0L
        for (x in 0 until n) {
            val y = x * 3
            if (y % 2 == 0) sum += y + 1
        }
        return sum
    }

    @Benchmark
    fun sequence(): Long = (0 until n).asSequence().map { it * 3 }.filter { it % 2 == 0 }.map { it + 1L }.sum()

    @Benchmark
    fun surefold(): Long {
        val exit = Stream.from(0 until n).map { it * 3 }.filter { it % 2 == 0 }.map { it + 1L }
            .runFold(0L) { s, x -> s + x }.run(Forks()).toCompletableFuture().join()
        return (exit as Exit.Done).value
    }

    @Benchmark
    fun reactor(): Long =
        Flux.range(0, n).map { it * 3 }.filter { it % 2 == 0 }.map { it + 1L }.reduce(0L) { s, x -> s + x }.block()!!

    @Benchmark
    fun pekko(): Long =
        Source.range(0, n - 1).map { it * 3 }.filter { it % 2 == 0 }.map { it + 1L }
            .runWith(Sink.fold(0L) { s, x -> s + x }, pekkoSystem).toCompletableFuture().join()
}
