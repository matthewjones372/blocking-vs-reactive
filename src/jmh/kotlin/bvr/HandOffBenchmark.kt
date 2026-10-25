package bvr

import io.github.matthewjones372.lark.stream.Exit
import io.github.matthewjones372.lark.stream.Forks
import io.github.matthewjones372.lark.stream.Stream
import io.github.matthewjones372.lark.stream.buffer
import io.github.matthewjones372.lark.stream.from
import io.github.matthewjones372.lark.stream.run
import io.github.matthewjones372.lark.stream.runFold
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
import org.openjdk.jmh.annotations.TearDown
import reactor.core.publisher.Flux
import reactor.core.scheduler.Schedulers
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.TimeUnit

/**
 * H5: a million ints handed from a producer on one thread to a consumer on another, through a buffer of 256. The
 * cost is the hand-off itself: Reactor's lock-free queue against a blocking queue between virtual threads.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
// One operation takes up to a second for the slower contestants: longer iterations, so each sees enough of them.
@Warmup(iterations = 10, time = 2)
@Measurement(iterations = 8, time = 2)
open class HandOffBenchmark {
    private val n = 1_000_000
    private val single = Schedulers.newSingle("consumer")

    @TearDown
    fun tearDown() = single.dispose()

    @Benchmark
    fun vtQueue(): Long {
        val queue = ArrayBlockingQueue<Int>(256)
        val producer = Thread.ofVirtual().start {
            for (x in 0 until n) queue.put(x)
            queue.put(-1)
        }
        var sum = 0L
        val consumer = Thread.ofVirtual().start {
            while (true) {
                val x = queue.take()
                if (x < 0) break
                sum += x
            }
        }
        producer.join()
        consumer.join()
        return sum
    }

    @Benchmark
    fun lark(): Long {
        val exit = Stream.from(0 until n).buffer(256).runFold(0L) { s, x -> s + x }.run(Forks()).toCompletableFuture().join()
        return (exit as Exit.Done).value
    }

    @Benchmark
    fun reactor(): Long = Flux.range(0, n).publishOn(single, 256).reduce(0L) { s, x -> s + x }.block()!!

    @Benchmark
    fun pekko(): Long =
        // An async boundary whose downstream input buffer holds 256, as the other contestants' buffers do.
        Source.range(0, n - 1).async("pekko.actor.default-dispatcher", 256)
            .runWith(Sink.fold(0L) { s, x -> s + x }, pekkoSystem).toCompletableFuture().join()
}
