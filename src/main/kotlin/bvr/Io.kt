package bvr

import org.apache.pekko.pattern.Patterns
import reactor.core.publisher.Mono
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.concurrent.ExecutionException
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.LongAdder

/**
 * Where an element is in its life: taken from the source ([produced]), handed to the I/O call ([started]), answered
 * by the device ([ioDone]), and taken by the sink ([consumed]). The differences are the queues between them:
 * produced − started is waiting for a permit, started − ioDone is pending I/O, ioDone − consumed is answered but not
 * yet consumed (an ordered window's buffer, a stream's internal queue, a callback not yet run).
 */
class Counters {
    val produced = LongAdder()
    val started = LongAdder()
    val ioDone = LongAdder()
    val consumed = LongAdder()
}

/** How each element's latency is chosen. Every choice is a function of the element alone, so every contestant sees
 *  the same latency for the same element. */
enum class Dist { FIXED, LOGNORMAL, BIMODAL }

/**
 * The work done per element.
 *
 * @param latencyMicros the I/O call's latency (the median, for [Dist.LOGNORMAL]).
 * @param failEvery every `failEvery`th element's call fails; 0 for none.
 * @param cpuIters iterations of a hash loop applied to each answer (about 1 ns each on the reference machine).
 * @param sinkIters iterations the single, serialised consumer spends per element, to make a slow consumer.
 */
data class Workload(
    val latencyMicros: Long,
    val dist: Dist = Dist.FIXED,
    val failEvery: Int = 0,
    val cpuIters: Int = 0,
    val sinkIters: Int = 0,
) {
    fun latencyNanos(x: Int): Long {
        val base = latencyMicros * 1_000
        return when (dist) {
            Dist.FIXED -> base
            Dist.BIMODAL -> if (mix(x) % 100 == 0L) base * 10 else base
            Dist.LOGNORMAL -> {
                // Box-Muller on two uniforms drawn from the element's hash: median `base`, sigma 0.8, so p99 is
                // about 6.4x the median. Capped at 20x so one draw cannot stall a run.
                val u1 = ((mix(x) ushr 11) + 1).toDouble() / (1L shl 53)
                val u2 = (mix(x + 0x5bd1e995) ushr 11).toDouble() / (1L shl 53)
                val z = kotlin.math.sqrt(-2 * kotlin.math.ln(u1)) * kotlin.math.cos(2 * Math.PI * u2)
                minOf((base * kotlin.math.exp(0.8 * z)).toLong(), base * 20)
            }
        }
    }

    fun fails(x: Int): Boolean = failEvery > 0 && x % failEvery == failEvery - 1

    /** The answer for element [x] after the CPU step; the identity on [x] whatever [cpuIters] is. */
    fun transform(x: Int): Int = if (cpuIters == 0) x else x + cpuWork(x, cpuIters)

    companion object {
        fun mix(x: Int): Long {
            var z = x.toLong() * -0x61c8864680b583ebL
            z = (z xor (z ushr 30)) * -0x40a7b892e31b1a47L
            z = (z xor (z ushr 27)) * -0x6b2fb644ecceee15L
            return (z xor (z ushr 31)) and Long.MAX_VALUE
        }
    }
}

@Volatile private var blackhole = 0L

/** [iters] rounds of xorshift seeded by [x]; returns 0, but only after doing the work the JIT cannot remove. */
fun cpuWork(x: Int, iters: Int): Int {
    var h = x.toLong() or 1
    repeat(iters) {
        h = h xor (h shl 13)
        h = h xor (h ushr 7)
        h = h xor (h shl 17)
    }
    if (h == 0x2545F4914F6CDD1DL) blackhole = h
    return 0
}

/**
 * The I/O call in each model's shape. [blocking] parks the calling thread until the answer, and an interrupt
 * stops the wait. [async] answers a future; [mono] and [pekko] are the same call as Reactor and Pekko would
 * consume it. Every form counts the call as started.
 */
interface Io {
    val workload: Workload
    val counters: Counters
    fun blocking(x: Int): Int
    fun async(x: Int): CompletableFuture<Int>
    fun mono(x: Int): Mono<Int> = Mono.fromFuture(async(x))
    fun pekko(x: Int): CompletionStage<Int> = async(x)

    /** [blocking] with a timeout: a call that has not answered by then is cancelled and fails with [IoTimeout]. */
    fun blockingWithin(x: Int, timeoutNanos: Long): Int {
        val f = async(x)
        return try {
            f.get(timeoutNanos, TimeUnit.NANOSECONDS)
        } catch (_: java.util.concurrent.TimeoutException) {
            f.cancel(true)
            throw IoTimeout(x)
        } catch (failed: ExecutionException) {
            throw failed.cause ?: failed
        }
    }
}

/** Every model waits on the one [device]: the blocking form parks on the same future the async forms compose. */
class DeviceIo(private val device: Device, override val workload: Workload, override val counters: Counters) : Io {
    init { device.counters = counters }

    override fun async(x: Int): CompletableFuture<Int> {
        counters.started.increment()
        return device.call(x, workload.latencyNanos(x), workload.fails(x))
    }

    override fun blocking(x: Int): Int = try {
        async(x).get()
    } catch (failed: ExecutionException) {
        throw failed.cause ?: failed
    }
}

/**
 * Mode C: each model waits on its own timer. A virtual thread sleeps; Reactor uses `Mono.delay` on its parallel
 * scheduler; Pekko uses its scheduler, which ticks every millisecond; the plain `CompletableFuture` and Lark
 * `mapAsync` forms, which have no timer of their own, use a JDK [ScheduledExecutorService] with a thread per core.
 * The answers are the same as on a device, but where continuations run differs by model, which is the point.
 * [lag] is measured at the moment each timer fires.
 */
class NativeIo(override val workload: Workload, override val counters: Counters) : Io {
    val lag = Histogram()

    private fun fired(x: Int, deadline: Long): Int {
        lag.record(System.nanoTime() - deadline)
        counters.ioDone.increment()
        if (workload.fails(x)) throw IoFailure(x)
        return x
    }

    override fun blocking(x: Int): Int {
        counters.started.increment()
        val latency = workload.latencyNanos(x)
        val deadline = System.nanoTime() + latency
        Thread.sleep(Duration.ofNanos(latency))
        return fired(x, deadline)
    }

    override fun async(x: Int): CompletableFuture<Int> {
        counters.started.increment()
        val latency = workload.latencyNanos(x)
        val deadline = System.nanoTime() + latency
        val f = CompletableFuture<Int>()
        jdkTimer.schedule({
            try { f.complete(fired(x, deadline)) } catch (e: IoFailure) { f.completeExceptionally(e) }
        }, latency, TimeUnit.NANOSECONDS)
        return f
    }

    override fun mono(x: Int): Mono<Int> {
        counters.started.increment()
        val latency = workload.latencyNanos(x)
        val deadline = System.nanoTime() + latency
        return Mono.delay(Duration.ofNanos(latency)).map { fired(x, deadline) }
    }

    override fun pekko(x: Int): CompletionStage<Int> {
        counters.started.increment()
        val latency = workload.latencyNanos(x)
        val deadline = System.nanoTime() + latency
        return Patterns.after(Duration.ofNanos(latency), pekkoSystem) {
            try { CompletableFuture.completedFuture(fired(x, deadline)) } catch (e: IoFailure) { CompletableFuture.failedFuture(e) }
        }
    }

    private companion object {
        val jdkTimer: ScheduledExecutorService =
            Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors()) { r ->
                Thread(r, "jdk-timer").apply { isDaemon = true }
            }
    }
}

/**
 * I/O that is already done: the blocking form returns at once and the async forms answer completed futures. With
 * it, what a contestant costs per element is its own machinery and nothing else (PerElementBenchmark).
 */
class ImmediateIo(override val counters: Counters = Counters()) : Io {
    override val workload = Workload(latencyMicros = 0)

    override fun blocking(x: Int): Int = x

    override fun async(x: Int): CompletableFuture<Int> = CompletableFuture.completedFuture(x)
}
