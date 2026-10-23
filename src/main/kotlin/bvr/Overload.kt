package bvr

import io.github.matthewjones372.lark.Limit
import io.github.matthewjones372.lark.stream.Exit
import io.github.matthewjones372.lark.stream.Forks
import io.github.matthewjones372.lark.stream.Stream
import io.github.matthewjones372.lark.stream.from
import io.github.matthewjones372.lark.stream.mapPar
import io.github.matthewjones372.lark.stream.run
import io.github.matthewjones372.lark.stream.runFold
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * A dependency that serves [capacity] calls at once, each in [serviceMicros]; a call beyond that waits in the
 * dependency's own queue, as a database's connection pool or a thread-per-request server's backlog makes it wait.
 * The capacity can drop mid-run, as it does when a replica fails. Each call's latency, queue included, is what
 * the dependency's callers' timeouts would see, and is recorded.
 */
class Dependency(@Volatile var capacity: Int, private val serviceMicros: Long, calls: Int) {
    private val timer = ScheduledThreadPoolExecutor(2) { r -> Thread(r, "dependency").apply { isDaemon = true } }
    private val waiting = ConcurrentLinkedQueue<Pending>()
    private val serving = AtomicInteger()
    val latencies = LongArray(calls)
    private val recorded = AtomicInteger()

    private class Pending(val x: Int, val arrived: Long, val answer: CompletableFuture<Int>)

    fun call(x: Int): CompletableFuture<Int> {
        val pending = Pending(x, System.nanoTime(), CompletableFuture())
        waiting.offer(pending)
        admit()
        return pending.answer
    }

    private fun admit() {
        while (true) {
            val now = serving.get()
            if (now >= capacity || waiting.isEmpty()) return
            if (!serving.compareAndSet(now, now + 1)) continue
            val next = waiting.poll()
            if (next == null) {
                serving.decrementAndGet()
                return
            }
            timer.schedule({ finish(next) }, serviceMicros, TimeUnit.MICROSECONDS)
        }
    }

    private fun finish(done: Pending) {
        val i = recorded.getAndIncrement()
        if (i < latencies.size) latencies[i] = System.nanoTime() - done.arrived
        serving.decrementAndGet()
        admit()
        done.answer.complete(done.x)
    }

    /** Latency percentiles in ms over calls [from] until [until], in the order they finished. */
    fun percentiles(from: Int, until: Int): Pair<Double, Double> {
        val sorted = latencies.copyOfRange(from, minOf(until, recorded.get())).sorted()
        fun at(q: Double) = sorted[((sorted.size - 1) * q).toInt()] / 1e6
        return at(0.50) to at(0.99)
    }
}

data class OverloadParams(val items: Int, val capacity: Int, val droppedTo: Int, val serviceMicros: Long)

/** Drops the dependency's capacity once half of the calls have finished. */
private fun dropHalfway(p: OverloadParams, dependency: Dependency, finished: AtomicInteger) {
    if (finished.incrementAndGet() == p.items / 2) dependency.capacity = p.droppedTo
}

// region overload-reactor
fun overloadReactor(p: OverloadParams, dependency: Dependency, concurrency: Int): Long {
    val finished = AtomicInteger()
    return Flux.range(0, p.items)
        .flatMap({ x -> Mono.fromFuture(dependency.call(x)) }, concurrency)
        .doOnNext { dropHalfway(p, dependency, finished) }
        .reduce(0L) { sum, x -> sum + x }
        .block()!!
}
// endregion

// region overload-lark
fun overloadLark(p: OverloadParams, dependency: Dependency, limit: Limit): Long {
    val finished = AtomicInteger()
    val exit = Stream.from(0 until p.items)
        .mapPar(limit) { x -> dependency.call(x).join() }
        .runFold(0L) { sum, x -> dropHalfway(p, dependency, finished).let { sum + x } }
        .run(Forks())
        .toCompletableFuture().join()
    return (exit as Exit.Done).value
}
// endregion

val overloadContestants: Map<String, (OverloadParams, Dependency) -> Long> = linkedMapOf(
    "reactor-1000" to { p, d -> overloadReactor(p, d, 1_000) },
    "reactor-tuned" to { p, d -> overloadReactor(p, d, p.capacity) },
    "lark-1000" to { p, d -> overloadLark(p, d, Limit.fixed(1_000)) },
    "lark-adaptive" to { p, d -> overloadLark(p, d, Limit.adaptive("dependency", initial = 20, max = 1_000).also(::trace)) },
)

/** With -Dbvr.trace, prints the limit every 250 ms, to see how it moves. */
private fun trace(limit: Limit) {
    if (System.getProperty("bvr.trace") == null) return
    Thread.ofPlatform().daemon().start {
        while (true) {
            System.err.println("limit=${limit.current} inside=${limit.inFlight}")
            Thread.sleep(250)
        }
    }
}

/** `Main overload <contestant>`: one run, with the capacity dropping by 5x halfway, in a fresh JVM. */
fun overload(name: String) {
    val p = OverloadParams(items = 200_000, capacity = 500, droppedTo = 100, serviceMicros = 5_000)
    val contestant = overloadContestants.getValue(name)
    // A warm-up at full capacity throughout, so the JIT has seen every path before the measured run.
    contestant(p.copy(items = 50_000, droppedTo = p.capacity), Dependency(p.capacity, p.serviceMicros, 50_000))
    val dependency = Dependency(p.capacity, p.serviceMicros, p.items)
    val started = System.nanoTime()
    contestant(p, dependency)
    val seconds = (System.nanoTime() - started) / 1e9
    val (before50, before99) = dependency.percentiles(0, p.items / 2)
    val (after50, after99) = dependency.percentiles(p.items / 2 + p.capacity, p.items)
    println(
        "RESULT scenario=overload contestant=$name seconds=%.2f opsPerSec=%.0f p50BeforeMs=%.1f p99BeforeMs=%.1f p50AfterMs=%.1f p99AfterMs=%.1f"
            .format(seconds, p.items / seconds, before50, before99, after50, after99),
    )
}
