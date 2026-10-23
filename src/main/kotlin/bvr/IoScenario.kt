package bvr

import io.github.matthewjones372.lark.stream.Exit
import io.github.matthewjones372.lark.stream.Forks
import io.github.matthewjones372.lark.stream.Stream
import io.github.matthewjones372.lark.stream.from
import io.github.matthewjones372.lark.stream.mapAsync
import io.github.matthewjones372.lark.stream.mapPar
import io.github.matthewjones372.lark.stream.run
import io.github.matthewjones372.lark.stream.runFold
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.javadsl.Sink
import org.apache.pekko.stream.javadsl.Source
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import com.typesafe.config.ConfigFactory
import org.apache.pekko.pattern.Patterns
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.concurrent.Executors
import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.LongAdder

/**
 * `items` elements from a source as fast as it can make them, each sent through one I/O call of `latencyMicros`, with
 * at most `concurrency` calls in flight, and the answers summed. The sum is returned so nothing is optimised away.
 *
 * Each contestant is written the way its own documentation writes it. The code between the `region` markers is
 * what the line counts in the README count.
 */
data class IoParams(val items: Int, val concurrency: Int, val latencyMicros: Long, val native: Boolean = false)

/*
 * The I/O call, in each model's own shape. With `native`, each model waits on its own timer: a virtual thread sleeps,
 * Reactor uses `Mono.delay` on its parallel scheduler, Pekko uses its scheduler (ticking every millisecond). Without it,
 * every model waits on the one shared [FakeIo] device, whose completions run on the device's own two threads, as a
 * socket's would on a poller or an event loop.
 */
fun blockingIo(x: Int, p: IoParams): Int =
    if (p.native) { Thread.sleep(Duration.ofNanos(p.latencyMicros * 1_000)); x } else FakeIo.call(x, p.latencyMicros).join()

fun monoIo(x: Int, p: IoParams): Mono<Int> =
    if (p.native) Mono.delay(Duration.ofNanos(p.latencyMicros * 1_000)).thenReturn(x)
    else Mono.fromFuture(FakeIo.call(x, p.latencyMicros))

fun stageIo(x: Int, p: IoParams): CompletionStage<Int> =
    if (p.native) Patterns.after(Duration.ofNanos(p.latencyMicros * 1_000), pekkoSystem) { CompletableFuture.completedFuture(x) }
    else FakeIo.call(x, p.latencyMicros)

fun interface IoContestant {
    fun run(p: IoParams, probe: Probe): Long
}

/** A source that counts what it hands out, so a contestant's backpressure is measured where it pulls. */
fun counted(n: Int, probe: Probe): Iterator<Int> = object : Iterator<Int> {
    var i = 0
    override fun hasNext() = i < n
    override fun next(): Int { probe.produced(); return i++ }
}

// region vt-plain
fun vtPlain(p: IoParams, probe: Probe): Long {
    val permits = Semaphore(p.concurrency)
    val sum = LongAdder()
    Executors.newVirtualThreadPerTaskExecutor().use { threads ->
        for (x in counted(p.items, probe)) {
            permits.acquire()
            threads.submit {
                try {
                    sum.add(blockingIo(x, p).toLong())
                    probe.completed()
                } finally {
                    permits.release()
                }
            }
        }
    }
    return sum.sum()
}
// endregion

/** [vtPlain] with its producer loop on a virtual thread too, as it would be inside a virtual-thread server. */
fun vtPlainOnVt(p: IoParams, probe: Probe): Long {
    var result = 0L
    Thread.ofVirtual().start { result = vtPlain(p, probe) }.join()
    return result
}

// region vt-naive
fun vtNaive(p: IoParams, probe: Probe): Long {
    val sum = LongAdder()
    Executors.newVirtualThreadPerTaskExecutor().use { threads ->
        for (x in counted(p.items, probe)) {
            threads.submit {
                sum.add(blockingIo(x, p).toLong())
                probe.completed()
            }
        }
    }
    return sum.sum()
}
// endregion

// region lark
fun lark(p: IoParams, probe: Probe): Long {
    val exit = Stream.from(Iterable { counted(p.items, probe) })
        .mapPar(p.concurrency) { x -> blockingIo(x, p) }
        .runFold(0L) { sum, x -> probe.completed(); sum + x }
        .run(Forks())
        .toCompletableFuture().join()
    return (exit as Exit.Done).value
}
// endregion

// region lark-async
fun larkAsync(p: IoParams, probe: Probe): Long {
    val exit = Stream.from(Iterable { counted(p.items, probe) })
        .mapAsync(p.concurrency) { x -> stageIo(x, p) }
        .runFold(0L) { sum, x -> probe.completed(); sum + x }
        .run(Forks())
        .toCompletableFuture().join()
    return (exit as Exit.Done).value
}
// endregion

// region reactor
fun reactor(p: IoParams, probe: Probe): Long =
    Flux.range(0, p.items)
        .doOnNext { probe.produced() }
        .flatMap({ x -> monoIo(x, p) }, p.concurrency)
        .reduce(0L) { sum, x -> probe.completed(); sum + x }
        .block()!!
// endregion

val pekkoSystem: ActorSystem by lazy {
    ActorSystem.create("bvr", ConfigFactory.parseString("pekko.scheduler.tick-duration = 1ms").withFallback(ConfigFactory.load()))
}

// region pekko
fun pekko(p: IoParams, probe: Probe): Long =
    Source.fromIterator { counted(p.items, probe) }
        .mapAsyncUnordered(p.concurrency) { x -> stageIo(x, p) }
        .runWith(Sink.fold(0L) { sum, x -> probe.completed(); sum + x }, pekkoSystem)
        .toCompletableFuture().join()
// endregion

val ioContestants: Map<String, IoContestant> = linkedMapOf(
    "vt-plain" to IoContestant(::vtPlain),
    "vt-plain-vt" to IoContestant(::vtPlainOnVt),
    "vt-naive" to IoContestant(::vtNaive),
    "lark" to IoContestant(::lark),
    "lark-async" to IoContestant(::larkAsync),
    "reactor" to IoContestant(::reactor),
    "pekko" to IoContestant(::pekko),
)
