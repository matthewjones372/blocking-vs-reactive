package bvr

import io.github.matthewjones372.surefold.await
import io.github.matthewjones372.surefold.stream.Forks
import io.github.matthewjones372.surefold.stream.Stream
import io.github.matthewjones372.surefold.stream.buffer
import io.github.matthewjones372.surefold.stream.from
import io.github.matthewjones372.surefold.stream.run
import io.github.matthewjones372.surefold.stream.runFold
import org.apache.pekko.stream.javadsl.Sink
import org.apache.pekko.stream.javadsl.Source
import reactor.core.publisher.Flux
import reactor.core.scheduler.Schedulers
import java.util.concurrent.ArrayBlockingQueue

/**
 * Does HandOffBenchmark's hand-off cross threads? Each contestant is run as the benchmark runs it, a million ints
 * through a buffer of 256, but the source records the thread that produced each element and the consumer counts the
 * elements produced on its own thread. A hand-off that mostly stays on one thread measures something else.
 * `handoff-threads` prints one `RESULT` per contestant.
 */
private const val N = 1_000_000

private fun recording(producers: LongArray): Iterable<Int> = Iterable {
    object : Iterator<Int> {
        var i = 0
        override fun hasNext() = i < N
        override fun next(): Int { producers[i] = Thread.currentThread().threadId(); return i++ }
    }
}

private class Consumer(private val producers: LongArray) {
    var sameThread = 0L
    fun take(x: Int) { if (producers[x] == Thread.currentThread().threadId()) sameThread++ }
}

fun handOffThreads() {
    val runs = linkedMapOf<String, (LongArray, Consumer) -> Unit>(
        "reactor publishOn" to { p, c ->
            val single = Schedulers.newSingle("consumer")
            Flux.fromIterable(recording(p)).publishOn(single, 256).doOnNext(c::take).then().block()
            single.dispose()
        },
        "surefold buffer" to { p, c ->
            Stream.from(recording(p)).buffer(256).runFold(0L) { n, x -> c.take(x); n + 1 }.run(Forks()).await()
        },
        "pekko async boundary" to { p, c ->
            Source.fromIterator { recording(p).iterator() }.async("pekko.actor.default-dispatcher", 256)
                .runWith(Sink.foreach(c::take), pekkoSystem).toCompletableFuture().get()
        },
        "virtual threads + ArrayBlockingQueue" to { p, c ->
            val queue = ArrayBlockingQueue<Int>(256)
            val producer = Thread.ofVirtual().start { for (x in recording(p)) queue.put(x); queue.put(-1) }
            val consumer = Thread.ofVirtual().start { while (true) { val x = queue.take(); if (x < 0) break; c.take(x) } }
            producer.join(); consumer.join()
        },
    )
    for ((name, run) in runs) {
        repeat(3) { run(LongArray(N), Consumer(LongArray(N).also { it.fill(-1) })) } // warm-up
        val producers = LongArray(N)
        val consumer = Consumer(producers)
        run(producers, consumer)
        emit(linkedMapOf<String, Any?>("kind" to "handoff-threads", "contestant" to name, "elements" to N,
            "producedOnConsumerThread" to consumer.sameThread, "fraction" to consumer.sameThread.toDouble() / N) + environment())
    }
}
