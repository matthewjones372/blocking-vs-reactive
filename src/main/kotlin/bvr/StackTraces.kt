package bvr

import io.github.matthewjones372.lark.stream.Exit
import io.github.matthewjones372.lark.stream.Forks
import io.github.matthewjones372.lark.stream.Stream
import io.github.matthewjones372.lark.stream.from
import io.github.matthewjones372.lark.stream.mapPar
import io.github.matthewjones372.lark.stream.run
import io.github.matthewjones372.lark.stream.runFold
import org.apache.pekko.stream.javadsl.Sink
import org.apache.pekko.stream.javadsl.Source
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.util.concurrent.Executors
import java.util.concurrent.Semaphore

/**
 * H6: a request handler loads ten orders, and the third one's I/O call fails. Prints what each contestant hands the
 * person on call: the stack trace of the exception the handler sees, with no debug agent or hook switched on.
 *
 * Each side fails the way its own clients fail. A blocking client (JDBC, a socket read, the JDK `HttpClient.send`)
 * throws on the caller's stack, so the blocking contestants sleep and throw. An async client fails on its event
 * loop and completes a future, so the reactive contestants use `FakeIo.failing`. What is compared is the exception
 * the handler catches, and whether its trace names `handleRequest` and `fetchOrder`.
 */
fun stackTraces() {
    val handlers = linkedMapOf<String, () -> Long>(
        "vt-plain" to ::handleRequestVt,
        "lark" to ::handleRequestLark,
        "reactor" to ::handleRequestReactor,
        "pekko" to ::handleRequestPekko,
    )
    for ((name, handler) in handlers) {
        println("===== $name")
        try {
            handler()
        } catch (t: Throwable) {
            val trace = java.io.StringWriter().also { t.printStackTrace(java.io.PrintWriter(it)) }.toString()
            println(trace.lines().take(40).joinToString("\n"))
            println("----- names handleRequest: ${"handleRequest" in trace}, names fetchOrder: ${"fetchOrder" in trace}, " +
                "lines: ${trace.lines().size}")
        }
    }
    System.exit(0)
}

private fun fetchOrderBlocking(id: Int): Int {
    Thread.sleep(java.time.Duration.ofNanos(100_000))
    if (id == 3) throw java.io.IOException("connection reset")
    return id
}
private fun fetchOrderAsync(id: Int) = if (id == 3) FakeIo.failing(100) else FakeIo.call(id, 100)

fun handleRequestVt(): Long {
    val permits = Semaphore(4)
    Executors.newVirtualThreadPerTaskExecutor().use { threads ->
        val orders = (1..10).map { id ->
            permits.acquire()
            threads.submit<Int> { try { fetchOrderBlocking(id) } finally { permits.release() } }
        }
        return orders.sumOf { it.get().toLong() }
    }
}

fun handleRequestLark(): Long {
    val exit = Stream.from(1..10).mapPar(4) { id -> fetchOrderBlocking(id) }.runFold(0L) { s, x -> s + x }
        .run(Forks()).toCompletableFuture().join()
    return when (exit) {
        is Exit.Done -> exit.value
        is Exit.Died -> throw exit.cause
        is Exit.Failed -> error(exit.error.toString())
    }
}

fun handleRequestReactor(): Long =
    Flux.range(1, 10).flatMap({ id -> Mono.fromFuture(fetchOrderAsync(id)) }, 4)
        .reduce(0L) { s, x -> s + x }.block()!!

fun handleRequestPekko(): Long =
    Source.range(1, 10).mapAsyncUnordered(4) { id -> fetchOrderAsync(id) }
        .runWith(Sink.fold(0L) { s, x -> s + x }, pekkoSystem).toCompletableFuture().join()
