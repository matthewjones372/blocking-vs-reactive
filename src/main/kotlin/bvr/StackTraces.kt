package bvr

import com.sun.management.HotSpotDiagnosticMXBean
import io.github.matthewjones372.lark.stream.Exit
import io.github.matthewjones372.lark.stream.Forks
import io.github.matthewjones372.lark.stream.Stream
import io.github.matthewjones372.lark.stream.from
import io.github.matthewjones372.lark.stream.mapAsync
import io.github.matthewjones372.lark.stream.mapPar
import io.github.matthewjones372.lark.stream.run
import io.github.matthewjones372.lark.stream.runFold
import org.apache.pekko.stream.javadsl.Sink
import org.apache.pekko.stream.javadsl.Source
import reactor.core.publisher.Flux
import reactor.core.publisher.Hooks
import reactor.core.publisher.Mono
import java.lang.management.ManagementFactory
import java.nio.file.Files
import java.util.concurrent.Callable
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionException
import java.util.concurrent.Executors
import java.util.concurrent.Semaphore
import java.util.concurrent.StructuredTaskScope
import java.util.concurrent.atomic.LongAdder

/**
 * A request handler loads ten orders, four at a time, and the third order's fetch fails. What is compared is the
 * exception the handler catches: whether its trace names `handleRequest` (the caller) and `fetchOrder` (the call
 * that failed), and how long it is.
 *
 * Each side fails the way its own clients fail. A blocking client (JDBC, a socket read, `HttpClient.send`) throws
 * on the caller's stack, so the blocking variants throw from [fetchOrderBlocking]. An async client fails on its
 * event loop and completes a future there, so the async variants fail on a device thread ([fetchOrderAsync]).
 * Reactor is shown three ways: by default, with `checkpoint()`, and with `Hooks.onOperatorDebug()`. Reactor's
 * `ReactorDebugAgent` (reactor-tools) gives assembly traces at lower cost than the hook; it is not run here because
 * it needs a further dependency, and the hook shows what assembly tracing adds.
 */
private val failingDevice = WheelDevice(inline = false)

private fun fetchOrderBlocking(id: Int): Int {
    Thread.sleep(java.time.Duration.ofNanos(100_000))
    if (id == 3) throw java.io.IOException("connection reset fetching order $id")
    return id
}

private fun fetchOrderAsync(id: Int): CompletableFuture<Int> = failingDevice.call(id, 100_000, fails = id == 3)

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

fun handleRequestStructured(): Long {
    val permits = Semaphore(4)
    val total = LongAdder()
    val joiner = StopOnFailureOrTake()
    StructuredTaskScope.open(joiner).use { scope ->
        for (id in 1..10) {
            if (!permits.acquireUnless { joiner.stopped }) break
            scope.fork(Callable { try { total.add(fetchOrderBlocking(id).toLong()) } finally { permits.release() } })
        }
        try {
            scope.join()
        } catch (failed: StructuredTaskScope.FailedException) {
            throw failed.cause!!
        }
        return total.sum()
    }
}

private fun <R : Any> Exit<*, R>.value(): R = when (this) {
    is Exit.Done -> value
    is Exit.Died -> throw cause
    is Exit.Failed -> error(error.toString())
}

fun handleRequestSurefold(): Long =
    Stream.from(1..10).mapPar(4) { id -> fetchOrderBlocking(id) }.runFold(0L) { s, x -> s + x }
        .run(Forks()).toCompletableFuture().join().value()

fun handleRequestSurefoldAsync(): Long =
    Stream.from(1..10).mapAsync(4) { id -> fetchOrderAsync(id) }.runFold(0L) { s, x -> s + x }
        .run(Forks()).toCompletableFuture().join().value()

fun handleRequestCf(): Long {
    val total = LongAdder()
    val permits = Semaphore(4)
    var failure: Throwable? = null
    for (id in 1..10) {
        permits.acquire()
        fetchOrderAsync(id).whenComplete { x, e -> if (e != null) failure = e else total.add(x.toLong()); permits.release() }
    }
    permits.acquire(4)
    failure?.let { throw it }
    return total.sum()
}

fun handleRequestReactor(): Long =
    Flux.range(1, 10).flatMap({ id -> Mono.fromFuture(fetchOrderAsync(id)) }, 4)
        .reduce(0L) { s, x -> s + x }.block()!!

fun handleRequestReactorCheckpoint(): Long =
    Flux.range(1, 10).flatMap({ id -> Mono.fromFuture(fetchOrderAsync(id)).checkpoint("fetchOrder") }, 4)
        .reduce(0L) { s, x -> s + x }.checkpoint("handleRequest").block()!!

fun handleRequestReactorDebug(): Long {
    Hooks.onOperatorDebug()
    try {
        return Flux.range(1, 10).flatMap({ id -> Mono.fromFuture(fetchOrderAsync(id)) }, 4)
            .reduce(0L) { s, x -> s + x }.block()!!
    } finally {
        Hooks.resetOnOperatorDebug()
    }
}

fun handleRequestPekko(): Long =
    Source.range(1, 10).mapAsyncUnordered(4) { id -> fetchOrderAsync(id) }
        .runWith(Sink.fold(0L) { s, x -> s + x }, pekkoSystem).toCompletableFuture().join()

fun stackTraces() {
    val handlers = linkedMapOf<String, () -> Long>(
        "vt-plain" to ::handleRequestVt,
        "vt-structured" to ::handleRequestStructured,
        "surefold-mappar" to ::handleRequestSurefold,
        "surefold-mapasync" to ::handleRequestSurefoldAsync,
        "cf-plain" to ::handleRequestCf,
        "reactor-flatmap" to ::handleRequestReactor,
        "reactor-flatmap + checkpoint()" to ::handleRequestReactorCheckpoint,
        "reactor-flatmap + Hooks.onOperatorDebug()" to ::handleRequestReactorDebug,
        "pekko-mapasync-unordered" to ::handleRequestPekko,
    )
    for ((name, handler) in handlers) {
        println("===== $name")
        try {
            handler()
            println("(no failure?)")
        } catch (t: Throwable) {
            val caught = if (t is CompletionException) t.cause ?: t else t
            val trace = java.io.StringWriter().also { caught.printStackTrace(java.io.PrintWriter(it)) }.toString()
            println(trace.lines().take(60).joinToString("\n"))
            emit(
                linkedMapOf<String, Any?>(
                    "kind" to "stacktrace", "contestant" to name, "exception" to caught::class.simpleName,
                    "namesHandleRequest" to ("handleRequest" in trace), "namesFetchOrder" to ("fetchOrder" in trace),
                    "lines" to trace.lines().size,
                ) + environment(),
            )
        }
    }
}

/**
 * With 100 calls pending in each contestant, what a thread dump shows: how many threads there are, and how many of
 * them have the contestant's call site on their stack. A blocking contestant's pending calls are threads you can
 * read; an async one's are objects on the heap, and the dump has nothing to say about them.
 */
fun threadDumps() {
    val bean = ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean::class.java)
    for (c in contestants.values.filter { it.model != Model.UNBOUNDED }) {
        val counters = Counters()
        val device = HeldDevice()
        val (t, _) = Semantics.start {
            c.run(Job(counted(200, counters), 100, DeviceIo(device, Workload(0), counters), sink = { counters.consumed.increment() }))
        }
        while (device.holding < 100) Thread.sleep(10)
        Thread.sleep(200)
        val file = Files.createTempFile("bvr-dump", ".json")
        Files.delete(file)
        bean.dumpThreads(file.toString(), HotSpotDiagnosticMXBean.ThreadDumpFormat.JSON)
        val json = Files.readString(file)
        Files.delete(file)
        // One entry per thread; split on the thread objects' "tid" fields rather than parse the whole document.
        val threads = json.split("\"tid\"").drop(1)
        val atCallSite = threads.count { "callBlocking" in it || "BlockingHttp" in it }
        device.release()
        t.join()
        emit(
            linkedMapOf<String, Any?>(
                "kind" to "threaddump", "contestant" to c.name, "model" to c.model, "pendingCalls" to 100,
                "threads" to threads.size, "threadsAtCallSite" to atCallSite,
            ) + environment(),
        )
    }
}
