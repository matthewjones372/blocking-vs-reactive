package bvr

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
import reactor.core.publisher.Mono
import reactor.netty.resources.ConnectionProvider
import java.io.BufferedInputStream
import java.io.InputStream
import java.io.OutputStream
import java.net.InetSocketAddress
import java.net.Socket
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Executors
import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.LongAdder
import reactor.netty.http.client.HttpClient as NettyClient

/*
 * The real-network scenario: the same bounded fan-out as IoScenario.kt, but each call is an HTTP/1.1 GET over loopback
 * to `NetServer`, a separate JVM that answers after 10 ms. `net-16`, `net-1k` and `net-10k` keep 16, 1,000 and 10,000
 * calls in flight. 100,000 calls at 10k in flight is only ten windows, but the JDK client manages under 2,000 calls/s
 * there (see [jdkClient]), and a longer run would take a quarter of an hour per iteration.
 *
 * Every contestant but `reactor-netty` uses one shared JDK `HttpClient`. That client is asynchronous inside: `send` is
 * `sendAsync(...).get()` (HttpClientImpl.java), and its one selector thread completes the future. So a virtual thread
 * in `send` is woken by a platform thread, as on the fake device, never by the JDK's I/O poller. The `-socket`
 * contestants are there to measure the poller: the same blocking code over a plain `java.net.Socket`, which parks the
 * virtual thread on the JDK's poller while it waits for the answer.
 */
val netScenarios: Map<String, IoParams> = linkedMapOf(
    "net-16" to IoParams(items = 4_000, concurrency = 16, latencyMicros = 10_000),
    "net-1k" to IoParams(items = 100_000, concurrency = 1_000, latencyMicros = 10_000),
    "net-10k" to IoParams(items = 100_000, concurrency = 10_000, latencyMicros = 10_000),
)

private val netHost = System.getProperty("bvr.net.host", "127.0.0.1")
private val netPort = System.getProperty("bvr.net.port", "8099").toInt()
private val netBase = "http://$netHost:$netPort"

/**
 * The one JDK client in this JVM. HTTP/1.1, as the server speaks; without it the client offers an h2c upgrade.
 * The JDK opens a new HTTP/1.1 connection whenever none is idle and never caps how many are open; the one limit it
 * has, `jdk.httpclient.connectionPoolSize`, is how many *idle* ones it keeps, and is unbounded by default
 * (ConnectionPool.java). [netMain] sets it to 20,000 so that is explicit, and the keep-alive timeout to 10 minutes so
 * no connection closes between iterations.
 *
 * Its executor is left at the default, an unbounded cached pool of platform threads, which is what a caller gets
 * unless they choose otherwise. Two things in it grow with the number of open connections. Every checkout and
 * every return walks a `LinkedList` of all idle connections under the pool's one lock (`ExpiryList.remove` and
 * `ExpiryList.add`, whose deadlines are cut to whole seconds, so a return usually walks the whole list), and the
 * cached pool grows a platform thread per task waiting on that lock: about 8,800 of them at 10k in flight.
 */
val jdkClient: HttpClient by lazy { HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build() }

private fun request(x: Int): HttpRequest = HttpRequest.newBuilder(URI.create("$netBase/?x=$x")).GET().build()

/** A blocking call through the JDK client. */
fun jdkSend(x: Int): Int = jdkClient.send(request(x), HttpResponse.BodyHandlers.ofString()).body().toInt()

/** An asynchronous call through the JDK client. */
fun jdkSendAsync(x: Int): CompletableFuture<Int> =
    jdkClient.sendAsync(request(x), HttpResponse.BodyHandlers.ofString()).thenApply { it.body().toInt() }

/** Reactor Netty's client, its pool sized for the concurrency of the scenario this JVM runs. */
private val nettyClients = HashMap<Int, NettyClient>()

fun nettyClient(n: Int): NettyClient = synchronized(nettyClients) {
    nettyClients.getOrPut(n) {
        val pool = ConnectionProvider.builder("bvr-net").maxConnections(n).pendingAcquireMaxCount(-1)
            .maxIdleTime(Duration.ofMinutes(10)).build()
        NettyClient.create(pool).baseUrl(netBase)
    }
}

/**
 * A blocking HTTP/1.1 client over `java.net.Socket`, as small as the server lets it be: a pool of keep-alive
 * connections, one request at a time on each, and a body read by its `content-length`. On a virtual thread, its reads
 * park the thread on the JDK's poller, which is what the `-socket` contestants measure.
 */
object SocketHttp {
    private class Conn(val socket: Socket) {
        val out: OutputStream = socket.getOutputStream()
        val input: InputStream = BufferedInputStream(socket.getInputStream(), 1024)
    }

    private val idle = ConcurrentLinkedQueue<Conn>()
    private val host = "Host: $netHost:$netPort\r\n\r\n".toByteArray(Charsets.US_ASCII)

    fun get(x: Int): Int {
        val conn = idle.poll() ?: Conn(Socket().apply { tcpNoDelay = true; connect(InetSocketAddress(netHost, netPort)) })
        conn.out.write("GET /?x=$x HTTP/1.1\r\n".toByteArray(Charsets.US_ASCII) + host)
        conn.out.flush()
        var length = -1
        while (true) {
            val line = readLine(conn.input)
            if (line.isEmpty()) break
            if (line.startsWith("content-length:", ignoreCase = true)) length = line.substring(15).trim().toInt()
        }
        check(length >= 0) { "no content-length" }
        val body = conn.input.readNBytes(length)
        idle.offer(conn)
        return String(body, Charsets.US_ASCII).toInt()
    }

    private fun readLine(input: InputStream): String {
        val sb = StringBuilder()
        while (true) {
            val c = input.read()
            check(c >= 0) { "connection closed" }
            if (c == '\n'.code) return sb.toString()
            if (c != '\r'.code) sb.append(c.toChar())
        }
    }
}

// region vt-plain
fun netVtPlain(p: IoParams, probe: Probe): Long {
    val permits = Semaphore(p.concurrency)
    val sum = LongAdder()
    Executors.newVirtualThreadPerTaskExecutor().use { threads ->
        for (x in counted(p.items, probe)) {
            permits.acquire()
            threads.submit {
                try {
                    sum.add(jdkSend(x).toLong())
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

// region lark
fun netLark(p: IoParams, probe: Probe): Long {
    val exit = Stream.from(Iterable { counted(p.items, probe) })
        .mapPar(p.concurrency) { x -> jdkSend(x) }
        .runFold(0L) { sum, x -> probe.completed(); sum + x }
        .run(Forks())
        .toCompletableFuture().join()
    return (exit as Exit.Done).value
}
// endregion

// region lark-async
fun netLarkAsync(p: IoParams, probe: Probe): Long {
    val exit = Stream.from(Iterable { counted(p.items, probe) })
        .mapAsync(p.concurrency) { x -> jdkSendAsync(x) }
        .runFold(0L) { sum, x -> probe.completed(); sum + x }
        .run(Forks())
        .toCompletableFuture().join()
    return (exit as Exit.Done).value
}
// endregion

// region reactor-netty
fun netReactorNetty(p: IoParams, probe: Probe): Long {
    val client = nettyClient(p.concurrency)
    return Flux.range(0, p.items)
        .doOnNext { probe.produced() }
        .flatMap({ x -> client.get().uri("/?x=$x").responseContent().aggregate().asString().map { it.toInt() } }, p.concurrency)
        .reduce(0L) { sum, x -> probe.completed(); sum + x }
        .block()!!
}
// endregion

// region reactor-jdk
fun netReactorJdk(p: IoParams, probe: Probe): Long =
    Flux.range(0, p.items)
        .doOnNext { probe.produced() }
        .flatMap({ x -> Mono.fromFuture(jdkSendAsync(x)) }, p.concurrency)
        .reduce(0L) { sum, x -> probe.completed(); sum + x }
        .block()!!
// endregion

// region pekko
fun netPekko(p: IoParams, probe: Probe): Long =
    Source.fromIterator { counted(p.items, probe) }
        .mapAsyncUnordered(p.concurrency) { x -> jdkSendAsync(x) }
        .runWith(Sink.fold(0L) { sum, x -> probe.completed(); sum + x }, pekkoSystem)
        .toCompletableFuture().join()
// endregion

// region vt-socket
fun netVtSocket(p: IoParams, probe: Probe): Long {
    val permits = Semaphore(p.concurrency)
    val sum = LongAdder()
    Executors.newVirtualThreadPerTaskExecutor().use { threads ->
        for (x in counted(p.items, probe)) {
            permits.acquire()
            threads.submit {
                try {
                    sum.add(SocketHttp.get(x).toLong())
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

// region lark-socket
fun netLarkSocket(p: IoParams, probe: Probe): Long {
    val exit = Stream.from(Iterable { counted(p.items, probe) })
        .mapPar(p.concurrency) { x -> SocketHttp.get(x) }
        .runFold(0L) { sum, x -> probe.completed(); sum + x }
        .run(Forks())
        .toCompletableFuture().join()
    return (exit as Exit.Done).value
}
// endregion

val netContestants: Map<String, IoContestant> = linkedMapOf(
    "vt-plain" to IoContestant { p, probe -> netVtPlain(p, probe) },
    "lark" to IoContestant { p, probe -> netLark(p, probe) },
    "lark-async" to IoContestant { p, probe -> netLarkAsync(p, probe) },
    "reactor-netty" to IoContestant { p, probe -> netReactorNetty(p, probe) },
    "reactor-jdk" to IoContestant { p, probe -> netReactorJdk(p, probe) },
    "pekko" to IoContestant { p, probe -> netPekko(p, probe) },
    "vt-socket" to IoContestant { p, probe -> netVtSocket(p, probe) },
    "lark-socket" to IoContestant { p, probe -> netLarkSocket(p, probe) },
)

/**
 * `Main <net-scenario> <contestant> [warmups] [iterations] [label]`: the loop in Main.kt, against the server. `label`
 * names a variant of the JVM's settings (a scheduler parallelism or poller mode), and is reported as `config=`.
 */
fun netMain(args: Array<String>) {
    System.getProperties().putIfAbsent("jdk.httpclient.connectionPoolSize", "20000")
    System.getProperties().putIfAbsent("jdk.httpclient.keepalive.timeout", "600")
    val scenario = args[0]
    val name = args[1]
    val warmups = args.getOrNull(2)?.toInt() ?: 3
    val iterations = args.getOrNull(3)?.toInt() ?: 5
    val config = args.getOrNull(4)?.let { " config=$it" } ?: ""
    val p = netScenarios.getValue(scenario)
    val contestant = netContestants.getValue(name)
    val expected = p.items.toLong() * (p.items - 1) / 2
    val tag = "scenario=$scenario contestant=$name$config"

    repeat(warmups) { Jvm.measure { probe -> check(contestant.run(p, probe) == expected) } }
    repeat(iterations) { i ->
        System.gc()
        val m = Jvm.measure { probe -> check(contestant.run(p, probe) == expected) }
        val opsPerSec = p.items / (m.wallMs / 1000)
        println(
            ("RESULT $tag iteration=$i wallMs=%.1f opsPerSec=%.0f cpuMs=%.1f " +
                "cpuUsPerOp=%.2f allocMb=%.1f allocBytesPerOp=%.0f gcCount=%d gcMs=%d maxOutstanding=%d")
                .format(
                    m.wallMs, opsPerSec, m.cpuMs, m.cpuMs * 1000 / p.items, m.allocatedMb,
                    m.allocatedMb * 1e6 / p.items, m.gcCount, m.gcMs, m.maxOutstanding,
                ),
        )
    }
    if (p.concurrency >= 1_000) {
        val perOp = Jvm.liveBytesPerInFlight(p.concurrency) { probe -> contestant.run(p, probe) }
        println("RESULT $tag liveBytesPerInFlight=%.0f".format(perOp))
    }
    println("RESULT $tag rssHighWaterMb=%.0f".format(Jvm.rssHighWaterMb()))
    System.exit(0)
}
