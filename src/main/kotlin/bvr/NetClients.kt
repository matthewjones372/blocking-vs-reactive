package bvr

import java.io.InputStream
import java.net.InetSocketAddress
import java.net.Socket
import java.net.StandardSocketOptions
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.ByteBuffer
import java.nio.channels.AsynchronousChannelGroup
import java.nio.channels.AsynchronousSocketChannel
import java.nio.channels.CompletionHandler
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import reactor.core.publisher.Mono
import reactor.netty.resources.ConnectionProvider
import java.time.Duration
import reactor.netty.http.client.HttpClient as NettyClient

/**
 * The two raw clients share their protocol: one `GET` per connection at a time (no pipelining), a response of
 * headers and a `Content-Length` body. They differ only in how they wait: [BlockingHttp] reads a `java.net.Socket`,
 * which parks a virtual thread on the JDK's poller; [AsyncHttp] reads an `AsynchronousSocketChannel`, whose
 * completion handlers run on a fixed pool of its own. Each keeps one connection per unit of concurrency, so the pool
 * itself never queues.
 */
internal fun requestBytes(micros: Long, size: Int): ByteArray =
    "GET /?d=$micros&s=$size HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n".toByteArray(Charsets.ISO_8859_1)

/** Parses a response's head out of `buf[0 until n]`: the body length and where the body starts, or null if incomplete. */
internal fun responseHead(buf: ByteArray, n: Int): Pair<Int, Int>? {
    for (i in 0 until n - 3) {
        if (buf[i] == '\r'.code.toByte() && buf[i + 1] == '\n'.code.toByte() &&
            buf[i + 2] == '\r'.code.toByte() && buf[i + 3] == '\n'.code.toByte()
        ) {
            val head = String(buf, 0, i, Charsets.ISO_8859_1)
            val at = head.indexOf("Content-Length: ")
            check(at >= 0) { "no Content-Length in $head" }
            val length = head.substring(at + 16).takeWhile { it.isDigit() }.toInt()
            return length to i + 4
        }
    }
    return null
}

class BlockingHttp(private val port: Int, connections: Int) : AutoCloseable {
    private class Conn(port: Int) {
        val socket = Socket().apply { tcpNoDelay = true; connect(InetSocketAddress("127.0.0.1", port)) }
        val out = socket.getOutputStream()
        val inp: InputStream = socket.getInputStream()
        val buf = ByteArray(64 * 1024)

        fun get(micros: Long, size: Int): Int {
            out.write(requestBytes(micros, size))
            var n = 0
            var head: Pair<Int, Int>? = null
            while (head == null) {
                val r = inp.read(buf, n, buf.size - n)
                check(r >= 0) { "server closed the connection" }
                n += r
                head = responseHead(buf, n)
            }
            val (length, bodyAt) = head
            var body = n - bodyAt
            while (body < length) {
                val r = inp.read(buf, 0, minOf(buf.size, length - body))
                check(r >= 0) { "server closed the connection" }
                body += r
            }
            return length
        }
    }

    private val pool = ArrayBlockingQueue<Conn>(connections).apply { repeat(connections) { add(Conn(port)) } }

    /** Parks the calling thread until the answer; an interrupted call closes its connection and opens another. */
    fun get(micros: Long, size: Int): Int {
        val c = pool.take()
        var ok = false
        try {
            return c.get(micros, size).also { ok = true }
        } finally {
            if (ok) pool.put(c) else { c.socket.close(); pool.put(Conn(port)) }
        }
    }

    override fun close() { pool.forEach { it.socket.close() } }
}

class AsyncHttp(private val port: Int, connections: Int, threads: Int = Runtime.getRuntime().availableProcessors()) : AutoCloseable {
    private val n = AtomicInteger()
    private val group = AsynchronousChannelGroup.withFixedThreadPool(threads) { r ->
        Thread(r, "nio-client-${n.incrementAndGet()}").apply { isDaemon = true }
    }

    private inner class Conn {
        val ch: AsynchronousSocketChannel = AsynchronousSocketChannel.open(group).apply {
            setOption(StandardSocketOptions.TCP_NODELAY, true)
            connect(InetSocketAddress("127.0.0.1", port)).get()
        }
        val buf = ByteArray(64 * 1024)
        val readBuf: ByteBuffer = ByteBuffer.wrap(buf)
        var writeBuf: ByteBuffer = ByteBuffer.allocate(0)
        var answer = CompletableFuture<Int>()
        var head: Pair<Int, Int>? = null
        var body = 0

        fun get(micros: Long, size: Int): CompletableFuture<Int> {
            answer = CompletableFuture()
            head = null
            body = 0
            readBuf.clear()
            writeBuf = ByteBuffer.wrap(requestBytes(micros, size))
            ch.write(writeBuf, this, wrote)
            return answer
        }

        fun fail(e: Throwable) { answer.completeExceptionally(e) }
    }

    private val wrote = object : CompletionHandler<Int, Conn> {
        override fun completed(result: Int, c: Conn) {
            if (c.writeBuf.hasRemaining()) c.ch.write(c.writeBuf, c, this) else c.ch.read(c.readBuf, c, read)
        }
        override fun failed(e: Throwable, c: Conn) = c.fail(e)
    }

    private val read = object : CompletionHandler<Int, Conn> {
        override fun completed(result: Int, c: Conn) {
            if (result < 0) return c.fail(IllegalStateException("server closed the connection"))
            val head = responseHead(c.buf, c.readBuf.position())
            if (head == null) { c.ch.read(c.readBuf, c, this); return }
            c.head = head
            c.body = c.readBuf.position() - head.second
            restOfBody(c)
        }
        override fun failed(e: Throwable, c: Conn) = c.fail(e)
    }

    private val bodyRead = object : CompletionHandler<Int, Conn> {
        override fun completed(result: Int, c: Conn) {
            if (result < 0) return c.fail(IllegalStateException("server closed the connection"))
            c.body += result
            restOfBody(c)
        }
        override fun failed(e: Throwable, c: Conn) = c.fail(e)
    }

    /** Reads what is left of the body over the start of the buffer, since only its length matters, then answers. */
    private fun restOfBody(c: Conn) {
        val length = c.head!!.first
        if (c.body < length) {
            c.readBuf.clear()
            c.readBuf.limit(minOf(c.buf.size, length - c.body))
            c.ch.read(c.readBuf, c, bodyRead)
            return
        }
        // Read the answer before the connection goes back: whoever takes it next replaces `answer`.
        val answer = c.answer
        idle.offer(c)
        answer.complete(length)
    }

    private val idle = ConcurrentLinkedQueue<Conn>().apply { repeat(connections) { add(Conn()) } }

    /**
     * The connection goes back to the pool before the answer completes, so whatever the answer starts next finds
     * it free. The contestant's own bound keeps calls in flight at or under the number of connections.
     */
    fun get(micros: Long, size: Int): CompletableFuture<Int> =
        (idle.poll() ?: error("more calls in flight than connections")).get(micros, size)

    override fun close() {
        idle.forEach { it.ch.close() }
        group.shutdownNow()
    }
}

/**
 * The JDK's own `HttpClient`, as either side would use it: [send] parks the caller, [sendAsync] answers a future
 * whose dependent stages run on the client's default executor. HTTP/1.1, so it pools a connection per request in
 * flight as the raw clients do.
 */
class JdkHttp(private val port: Int) : AutoCloseable {
    private val client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build()

    private fun request(micros: Long, size: Int) =
        HttpRequest.newBuilder(URI.create("http://127.0.0.1:$port/?d=$micros&s=$size")).GET().build()

    fun send(micros: Long, size: Int): Int = client.send(request(micros, size), HttpResponse.BodyHandlers.ofByteArray()).body().size

    fun sendAsync(micros: Long, size: Int): CompletableFuture<Int> =
        client.sendAsync(request(micros, size), HttpResponse.BodyHandlers.ofByteArray()).thenApply { it.body().size }

    override fun close() = client.close()
}

/** A plain request on its own connection, outside any pool: how the bench reads and resets the server's stats. */
fun controlRequest(port: Int, path: String): String = Socket("127.0.0.1", port).use { s ->
    s.getOutputStream().write("GET $path HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n".toByteArray())
    val buf = ByteArray(64 * 1024)
    var n = 0
    var head: Pair<Int, Int>? = null
    while (head == null || n < head.second + head.first) {
        val r = s.getInputStream().read(buf, n, buf.size - n)
        check(r >= 0) { "server closed the control connection" }
        n += r
        head = head ?: responseHead(buf, n)
    }
    String(buf, head.second, head.first)
}

/**
 * Reactor Netty's `HttpClient`, as a Reactor service would call its dependencies: a pool of at most [connections]
 * (one per call in flight, as the raw clients have), whose acquirers queue rather than fail, and whose idle
 * connections stay for ten minutes. Answers are read whole and counted. Ported from main's network scenario.
 */
class NettyHttp(port: Int, connections: Int) : AutoCloseable {
    private val pool = ConnectionProvider.builder("bvr-net").maxConnections(connections).pendingAcquireMaxCount(-1)
        .maxIdleTime(Duration.ofMinutes(10)).build()
    private val client = NettyClient.create(pool).baseUrl("http://127.0.0.1:$port")

    fun get(micros: Long, size: Int): Mono<Int> =
        client.get().uri("/?d=$micros&s=$size").responseContent().aggregate().asByteArray().map { it.size }

    override fun close() = pool.dispose()
}
