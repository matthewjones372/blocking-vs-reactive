package bvr

import com.sun.management.OperatingSystemMXBean
import java.lang.management.ManagementFactory
import java.net.InetSocketAddress
import java.net.StandardSocketOptions
import java.nio.ByteBuffer
import java.nio.channels.SelectionKey
import java.nio.channels.Selector
import java.nio.channels.ServerSocketChannel
import java.nio.channels.SocketChannel
import java.util.PriorityQueue
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.LongAdder
import java.util.concurrent.locks.LockSupport

/**
 * The test server for the socket scenarios: HTTP/1.1 with keep-alive, `GET /?d=<micros>&s=<bytes>` answered after
 * `d` microseconds with `s` bytes of body.
 *
 * No thread sleeps per request. Each of [selectors] threads owns its connections, parses their requests and keeps
 * its own queue of responses ordered by due time; one timer thread wakes a selector, every 100 µs at most, when its
 * earliest response is due. A slow response therefore costs a queue entry, not a thread, and the server's own delay
 * (`lag`: a response's write against its due time) is measured and reported, so a run can show that the server kept
 * up. `GET /__stats` answers that, with the server's CPU time; `GET /__reset` clears it.
 */
class NetServer(port: Int, selectors: Int) {
    private val server = ServerSocketChannel.open().apply {
        setOption(StandardSocketOptions.SO_REUSEADDR, true)
        bind(InetSocketAddress("127.0.0.1", port), 4096)
    }
    private val loops = List(selectors) { Loop(it) }
    private val next = AtomicInteger()
    @Volatile var lag = Histogram(); private set
    val served = LongAdder()
    private val os = ManagementFactory.getOperatingSystemMXBean() as OperatingSystemMXBean
    @Volatile private var cpuAtReset = os.processCpuTime
    @Volatile private var nanosAtReset = System.nanoTime()

    private class Due(val at: Long, val key: SelectionKey, val conn: Conn, val size: Int) : Comparable<Due> {
        override fun compareTo(other: Due) = at.compareTo(other.at)
    }

    private class Conn {
        val inBuf: ByteBuffer = ByteBuffer.allocate(8192)
        val pending = ArrayDeque<ByteBuffer>()
        var writing: ByteBuffer? = null
    }

    private inner class Loop(i: Int) {
        val selector: Selector = Selector.open()
        val registering = ConcurrentLinkedQueue<SocketChannel>()
        val due = PriorityQueue<Due>()
        @Volatile var earliest = Long.MAX_VALUE

        val thread: Thread = Thread.ofPlatform().daemon().name("server-$i").start { run() }

        fun run() {
            while (true) {
                selector.select()
                while (true) {
                    val ch = registering.poll() ?: break
                    ch.configureBlocking(false)
                    ch.setOption(StandardSocketOptions.TCP_NODELAY, true)
                    ch.register(selector, SelectionKey.OP_READ, Conn())
                }
                val keys = selector.selectedKeys().iterator()
                while (keys.hasNext()) {
                    val key = keys.next()
                    keys.remove()
                    try {
                        if (!key.isValid) continue
                        if (key.isReadable) read(key)
                        if (key.isValid && key.isWritable) flush(key)
                    } catch (_: java.io.IOException) {
                        key.cancel(); key.channel().close()
                    }
                }
                val now = System.nanoTime()
                while (due.isNotEmpty() && due.peek().at <= now) {
                    val d = due.poll()
                    lag.record(now - d.at)
                    if (d.key.isValid) respond(d)
                }
                earliest = due.peek()?.at ?: Long.MAX_VALUE
            }
        }

        private fun read(key: SelectionKey) {
            val conn = key.attachment() as Conn
            val ch = key.channel() as SocketChannel
            if (ch.read(conn.inBuf) < 0) { key.cancel(); ch.close(); return }
            while (true) {
                val request = takeRequest(conn.inBuf) ?: break
                when {
                    request.startsWith("GET /__stats") -> queue(key, conn, stats().toByteArray())
                    request.startsWith("GET /__reset") -> { reset(); queue(key, conn, "{}".toByteArray()) }
                    else -> {
                        val at = System.nanoTime() + param(request, "d") * 1_000
                        due.add(Due(at, key, conn, param(request, "s").toInt()))
                    }
                }
            }
        }

        // One body per size, shared by every response of that size: the server copies nothing per request.
        private val bodies = HashMap<Int, ByteArray>()

        private fun respond(d: Due) {
            served.increment()
            queue(d.key, d.conn, bodies.getOrPut(d.size) { ByteArray(d.size) { 'x'.code.toByte() } })
        }

        private fun queue(key: SelectionKey, conn: Conn, body: ByteArray) {
            conn.pending.addLast(ByteBuffer.wrap("HTTP/1.1 200 OK\r\nContent-Length: ${body.size}\r\n\r\n".toByteArray()))
            conn.pending.addLast(ByteBuffer.wrap(body))
            flush(key)
        }

        fun flush(key: SelectionKey) {
            val conn = key.attachment() as Conn
            val ch = key.channel() as SocketChannel
            while (true) {
                val buf = conn.writing ?: conn.pending.removeFirstOrNull() ?: break
                conn.writing = buf
                ch.write(buf)
                if (buf.hasRemaining()) {
                    key.interestOps(SelectionKey.OP_READ or SelectionKey.OP_WRITE)
                    return
                }
                conn.writing = null
            }
            key.interestOps(SelectionKey.OP_READ)
        }
    }

    private fun reset() {
        lag = Histogram()
        served.reset()
        cpuAtReset = os.processCpuTime
        nanosAtReset = System.nanoTime()
    }

    private fun stats(): String {
        val cpuMs = (os.processCpuTime - cpuAtReset) / 1e6
        val wallMs = (System.nanoTime() - nanosAtReset) / 1e6
        return Json.write(
            mapOf(
                "served" to served.sum(), "lagP50Us" to lag.quantile(0.5) / 1000, "lagP99Us" to lag.quantile(0.99) / 1000,
                "lagMaxUs" to lag.max() / 1000, "cpuMs" to cpuMs, "wallMs" to wallMs,
                "cpuUtilisation" to cpuMs / wallMs / Runtime.getRuntime().availableProcessors(),
                "cpus" to Runtime.getRuntime().availableProcessors(),
            ),
        )
    }

    fun serve() {
        // The timer: wakes whichever selector has a response due.
        Thread.ofPlatform().daemon().name("server-timer").start {
            while (true) {
                val now = System.nanoTime()
                for (l in loops) if (l.earliest <= now) l.selector.wakeup()
                LockSupport.parkNanos(100_000)
            }
        }
        while (true) {
            val ch = server.accept()
            val l = loops[Math.floorMod(next.getAndIncrement(), loops.size)]
            l.registering.add(ch)
            l.selector.wakeup()
        }
    }
}

/** The request at the front of [buf], if a whole one has arrived; the bytes after it are kept for the next. */
internal fun takeRequest(buf: ByteBuffer): String? {
    val end = (0 until buf.position() - 3).firstOrNull {
        buf.get(it) == '\r'.code.toByte() && buf.get(it + 1) == '\n'.code.toByte() &&
            buf.get(it + 2) == '\r'.code.toByte() && buf.get(it + 3) == '\n'.code.toByte()
    } ?: return null
    val bytes = ByteArray(end + 4)
    buf.flip()
    buf.get(bytes)
    buf.compact()
    return String(bytes, Charsets.ISO_8859_1)
}

internal fun param(request: String, name: String): Long {
    val start = request.indexOf("$name=") + name.length + 1
    var end = start
    while (end < request.length && request[end].isDigit()) end++
    return request.substring(start, end).toLong()
}

/** `net-server <port> [selectors]`: serves until killed. */
fun netServerMain(args: List<String>) {
    val selectors = args.getOrNull(1)?.toInt() ?: Runtime.getRuntime().availableProcessors()
    System.err.println("net-server on ${args[0]} with $selectors selectors")
    NetServer(args[0].toInt(), selectors).serve()
}
