package bvr.arena

import java.io.File
import java.io.InputStream
import java.io.OutputStream
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.net.Socket
import java.net.StandardSocketOptions

/**
 * HttpArena's `baseline`, `pipelined` and `/json` endpoints on blocking code: `VtArenaKt [port] [dataset]`.
 *
 * One virtual thread per connection reads into a buffer of its own, answers every complete request already in it into
 * a write buffer of its own, and writes that once. The parser works on offsets into the read buffer, so a request
 * allocates no strings. This is the best case for a Lark-based Pelican backend: Pelican and Lark would add work on top
 * of it, never take any away.
 */
fun main(args: Array<String>) {
    val port = args.getOrNull(0)?.toInt() ?: 8080
    val items = Dataset.load(File(args.getOrNull(1) ?: "/data/dataset.json"))
    // net.core.somaxconn caps one backlog at 4,096 here; several listeners on SO_REUSEPORT each get their own.
    val listeners = List(Runtime.getRuntime().availableProcessors()) {
        ServerSocket().apply {
            setOption(StandardSocketOptions.SO_REUSEADDR, true)
            setOption(StandardSocketOptions.SO_REUSEPORT, true)
            bind(InetSocketAddress(port), 65_535)
        }
    }
    println("VTARENA ready port=$port listeners=${listeners.size} items=${items.size}")
    listeners.map { listener ->
        Thread.ofVirtual().start {
            while (true) {
                val socket = listener.accept()
                Thread.ofVirtual().start { Connection(socket, items).run() }
            }
        }
    }.forEach { it.join() }
}

private class Connection(private val socket: Socket, items: List<Dataset.Item>) : HttpConnection(items) {
    private val input: InputStream = socket.getInputStream()
    private val output: OutputStream = socket.getOutputStream()

    fun run() {
        socket.tcpNoDelay = true
        try {
            serve()
        } catch (_: java.io.IOException) {
        } finally {
            socket.close()
        }
    }

    override fun read(into: ByteArray, at: Int, max: Int): Int = input.read(into, at, max)

    override fun write(bytes: ByteArray, length: Int) = output.write(bytes, 0, length)
}

/**
 * One HTTP/1.1 connection's buffering and answers, whatever moves its bytes: every complete request already read is
 * answered into one write buffer, which is written once.
 */
internal abstract class HttpConnection(
    private val items: List<Dataset.Item>,
    // A runtime whose write never parks may share these between all the connections on one thread.
    private val out: Out = Out(4 * 1024),
    private val body: Out = Out(4 * 1024),
    // A runtime that drains its connections one at a time on a thread may lend them all that thread's read buffer:
    // a connection then keeps bytes of its own only while it holds part of a request.
    private val shared: ByteArray? = null,
) {
    // Small, and grown only when a request needs it: thousands of connections share each core's caches.
    private var own: ByteArray? = if (shared == null) ByteArray(4 * 1024) else null
    private var buf = own ?: shared!!
    private var start = 0
    private var end = 0

    /** Reads up to [max] bytes into [into] at [at]; negative or zero at end of stream. */
    protected abstract fun read(into: ByteArray, at: Int, max: Int): Int

    /** Like [read] but never waits: zero when nothing has arrived, negative at end of stream. */
    protected open fun readNow(into: ByteArray, at: Int, max: Int): Int = throw UnsupportedOperationException()

    protected abstract fun write(bytes: ByteArray, length: Int)

    fun serve() {
        while (true) {
            if (!fill()) return
            answer()
        }
    }

    /**
     * Answers whatever has arrived without waiting for more; false once the connection has ended. A runtime that
     * parks only between calls to this keeps the stack it saves and restores at each park small.
     */
    fun drain(): Boolean {
        if (shared != null && start == end) buf = shared
        while (true) {
            makeRoom()
            val n = readNow(buf, end, buf.size - end)
            if (n < 0) return false
            if (n == 0) break
            end += n
        }
        answer()
        if (buf === shared && start < end) keepRest()
        return true
    }

    /** Moves the part of a request left in the lent buffer into the connection's own, before another connection reads. */
    private fun keepRest() {
        val rest = end - start
        var mine = own
        if (mine == null || mine.size < rest) {
            mine = ByteArray(maxOf(4 * 1024, Integer.highestOneBit(rest - 1) shl 1))
            own = mine
        }
        System.arraycopy(buf, start, mine, 0, rest)
        buf = mine
        start = 0
        end = rest
    }

    /** Answers every complete request read so far, with one write. */
    private fun answer() {
        while (true) {
            val consumed = handle(start, end)
            if (consumed <= 0) break
            start += consumed
        }
        if (out.size > 0) {
            write(out.bytes, out.size)
            out.size = 0
        }
    }

    /** Reads at least one more byte; false at end of stream. */
    private fun fill(): Boolean {
        makeRoom()
        val n = read(buf, end, buf.size - end)
        if (n <= 0) return false
        end += n
        return true
    }

    /** Compacts or grows the buffer so there is room after [end]. */
    private fun makeRoom() {
        if (start == end) {
            start = 0; end = 0
        } else if (end == buf.size) {
            if (start > 0) {
                System.arraycopy(buf, start, buf, 0, end - start)
                end -= start; start = 0
            } else {
                buf = buf.copyOf(buf.size * 2)
                own = buf
            }
        }
    }

    /** Answers the request at [from] into [out] and returns the bytes it took, or 0 if it is not complete yet. */
    private fun handle(from: Int, to: Int): Int {
        val b = buf
        // One pass: the request line, then each header line up to the empty one. Running out of bytes anywhere means
        // the request is not complete yet.
        val methodEnd = indexOf(b, ' '.code, from, to)
        if (methodEnd < 0) return 0
        val post = methodEnd - from == 4 && b[from] == 'P'.code.toByte()
        val targetStart = methodEnd + 1
        val targetEnd = indexOf(b, ' '.code, targetStart, to)
        if (targetEnd < 0) return 0
        var pathEnd = indexOf(b, '?'.code, targetStart, targetEnd)
        if (pathEnd < 0) pathEnd = targetEnd
        val requestLineEnd = indexOf(b, '\n'.code, targetEnd, to)
        if (requestLineEnd < 0) return 0

        // Headers: only the two that frame a body, and only lines starting with their first letter are looked into.
        var contentLength = 0
        var chunked = false
        var line = requestLineEnd + 1
        while (true) {
            if (line + 1 >= to) return 0
            if (b[line] == '\r'.code.toByte()) break
            val lineEnd = indexOf(b, '\r'.code, line, to)
            if (lineEnd < 0 || lineEnd + 1 >= to) return 0
            val first = b[line].toInt() or 0x20
            if (first == 'c'.code || first == 't'.code) {
                val colon = indexOf(b, ':'.code, line, lineEnd)
                if (colon > 0) {
                    var v = colon + 1
                    while (b[v] == ' '.code.toByte()) v++
                    if (nameIs(b, line, colon, CONTENT_LENGTH)) contentLength = parseLong(b, v, lineEnd).toInt()
                    else if (nameIs(b, line, colon, TRANSFER_ENCODING)) chunked = true
                }
            }
            line = lineEnd + 2
        }
        val headEnd = line - 2

        var next = headEnd + 4
        var bodyValue = 0L
        if (chunked) {
            // Each chunk: size in hex, CRLF, data, CRLF; a zero size ends it (no trailers in these requests).
            var p = next
            while (true) {
                val sizeEnd = indexOf(b, '\r'.code, p, to)
                if (sizeEnd < 0 || sizeEnd + 1 >= to) return 0
                val size = parseHex(b, p, sizeEnd)
                val data = sizeEnd + 2
                if (data + size + 2 > to) return 0
                if (size == 0) { p = data + 2; break }
                bodyValue = digitsInto(bodyValue, b, data, data + size)
                p = data + size + 2
            }
            next = p
        } else if (contentLength > 0) {
            if (next + contentLength > to) return 0
            bodyValue = digitsInto(0, b, next, next + contentLength)
            next += contentLength
        }

        when {
            pathIs(b, targetStart, pathEnd, BASELINE) -> {
                var sum = querySum(b, pathEnd, targetEnd)
                if (post) sum += bodyValue
                out.textResponse(sum)
            }
            pathIs(b, targetStart, pathEnd, PIPELINE) -> out.response(OK_TEXT, OK)
            pathStarts(b, targetStart, pathEnd, JSON) -> {
                val count = parseLong(b, targetStart + JSON.size, pathEnd).toInt().coerceIn(0, items.size)
                val m = queryValue(b, pathEnd, targetEnd, 'm'.code.toByte(), 1)
                out.jsonResponse(body, items, count, m)
            }
            else -> out.response(NOT_FOUND_TEXT, NOT_FOUND_BODY)
        }
        return next - from
    }
}

private fun indexOf(b: ByteArray, c: Int, from: Int, to: Int): Int = Scan.indexOf(b, c, from, to)

private fun nameIs(b: ByteArray, from: Int, to: Int, lowerName: ByteArray): Boolean {
    if (to - from != lowerName.size) return false
    for (i in lowerName.indices) if ((b[from + i].toInt() or 0x20) != lowerName[i].toInt()) return false
    return true
}

private fun pathIs(b: ByteArray, from: Int, to: Int, path: ByteArray): Boolean =
    to - from == path.size && pathStarts(b, from, to, path)

private fun pathStarts(b: ByteArray, from: Int, to: Int, prefix: ByteArray): Boolean {
    if (to - from < prefix.size) return false
    for (i in prefix.indices) if (b[from + i] != prefix[i]) return false
    return true
}

private fun parseLong(b: ByteArray, from: Int, to: Int): Long {
    var v = 0L
    var neg = false
    var i = from
    if (i < to && b[i] == '-'.code.toByte()) { neg = true; i++ }
    while (i < to) {
        val d = b[i] - '0'.code.toByte()
        if (d !in 0..9) break
        v = v * 10 + d
        i++
    }
    return if (neg) -v else v
}

private fun parseHex(b: ByteArray, from: Int, to: Int): Int {
    var v = 0
    for (i in from until to) {
        val c = b[i].toInt() or 0x20
        v = v * 16 + if (c in '0'.code..'9'.code) c - '0'.code else c - 'a'.code + 10
    }
    return v
}

/** A body that is a decimal number, continued from [acc]: what is not a digit is not part of it. */
private fun digitsInto(acc: Long, b: ByteArray, from: Int, to: Int): Long {
    var v = acc
    for (i in from until to) {
        val d = b[i] - '0'.code.toByte()
        if (d in 0..9) v = v * 10 + d
    }
    return v
}

/** Every value in the query `?a=1&b=2` added up. */
private fun querySum(b: ByteArray, queryStart: Int, to: Int): Long {
    var sum = 0L
    var i = queryStart + 1
    while (i < to) {
        val eq = indexOf(b, '='.code, i, to)
        if (eq < 0) break
        var amp = indexOf(b, '&'.code, eq, to)
        if (amp < 0) amp = to
        sum += parseLong(b, eq + 1, amp)
        i = amp + 1
    }
    return sum
}

private fun queryValue(b: ByteArray, queryStart: Int, to: Int, name: Byte, default: Long): Long {
    var i = queryStart + 1
    while (i < to) {
        var amp = indexOf(b, '&'.code, i, to)
        if (amp < 0) amp = to
        if (b[i] == name && i + 1 < amp && b[i + 1] == '='.code.toByte()) return parseLong(b, i + 2, amp)
        i = amp + 1
    }
    return default
}

private fun ascii(s: String) = s.toByteArray(Charsets.US_ASCII)

private val CONTENT_LENGTH = ascii("content-length")
private val TRANSFER_ENCODING = ascii("transfer-encoding")
private val BASELINE = ascii("/baseline11")
private val PIPELINE = ascii("/pipeline")
private val JSON = ascii("/json/")
private val OK = ascii("ok")
private val NOT_FOUND_BODY = ascii("not found")
// Status line, content type and the Content-Length name in one piece, so a response's head is three copies.
private val OK_TEXT = ascii("HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: ")
private val OK_JSON = ascii("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: ")
private val NOT_FOUND_TEXT = ascii("HTTP/1.1 404 Not Found\r\nContent-Type: text/plain\r\nContent-Length: ")

/**
 * `\r\nDate: <IMF-fixdate>\r\n\r\n`, rebuilt once a second by a thread of its own, so a response reads a field instead
 * of asking the clock whether it is time to rebuild it.
 */
private object DateHeader {
    @Volatile private var bytes = render()

    init {
        Thread.ofPlatform().daemon().name("date-header").start {
            while (true) {
                Thread.sleep(1000 - System.currentTimeMillis() % 1000)
                bytes = render()
            }
        }
    }

    private fun render() = ascii(
        "\r\nDate: " + java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME
            .format(java.time.ZonedDateTime.now(java.time.ZoneOffset.UTC)) + "\r\n\r\n",
    )

    fun current(): ByteArray = bytes
}

/** A connection's write buffer: responses for a whole batch of requests, written to the socket once. */
internal class Out(capacity: Int) {
    var bytes = ByteArray(capacity)
    var size = 0
    private val scratch = ByteArray(20)

    private fun ensure(n: Int) {
        if (size + n > bytes.size) bytes = bytes.copyOf(maxOf(bytes.size * 2, size + n))
    }

    fun put(b: ByteArray) {
        ensure(b.size)
        System.arraycopy(b, 0, bytes, size, b.size)
        size += b.size
    }

    fun put(c: Char) {
        ensure(1)
        bytes[size++] = c.code.toByte()
    }

    fun putLong(value: Long) {
        ensure(20)
        if (value == 0L) { bytes[size++] = '0'.code.toByte(); return }
        var v = value
        if (v < 0) { bytes[size++] = '-'.code.toByte(); v = -v }
        var n = 0
        while (v > 0) { scratch[n++] = ('0'.code + (v % 10).toInt()).toByte(); v /= 10 }
        while (n > 0) bytes[size++] = scratch[--n]
    }

    /** A JSON string. The dataset's strings need no escaping, which [Dataset.load] checks. */
    fun putString(s: String) {
        ensure(s.length + 2)
        bytes[size++] = '"'.code.toByte()
        for (i in s.indices) bytes[size++] = s[i].code.toByte()
        bytes[size++] = '"'.code.toByte()
    }

    /** [start] is the status line and headers up to the Content-Length value, which follows, then the Date. */
    private fun head(start: ByteArray, length: Int) {
        put(start); putLong(length.toLong()); put(DateHeader.current())
    }

    fun response(start: ByteArray, body: ByteArray) {
        head(start, body.size)
        put(body)
    }

    fun textResponse(value: Long) {
        val digits = if (value == 0L) 1 else (if (value < 0) 1 else 0) + digitCount(if (value < 0) -value else value)
        head(OK_TEXT, digits)
        putLong(value)
    }

    /** The body goes into [body], a buffer the connection reuses, so its length is known before the head is written. */
    fun jsonResponse(body: Out, items: List<Dataset.Item>, count: Int, m: Long) {
        body.size = 0
        body.jsonBody(items, count, m)
        head(OK_JSON, body.size)
        ensure(body.size)
        System.arraycopy(body.bytes, 0, bytes, size, body.size)
        size += body.size
    }

    /** `/json`'s body: the first [count] items, each with its total for multiplier [m], and the count. */
    fun jsonBody(items: List<Dataset.Item>, count: Int, m: Long) {
        put('{'); put(ITEMS)
        for (i in 0 until count) {
            if (i > 0) put(',')
            val it = items[i]
            put(ID); putLong(it.id.toLong())
            put(NAME); putString(it.name)
            put(CATEGORY); putString(it.category)
            put(PRICE); putLong(it.price.toLong())
            put(QUANTITY); putLong(it.quantity.toLong())
            put(ACTIVE); put(if (it.active) TRUE else FALSE)
            put(TAGS)
            for (t in it.tags.indices) { if (t > 0) put(','); putString(it.tags[t]) }
            put(RATING); putLong(it.score.toLong()); put(RATING_COUNT); putLong(it.ratingCount.toLong())
            put(TOTAL); putLong(it.price.toLong() * it.quantity * m); put('}')
        }
        put(COUNT); putLong(count.toLong()); put('}')
    }
}

private fun digitCount(v: Long): Int {
    var n = 1
    var x = v
    while (x >= 10) { x /= 10; n++ }
    return n
}

private val ITEMS = ascii("\"items\":[")
private val ID = ascii("{\"id\":")
private val NAME = ascii(",\"name\":")
private val CATEGORY = ascii(",\"category\":")
private val PRICE = ascii(",\"price\":")
private val QUANTITY = ascii(",\"quantity\":")
private val ACTIVE = ascii(",\"active\":")
private val TRUE = ascii("true")
private val FALSE = ascii("false")
private val TAGS = ascii(",\"tags\":[")
private val RATING = ascii("],\"rating\":{\"score\":")
private val RATING_COUNT = ascii(",\"count\":")
private val TOTAL = ascii("},\"total\":")
private val COUNT = ascii("],\"count\":")
