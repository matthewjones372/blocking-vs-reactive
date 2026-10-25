package bvr.arena

import java.io.File

/**
 * vt-arena's handler on a runtime of our own: `UringArenaKt [port] [dataset]`. One [Carrier] per core, each with its
 * own io_uring and listening socket; each connection is a fiber whose reads and writes look blocking.
 *
 * Needs `--enable-native-access=ALL-UNNAMED --add-exports java.base/jdk.internal.vm=ALL-UNNAMED`.
 */
fun main(args: Array<String>) {
    val port = args.getOrNull(0)?.toInt() ?: 8080
    val items = Dataset.load(File(args.getOrNull(1) ?: "/data/dataset.json"))
    val carriers = List(Runtime.getRuntime().availableProcessors()) { i ->
        // Io.write never parks and copies before it returns, so one carrier's connections can share these.
        val out = Out(64 * 1024)
        val body = Out(16 * 1024)
        // The read buffer too: a connection only reads inside drain(), which never parks.
        val read = ByteArray(16 * 1024)
        Carrier("carrier-$i", port) { fd -> UringConnection(fd, items, out, body, read).run() }
    }
    carriers.forEach { it.start() }
    println("URINGARENA ready port=$port carriers=${carriers.size} items=${items.size}")
    carriers.forEach { it.join() }
}

private class UringConnection(private val fd: Int, items: List<Dataset.Item>, out: Out, body: Out, read: ByteArray?) :
    HttpConnection(items, out, body, read) {
    /** Parks only here, between drains, so each park saves and restores a few small frames. */
    fun run() {
        while (Io.awaitData(fd) && drain()) Unit
    }

    override fun read(into: ByteArray, at: Int, max: Int): Int = Io.read(fd, into, at, max)

    override fun readNow(into: ByteArray, at: Int, max: Int): Int = Io.readNow(fd, into, at, max)

    override fun write(bytes: ByteArray, length: Int) = Io.write(fd, bytes, length)
}
