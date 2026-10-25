package bvr

import com.sun.management.OperatingSystemMXBean
import io.netty.bootstrap.ServerBootstrap
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInitializer
import io.netty.channel.ChannelOption
import io.netty.channel.MultiThreadIoEventLoopGroup
import io.netty.channel.epoll.Epoll
import io.netty.channel.epoll.EpollChannelOption
import io.netty.channel.epoll.EpollIoHandler
import io.netty.channel.epoll.EpollServerSocketChannel
import io.netty.channel.nio.NioIoHandler
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.codec.ByteToMessageDecoder
import java.lang.management.ManagementFactory
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.LongAdder

/**
 * The second server for the socket scenarios, from main's network scenario (`7de18eb`): Netty, with each delayed
 * answer a task on the connection's own event loop (Netty's scheduled-task queue, which epoll wakes with a timerfd),
 * so no thread is held per request. It speaks what [NetServer] speaks (`GET /?d=<micros>&s=<bytes>`, `/__stats`,
 * `/__reset`) and reports the same lag and CPU, so one scenario can run against both and show whether the server's
 * design changes the result.
 *
 * With epoll, several listeners share the port through `SO_REUSEPORT`, each with its own backlog, because one backlog
 * (capped at `net.core.somaxconn`) overflows when thousands of clients connect at once.
 */
class NettyServer(private val port: Int, threads: Int) {
    @Volatile var lag = Histogram(); private set
    val served = LongAdder()
    private val os = ManagementFactory.getOperatingSystemMXBean() as OperatingSystemMXBean
    @Volatile private var cpuAtReset = os.processCpuTime
    @Volatile private var nanosAtReset = System.nanoTime()
    private val bodies = ConcurrentHashMap<Int, ByteBuf>()
    private val epoll = Epoll.isAvailable()
    private val group = MultiThreadIoEventLoopGroup(threads, if (epoll) EpollIoHandler.newFactory() else NioIoHandler.newFactory())

    fun serve() {
        val listeners = if (epoll) 4 else 1
        val bootstrap = ServerBootstrap()
            .group(group)
            .channel(if (epoll) EpollServerSocketChannel::class.java else NioServerSocketChannel::class.java)
            .option(ChannelOption.SO_BACKLOG, 16_384)
            .option(ChannelOption.SO_REUSEADDR, true)
            .childOption(ChannelOption.TCP_NODELAY, true)
            .childHandler(object : ChannelInitializer<SocketChannel>() {
                override fun initChannel(ch: SocketChannel) {
                    ch.pipeline().addLast(DelayedAnswer())
                }
            })
        if (epoll) bootstrap.option(EpollChannelOption.SO_REUSEPORT, true)
        val channels = List(listeners) { bootstrap.bind("127.0.0.1", port).sync().channel() }
        System.err.println("netty server on $port, epoll=$epoll, listeners=$listeners")
        channels.forEach { it.closeFuture().sync() }
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

    private fun answer(ctx: ChannelHandlerContext, body: ByteBuf) {
        val head = Unpooled.wrappedBuffer("HTTP/1.1 200 OK\r\nContent-Length: ${body.readableBytes()}\r\n\r\n".toByteArray())
        ctx.writeAndFlush(Unpooled.wrappedBuffer(head, body))
    }

    /**
     * A minimal HTTP/1.1 codec, enough for what the clients send: a `GET` with headers and no body. main found
     * Netty's full `HttpServerCodec` cost the one-core server about 14 µs a request.
     */
    private inner class DelayedAnswer : ByteToMessageDecoder() {
        override fun decode(ctx: ChannelHandlerContext, input: ByteBuf, out: MutableList<Any>) {
            while (true) {
                var end = -1
                for (i in input.readerIndex()..input.writerIndex() - 4) {
                    if (input.getInt(i) == CRLFCRLF) { end = i; break }
                }
                if (end < 0) return
                val request = input.toString(input.readerIndex(), end - input.readerIndex(), Charsets.ISO_8859_1)
                input.readerIndex(end + 4)
                when {
                    request.startsWith("GET /__stats") -> answer(ctx, Unpooled.wrappedBuffer(stats().toByteArray()))
                    request.startsWith("GET /__reset") -> { reset(); answer(ctx, Unpooled.wrappedBuffer("{}".toByteArray())) }
                    else -> {
                        val micros = param(request, "d")
                        val size = param(request, "s").toInt()
                        val due = System.nanoTime() + micros * 1_000
                        ctx.executor().schedule({
                            lag.record(System.nanoTime() - due)
                            served.increment()
                            val body = bodies.computeIfAbsent(size) { n ->
                                Unpooled.unreleasableBuffer(Unpooled.wrappedBuffer(ByteArray(n) { 'x'.code.toByte() }))
                            }
                            answer(ctx, body.duplicate())
                        }, micros, TimeUnit.MICROSECONDS)
                    }
                }
            }
        }

        @Deprecated("Deprecated in Java")
        override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) { ctx.close() }
    }

    private companion object {
        const val CRLFCRLF = 0x0d0a0d0a
    }
}

/** `net-server-netty <port> [threads]`: serves until killed. */
fun nettyServerMain(args: List<String>) {
    NettyServer(args[0].toInt(), args.getOrNull(1)?.toInt() ?: 1).serve()
}
