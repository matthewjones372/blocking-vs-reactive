package bvr

import io.netty.bootstrap.ServerBootstrap
import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufUtil
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
import java.util.concurrent.TimeUnit

/**
 * The server for the real-network scenario, run in its own JVM so the client's process CPU stays clean:
 * `NetServerKt [port] [delayMicros] [threads]`.
 *
 * It serves HTTP/1.1 with keep-alive, and answers `GET /?x=<n>` with the body `<n>` after `delayMicros`. The delay is
 * a task on the connection's own event loop (Netty's scheduled-task queue, which epoll wakes with a timerfd), so no
 * thread is held per request, and 10,000 waiting requests cost 10,000 small queue entries.
 */
fun main(args: Array<String>) {
    val port = args.getOrNull(0)?.toInt() ?: 8099
    val delayMicros = args.getOrNull(1)?.toLong() ?: 10_000
    val threads = args.getOrNull(2)?.toInt() ?: 1
    val epoll = Epoll.isAvailable()
    val group = MultiThreadIoEventLoopGroup(threads, if (epoll) EpollIoHandler.newFactory() else NioIoHandler.newFactory())
    // The kernel caps each listen backlog at net.core.somaxconn (4,096 here), and 10,000 clients connecting at once
    // overflow one: the dropped handshakes then wait out TCP's retransmit back-off. With epoll, several listeners
    // share the port through SO_REUSEPORT, each with its own backlog, all on the same event loop.
    val listeners = if (epoll) 4 else 1
    val bootstrap = ServerBootstrap()
        .group(group)
        .channel(if (epoll) EpollServerSocketChannel::class.java else NioServerSocketChannel::class.java)
        .option(ChannelOption.SO_BACKLOG, 16_384)
        .option(ChannelOption.SO_REUSEADDR, true)
        .childOption(ChannelOption.TCP_NODELAY, true)
        .childHandler(object : ChannelInitializer<SocketChannel>() {
            override fun initChannel(ch: SocketChannel) {
                ch.pipeline().addLast(DelayedEcho(delayMicros))
            }
        })
    if (epoll) bootstrap.option(EpollChannelOption.SO_REUSEPORT, true)
    val channels = List(listeners) { bootstrap.bind("127.0.0.1", port).sync().channel() }
    println("NETSERVER ready port=$port delayMicros=$delayMicros threads=$threads epoll=$epoll listeners=$listeners")
    channels.forEach { it.closeFuture().sync() }
}

/**
 * A minimal HTTP/1.1 server codec, enough for what the clients send: a `GET` with headers and no body, one at a time
 * per connection (pipelined ones are answered in order too, since equal delays fire in the order they were set).
 * Netty's full `HttpServerCodec` cost the one-core server about 14 µs a request and capped it near 70k requests/s.
 */
private class DelayedEcho(private val delayMicros: Long) : ByteToMessageDecoder() {
    override fun decode(ctx: ChannelHandlerContext, input: ByteBuf, out: MutableList<Any>) {
        while (true) {
            // The end of the headers, `\r\n\r\n`, as an absolute index; none yet means the request is incomplete.
            var end = -1
            for (i in input.readerIndex()..input.writerIndex() - 4) {
                if (input.getInt(i) == CRLFCRLF) { end = i; break }
            }
            if (end < 0) return
            // The request line is `GET /?x=<n> HTTP/1.1`; the answer is `<n>`.
            var from = input.readerIndex()
            while (input.getByte(from) != '='.code.toByte()) from++
            var to = ++from
            while (input.getByte(to) in '0'.code.toByte()..'9'.code.toByte()) to++
            val digits = input.toString(from, to - from, Charsets.US_ASCII)
            input.readerIndex(end + 4)
            ctx.executor().schedule({
                val response = "HTTP/1.1 200 OK\r\ncontent-length: ${digits.length}\r\n\r\n$digits"
                ctx.writeAndFlush(ByteBufUtil.writeAscii(ctx.alloc(), response))
            }, delayMicros, TimeUnit.MICROSECONDS)
        }
    }

    override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) { ctx.close() }

    private companion object {
        const val CRLFCRLF = 0x0d0a0d0a
    }
}
