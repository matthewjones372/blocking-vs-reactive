package bvr.arena

import org.eclipse.jetty.http.HttpHeader
import org.eclipse.jetty.io.Content
import org.eclipse.jetty.server.Handler
import org.eclipse.jetty.server.HttpConfiguration
import org.eclipse.jetty.server.HttpConnectionFactory
import org.eclipse.jetty.server.Request
import org.eclipse.jetty.server.Response
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.server.ServerConnector
import org.eclipse.jetty.util.Callback
import org.eclipse.jetty.util.thread.QueuedThreadPool
import java.io.File
import java.nio.ByteBuffer

/**
 * The same endpoints as [VtArena][main] on a hand-written Jetty 12 core handler: `JettyArenaKt [port] [dataset]`.
 *
 * Declared non-blocking, so Jetty calls it on the selector that read the request, with no dispatch to a pool. This is
 * the floor under Pelican's Jetty backend, which interprets descriptions on top of the same calls.
 */
fun main(args: Array<String>) {
    val port = args.getOrNull(0)?.toInt() ?: 8080
    val items = Dataset.load(File(args.getOrNull(1) ?: "/data/dataset.json"))
    val server = Server(QueuedThreadPool(200))
    val http = HttpConnectionFactory(HttpConfiguration().apply { sendServerVersion = false })
    // A selector per core: the handler runs on the selector, and Jetty's default on two cores is one selector.
    val selectors = Runtime.getRuntime().availableProcessors()
    server.addConnector(ServerConnector(server, 1, selectors, http).apply { this.port = port; acceptQueueSize = 65_535 })
    server.handler = Arena(items)
    server.start()
    println("JETTYARENA ready port=$port items=${items.size}")
    server.join()
}

private class Arena(private val items: List<Dataset.Item>) : Handler.Abstract.NonBlocking() {
    override fun handle(request: Request, response: Response, callback: Callback): Boolean {
        val path = request.httpURI.path
        val query = request.httpURI.query
        when {
            path == "/baseline11" -> {
                val sum = querySum(query)
                if (request.method == "POST") {
                    Content.Source.asStringAsync(request, Charsets.US_ASCII).whenComplete { body, failure ->
                        if (failure != null) callback.failed(failure)
                        else text(response, callback, (sum + body.filter(Char::isDigit).ifEmpty { "0" }.toLong()).toString())
                    }
                } else {
                    text(response, callback, sum.toString())
                }
            }
            path == "/pipeline" -> text(response, callback, "ok")
            path.startsWith("/json/") -> {
                val count = path.substring(6).toIntOrNull()?.coerceIn(0, items.size) ?: 0
                val m = queryParam(query, "m")?.toLongOrNull() ?: 1
                val out = Out(16 * 1024)
                out.jsonBody(items, count, m)
                response.headers.put(HttpHeader.CONTENT_TYPE, "application/json")
                response.headers.put(HttpHeader.CONTENT_LENGTH, out.size.toLong())
                response.write(true, ByteBuffer.wrap(out.bytes, 0, out.size), callback)
            }
            else -> {
                response.status = 404
                text(response, callback, "not found")
            }
        }
        return true
    }

    private fun text(response: Response, callback: Callback, body: String) {
        val bytes = body.toByteArray(Charsets.US_ASCII)
        response.headers.put(HttpHeader.CONTENT_TYPE, "text/plain")
        response.headers.put(HttpHeader.CONTENT_LENGTH, bytes.size.toLong())
        response.write(true, ByteBuffer.wrap(bytes), callback)
    }

    private fun querySum(query: String?): Long =
        query?.split('&')?.sumOf { it.substringAfter('=', "").toLongOrNull() ?: 0 } ?: 0

    private fun queryParam(query: String?, name: String): String? =
        query?.split('&')?.firstOrNull { it.startsWith("$name=") }?.substringAfter('=')
}
