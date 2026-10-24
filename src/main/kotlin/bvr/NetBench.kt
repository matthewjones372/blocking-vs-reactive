package bvr

import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.LongAdder
import reactor.core.publisher.Mono

/**
 * A socket scenario: [items] requests, each answered after [serviceMicros] with [payload] bytes, at most
 * [concurrency] in flight. [client] picks the clients:
 *
 * - `raw`: blocking contestants read [BlockingHttp], the rest [AsyncHttp]. Same protocol code, different waiting.
 * - `jdk`: blocking contestants call `HttpClient.send`, the rest `sendAsync`. Same library, different waiting.
 * - `netty`: Reactor Netty's `HttpClient`, the client a Reactor service would use; run by the Reactor contestants only.
 *
 * [server] is `nio` ([NetServer], on `bvr.netPort`) or `netty` ([NettyServer], on the port after it), so the suite
 * can show that the server's design does not decide the result. [jvmOpts] are added to the client JVM's options:
 * the scheduler and poller settings a virtual-thread service could change.
 */
data class NetScenario(
    val name: String,
    val items: Int,
    val concurrency: Int,
    val serviceMicros: Long,
    val payload: Int,
    val client: String,
    val contestants: List<String> = netContestants,
    val server: String = "nio",
    val jvmOpts: String = "",
)

val netContestants = listOf("vt-plain", "lark-mappar", "cf-plain", "lark-mapasync", "reactor-flatmap", "pekko-mapasync-unordered")

private val reactorOnly = listOf("reactor-flatmap", "reactor-flatmapsequential")
private val blockingOnly = listOf("vt-plain", "lark-mappar")

val netScenarios: Map<String, NetScenario> = buildList {
    for (client in listOf("raw", "jdk", "netty")) {
        val who = if (client == "netty") reactorOnly else netContestants
        add(NetScenario("n-fast-$client", 200_000, 64, 1_000, 128, client, who))
        add(NetScenario("n-wide-$client", 200_000, 1_000, 20_000, 128, client, who))
        add(NetScenario("n-large-$client", 100_000, 256, 5_000, 16_384, client, who))
        // Ten thousand connections, where the JDK client's pool was seen to collapse on main (3416fa3..7de18eb).
        add(NetScenario("n-10k-$client", 50_000, 10_000, 10_000, 128, client, who))
    }
    // The same calls against main's Netty server: does the server's design change the result?
    add(NetScenario("n-wide-raw-nettyserver", 200_000, 1_000, 20_000, 128, "raw", server = "netty"))
    // The JDK's virtual-thread knobs, for the contestants they touch: platform poller threads instead of virtual
    // ones, and a scheduler with two carriers instead of one per core.
    for (shape in listOf("wide", "10k")) {
        val base = if (shape == "wide") Triple(200_000, 1_000, 20_000L) else Triple(50_000, 10_000, 10_000L)
        add(NetScenario("n-$shape-raw-poller1", base.first, base.second, base.third, 128, "raw", blockingOnly, jvmOpts = "-Djdk.pollerMode=1"))
        add(NetScenario("n-$shape-raw-parallelism2", base.first, base.second, base.third, 128, "raw", blockingOnly,
            jvmOpts = "-Djdk.virtualThreadScheduler.parallelism=2"))
    }
    add(NetScenario("n-smoke-raw", 2_000, 16, 500, 128, "raw"))
    add(NetScenario("n-smoke-jdk", 2_000, 16, 500, 128, "jdk"))
    add(NetScenario("n-smoke-netty", 2_000, 16, 500, 128, "netty", reactorOnly))
    add(NetScenario("n-smoke-raw-nettyserver", 2_000, 16, 500, 128, "raw", listOf("vt-plain", "reactor-flatmap"), server = "netty"))
}.associateBy { it.name }

val netSuites: Map<String, List<Pair<String, String>>> = mapOf(
    "net" to netScenarios.values.filter { "smoke" !in it.name }.flatMap { s -> s.contestants.map { s.name to it } },
    "net-smoke" to netScenarios.values.filter { "smoke" in it.name }.flatMap { s -> s.contestants.map { s.name to it } },
)

/** The I/O call over a real socket: each form checks the body's length, then answers the element itself. */
class NetIo(private val s: NetScenario, port: Int, override val counters: Counters) : Io, AutoCloseable {
    override val workload = Workload(latencyMicros = s.serviceMicros)

    // Opened on first use: a contestant uses one form, and two pools of 10,000 connections would not fit in the
    // process's file descriptors.
    private val blockingRawLazy = lazy { BlockingHttp(port, s.concurrency) }
    private val asyncRawLazy = lazy { AsyncHttp(port, s.concurrency) }
    private val blockingRaw by blockingRawLazy
    private val asyncRaw by asyncRawLazy
    private val jdk = if (s.client == "jdk") JdkHttp(port) else null
    private val netty = if (s.client == "netty") NettyHttp(port, s.concurrency) else null

    private fun answered(x: Int, length: Int): Int {
        check(length == s.payload) { "expected ${s.payload} bytes, got $length" }
        counters.ioDone.increment()
        return x
    }

    override fun blocking(x: Int): Int {
        counters.started.increment()
        val length = when (s.client) {
            "raw" -> blockingRaw.get(s.serviceMicros, s.payload)
            "jdk" -> jdk!!.send(s.serviceMicros, s.payload)
            else -> error("${s.client} clients are asynchronous only")
        }
        return answered(x, length)
    }

    override fun async(x: Int): CompletableFuture<Int> {
        counters.started.increment()
        val f = when (s.client) {
            "raw" -> asyncRaw.get(s.serviceMicros, s.payload)
            "jdk" -> jdk!!.sendAsync(s.serviceMicros, s.payload)
            else -> netty!!.get(s.serviceMicros, s.payload).toFuture()
        }
        return f.thenApply { answered(x, it) }
    }

    override fun mono(x: Int): Mono<Int> {
        if (netty == null) return Mono.fromFuture(async(x))
        counters.started.increment()
        return netty.get(s.serviceMicros, s.payload).map { answered(x, it) }
    }

    override fun close() {
        if (blockingRawLazy.isInitialized()) blockingRaw.close()
        if (asyncRawLazy.isInitialized()) asyncRaw.close()
        jdk?.close()
        netty?.close()
    }
}

/** `net <scenario> <contestant> [warmups] [iterations]`, against a server already listening on `bvr.netPort`. */
fun netMain(args: List<String>) {
    val s = netScenarios.getValue(args[0])
    val c = contestants.getValue(args[1])
    val warmups = args.getOrNull(2)?.toInt() ?: 2
    val iterations = args.getOrNull(3)?.toInt() ?: 3
    // The netty server listens on the port after the nio one.
    val port = System.getProperty("bvr.netPort", "18080").toInt() + if (s.server == "netty") 1 else 0
    // The JDK client keeps every idle connection (unbounded by default) for ten minutes, so none closes between
    // iterations: explicit, as on main's network scenario.
    System.getProperties().putIfAbsent("jdk.httpclient.connectionPoolSize", "20000")
    System.getProperties().putIfAbsent("jdk.httpclient.keepalive.timeout", "600")
    var ok = true
    emit(linkedMapOf<String, Any?>("kind" to "calibration", "scenario" to s.name, "contestant" to c.name,
        "nsPerKiloRounds" to Jvm.calibrate()) + environment())
    for (i in 0 until warmups + iterations) {
        val warmup = i < warmups
        val counters = Counters()
        val io = NetIo(s, port, counters)
        val sum = LongAdder()
        val job = Job(counted(s.items, counters), s.concurrency, io, sink = { y -> sum.add(y.toLong()); counters.consumed.increment() })
        if (!warmup) { System.gc(); System.gc() }
        controlRequest(port, "/__reset")
        val sampler = Sampler(counters, s.items.toLong())
        val before = Jvm.Snapshot()
        val failure = runCatching { onVirtualThread { c.run(job) } }.exceptionOrNull()
        val after = Jvm.Snapshot()
        sampler.stop()
        val server = controlRequest(port, "/__stats")
        io.close()
        val valid = failure == null && sum.sum() == s.items.toLong() * (s.items - 1) / 2
        ok = ok && valid
        emit(
            linkedMapOf<String, Any?>(
                "kind" to "net", "scenario" to s.name, "contestant" to c.name, "model" to c.model, "ordered" to c.ordered,
                "client" to s.client, "server" to s.server, "jvmOpts" to s.jvmOpts, "items" to s.items, "concurrency" to s.concurrency, "serviceMicros" to s.serviceMicros,
                "payload" to s.payload, "iteration" to i - (if (warmup) 0 else warmups), "warmup" to warmup, "valid" to valid,
                "error" to failure?.let { "${it::class.simpleName}: ${it.message}" },
            ) + Jvm.delta(before, after, s.items.toLong()) + linkedMapOf(
                "steadyOpsPerSec" to sampler.steadyOpsPerSec, "pendingMean" to sampler.pendingMean,
                "pendingMax" to sampler.pendingMax, "bufferedMean" to sampler.bufferedMean,
                "impliedLatencyMs" to sampler.pendingMean / sampler.steadyOpsPerSec * 1000,
                "rssMeanMb" to sampler.rssMeanMb,
                // The server's own view of the same iteration, raw: the summary checks its lag and CPU.
                "serverStats" to server,
            ) + environment(),
        )
    }
    if (!ok) System.err.println("INVALID: ${s.name} ${c.name}")
    System.exit(if (ok) 0 else 3)
}
