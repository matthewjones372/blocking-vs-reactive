package bvr

import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.LongAdder

/**
 * A socket scenario: [items] requests to the [NetServer] on `bvr.netPort`, each answered after [serviceMicros] with
 * [payload] bytes, at most [concurrency] in flight. [client] picks the pair of clients:
 *
 * - `raw`: blocking contestants read [BlockingHttp], the rest [AsyncHttp]. Same protocol code, different waiting.
 * - `jdk`: blocking contestants call `HttpClient.send`, the rest `sendAsync`. Same library, different waiting.
 */
data class NetScenario(
    val name: String,
    val items: Int,
    val concurrency: Int,
    val serviceMicros: Long,
    val payload: Int,
    val client: String,
)

val netContestants = listOf("vt-plain", "lark-mappar", "cf-plain", "lark-mapasync", "reactor-flatmap", "pekko-mapasync-unordered")

val netScenarios: Map<String, NetScenario> = buildList {
    for (client in listOf("raw", "jdk")) {
        add(NetScenario("n-fast-$client", 200_000, 64, 1_000, 128, client))
        add(NetScenario("n-wide-$client", 200_000, 1_000, 20_000, 128, client))
        add(NetScenario("n-large-$client", 100_000, 256, 5_000, 16_384, client))
    }
    add(NetScenario("n-smoke-raw", 2_000, 16, 500, 128, "raw"))
    add(NetScenario("n-smoke-jdk", 2_000, 16, 500, 128, "jdk"))
}.associateBy { it.name }

val netSuites: Map<String, List<Pair<String, String>>> = mapOf(
    "net" to netScenarios.values.filter { "smoke" !in it.name }.flatMap { s -> netContestants.map { s.name to it } },
    "net-smoke" to netScenarios.values.filter { "smoke" in it.name }.flatMap { s -> netContestants.map { s.name to it } },
)

/** The I/O call over a real socket: each form checks the body's length, then answers the element itself. */
class NetIo(private val s: NetScenario, port: Int, override val counters: Counters) : Io, AutoCloseable {
    override val workload = Workload(latencyMicros = s.serviceMicros)
    private val blockingRaw = if (s.client == "raw") BlockingHttp(port, s.concurrency) else null
    private val asyncRaw = if (s.client == "raw") AsyncHttp(port, s.concurrency) else null
    private val jdk = if (s.client == "jdk") JdkHttp(port) else null

    private fun answered(x: Int, length: Int): Int {
        check(length == s.payload) { "expected ${s.payload} bytes, got $length" }
        counters.ioDone.increment()
        return x
    }

    override fun blocking(x: Int): Int {
        counters.started.increment()
        val length = blockingRaw?.get(s.serviceMicros, s.payload) ?: jdk!!.send(s.serviceMicros, s.payload)
        return answered(x, length)
    }

    override fun async(x: Int): CompletableFuture<Int> {
        counters.started.increment()
        val f = asyncRaw?.get(s.serviceMicros, s.payload) ?: jdk!!.sendAsync(s.serviceMicros, s.payload)
        return f.thenApply { answered(x, it) }
    }

    override fun close() {
        blockingRaw?.close()
        asyncRaw?.close()
        jdk?.close()
    }
}

/** `net <scenario> <contestant> [warmups] [iterations]`, against a server already listening on `bvr.netPort`. */
fun netMain(args: List<String>) {
    val s = netScenarios.getValue(args[0])
    val c = contestants.getValue(args[1])
    val warmups = args.getOrNull(2)?.toInt() ?: 2
    val iterations = args.getOrNull(3)?.toInt() ?: 3
    val port = System.getProperty("bvr.netPort", "18080").toInt()
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
                "client" to s.client, "items" to s.items, "concurrency" to s.concurrency, "serviceMicros" to s.serviceMicros,
                "payload" to s.payload, "iteration" to i - (if (warmup) 0 else warmups), "warmup" to warmup, "valid" to valid,
                "error" to failure?.let { "${it::class.simpleName}: ${it.message}" },
            ) + Jvm.delta(before, after, s.items.toLong()) + linkedMapOf(
                "steadyOpsPerSec" to sampler.steadyOpsPerSec, "pendingMean" to sampler.pendingMean,
                "pendingMax" to sampler.pendingMax, "bufferedMean" to sampler.bufferedMean,
                "impliedLatencyMs" to sampler.pendingMean / sampler.steadyOpsPerSec * 1000,
                "rssMeanMb" to sampler.rssMeanMb,
                // The server's own view of the same iteration, raw: the summary checks its lag and CPU.
                "server" to server,
            ) + environment(),
        )
    }
    if (!ok) System.err.println("INVALID: ${s.name} ${c.name}")
    System.exit(if (ok) 0 else 3)
}
