package bvr

/**
 * `Main <scenario> <contestant> [warmups] [iterations]`: one contestant in one fresh JVM, so neither JIT profile nor
 * heap carries over from another. Each measured iteration prints one `RESULT` line; `scripts/run-io.sh` collects them.
 */
val scenarios: Map<String, IoParams> = linkedMapOf(
    "backpressure" to IoParams(items = 20_000, concurrency = 16, latencyMicros = 1_000),
    "fanout-1k" to IoParams(items = 200_000, concurrency = 1_000, latencyMicros = 10_000),
    "fanout-10k" to IoParams(items = 1_000_000, concurrency = 10_000, latencyMicros = 10_000),
    "inflight-100k" to IoParams(items = 2_000_000, concurrency = 100_000, latencyMicros = 10_000),
).let { shared -> shared + shared.map { (name, p) -> "$name-native" to p.copy(native = true) } }

fun main(args: Array<String>) {
    if (args.firstOrNull() == "stacktraces") return stackTraces()
    val scenario = args[0]
    val name = args[1]
    val warmups = args.getOrNull(2)?.toInt() ?: 3
    val iterations = args.getOrNull(3)?.toInt() ?: 5
    val p = scenarios.getValue(scenario)
    val contestant = ioContestants.getValue(name)
    val expected = p.items.toLong() * (p.items - 1) / 2

    repeat(warmups) { Jvm.measure { probe -> check(contestant.run(p, probe) == expected) } }
    repeat(iterations) { i ->
        System.gc()
        val m = Jvm.measure { probe -> check(contestant.run(p, probe) == expected) }
        val opsPerSec = p.items / (m.wallMs / 1000)
        println(
            ("RESULT scenario=$scenario contestant=$name iteration=$i wallMs=%.1f opsPerSec=%.0f cpuMs=%.1f " +
                "cpuUsPerOp=%.2f allocMb=%.1f allocBytesPerOp=%.0f gcCount=%d gcMs=%d maxOutstanding=%d")
                .format(
                    m.wallMs, opsPerSec, m.cpuMs, m.cpuMs * 1000 / p.items, m.allocatedMb,
                    m.allocatedMb * 1e6 / p.items, m.gcCount, m.gcMs, m.maxOutstanding,
                ),
        )
    }
    if (p.concurrency >= 1_000) {
        val perOp = Jvm.liveBytesPerInFlight(p.concurrency) { probe -> contestant.run(p, probe) }
        println("RESULT scenario=$scenario contestant=$name liveBytesPerInFlight=%.0f".format(perOp))
    }
    println("RESULT scenario=$scenario contestant=$name rssHighWaterMb=%.0f".format(Jvm.rssHighWaterMb()))
    System.exit(0)
}
