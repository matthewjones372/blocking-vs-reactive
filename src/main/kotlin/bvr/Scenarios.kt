package bvr

/** Every bounded contestant: the I/O scenarios compare all of them. */
val bounded: List<String> = contestants.values.filter { it.model != Model.UNBOUNDED }.map { it.name }

/**
 * The I/O scenarios. Names are `<shape>-<device>`. The concurrency sweep (`c1k`, `c10k`, `c100k`) runs on all three
 * devices so the summary can show whether a ranking survives a change of device. The workload matrix (`w-*`) runs
 * on the dispatched device only, one property changed at a time against `c1k-dispatched`.
 *
 * Item counts are chosen so that one iteration takes about a second for the faster contestants, long enough for the
 * 10–90% steady window to hold several hundred samples.
 */
val ioScenarios: Map<String, IoScenario> = buildList {
    val tenMs = Workload(latencyMicros = 10_000)
    add(IoScenario("bp-dispatched", "dispatched", 10_000, 16, Workload(1_000), contestants = bounded + "vt-naive"))
    for (device in listOf("dispatched", "shared", "native")) {
        add(IoScenario("c1k-$device", device, 100_000, 1_000, tenMs, contestants = bounded))
        add(IoScenario("c10k-$device", device, 600_000, 10_000, tenMs, contestants = bounded))
        add(IoScenario("c100k-$device", device, 600_000, 100_000, tenMs, contestants = bounded))
    }
    // The inline wheel separates the shared device's two differences from the dispatched one: timer capacity and
    // where continuations run. One concurrency is enough to show which matters.
    add(IoScenario("c10k-wheel", "wheel", 600_000, 10_000, tenMs, contestants = bounded))
    // The workload matrix. Each changes one thing from c1k-dispatched.
    add(IoScenario("w-short-dispatched", "dispatched", 600_000, 1_000, Workload(1_000), contestants = bounded))
    add(IoScenario("w-long-dispatched", "dispatched", 200_000, 10_000, Workload(100_000), contestants = bounded))
    add(IoScenario("w-cpu-dispatched", "dispatched", 100_000, 1_000, tenMs.copy(cpuIters = 30_000), contestants = bounded))
    add(IoScenario("w-lognormal-dispatched", "dispatched", 200_000, 1_000, Workload(5_000, Dist.LOGNORMAL), contestants = bounded))
    add(IoScenario("w-errors-dispatched", "dispatched", 100_000, 1_000, tenMs.copy(failEvery = 100), recover = true, contestants = bounded))
    add(IoScenario("w-slow-consumer-dispatched", "dispatched", 100_000, 1_000, Workload(1_000, sinkIters = 10_000), contestants = bounded))
    // A small scenario for CI: proves the harness end to end in seconds, measures nothing worth publishing.
    add(IoScenario("smoke", "dispatched", 2_000, 50, Workload(500, failEvery = 50), recover = true, contestants = bounded + "vt-naive"))
}.associateBy { it.name }

/** The cells of each suite, one `scenario contestant` per line, for scripts/run-suite.py to shuffle and run. */
val suites: Map<String, List<Pair<String, String>>> = mapOf(
    "io" to ioScenarios.values.filter { it.name.startsWith("bp-") || it.name.matches(Regex("c\\d+k-.*")) }
        .flatMap { s -> s.contestants.map { s.name to it } },
    "workload" to ioScenarios.values.filter { it.name.startsWith("w-") }.flatMap { s -> s.contestants.map { s.name to it } },
    "smoke" to ioScenarios.getValue("smoke").let { s -> s.contestants.map { s.name to it } },
)
