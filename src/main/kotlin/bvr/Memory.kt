package bvr

import java.lang.management.ManagementFactory
import java.util.concurrent.atomic.LongAdder
import javax.management.ObjectName

/**
 * Retained memory per operation, measured where nothing moves.
 *
 * A [HeldDevice] answers no call, so a pipeline started with bound `n` settles with exactly `n` calls pending and
 * stays there. Once it has settled (the device holds `n` calls, and no counter has moved for [SETTLE_MS]), a class
 * histogram of the live heap is taken (it runs a full GC first), along with heap use, RSS and, when the JVM runs with
 * `-XX:NativeMemoryTracking=summary`, the native memory summary. The device then answers everything and the run is
 * checked for the right answer.
 *
 * This is done at two bounds in the same JVM, and the bytes per pending call are the slope between them: whatever a
 * pipeline holds regardless of `n` (its operators, its pools, classes loaded on first use) cancels. The slope covers
 * everything that grows with `n`: pending futures and callbacks, parked virtual threads and their stack chunks,
 * subscribers, window slots. It does not cover carrier or platform thread stacks, which are native and do not grow
 * with `n` for any contestant here; NMT and RSS show those.
 *
 * In `hol` mode the device holds only element 0 and answers the rest at once. An ordered contestant then fills its
 * window with `n - 1` answered but unconsumed results behind the one head that will not answer: the cost of buffering
 * completed results, as distinct from pending ones. Unordered contestants do not buffer there, so `hol` runs only the
 * ordered ones.
 */
private const val SETTLE_MS = 300L
private const val SETTLE_TIMEOUT_MS = 60_000L

private val diagnostics = ObjectName("com.sun.management:type=DiagnosticCommand")

private fun diagnostic(name: String, vararg args: String): String =
    ManagementFactory.getPlatformMBeanServer().invoke(
        diagnostics, name, arrayOf<Any>(arrayOf(*args)), arrayOf(Array<String>::class.java.name),
    ) as String

/** Live bytes by class, from `GC.class_histogram` (which runs a full GC), and the total. */
fun liveHistogram(): Pair<Long, Map<String, Long>> {
    val byClass = HashMap<String, Long>()
    var total = 0L
    for (line in diagnostic("gcClassHistogram").lines()) {
        val parts = line.trim().split(Regex("\\s+"))
        if (parts.firstOrNull() == "Total" && parts.size >= 3) total = parts[2].toLong()
        if (parts.size >= 4 && parts[0].endsWith(":")) byClass[parts[3]] = (byClass[parts[3]] ?: 0) + parts[2].toLong()
    }
    return total to byClass
}

/** Committed KB by NMT category, or empty when native memory tracking is off. */
fun nativeMemory(): Map<String, Long> = try {
    val text = diagnostic("vmNativeMemory", "summary")
    val total = Regex("Total: reserved=(\\d+)KB, committed=(\\d+)KB").find(text)?.groupValues?.get(2)?.toLong()
    Regex("-\\s+([A-Za-z][A-Za-z ]+?) \\(reserved=\\d+KB, committed=(\\d+)KB\\)").findAll(text)
        .associate { it.groupValues[1].trim() to it.groupValues[2].toLong() } + listOfNotNull(total?.let { "Total" to it })
} catch (_: Exception) {
    emptyMap()
}

private fun platformThreads() = ManagementFactory.getThreadMXBean().threadCount

class Settled(val ok: Boolean, val waitedMs: Long, val counters: Map<String, Long>)

private fun Counters.snapshot() = mapOf(
    "produced" to produced.sum(), "started" to started.sum(), "ioDone" to ioDone.sum(), "consumed" to consumed.sum(),
)

/** Waits until [ready] holds and no counter has moved for [SETTLE_MS]. */
private fun settle(counters: Counters, ready: () -> Boolean): Settled {
    val t0 = System.currentTimeMillis()
    var last = counters.snapshot()
    var stableSince = System.currentTimeMillis()
    while (System.currentTimeMillis() - t0 < SETTLE_TIMEOUT_MS) {
        Thread.sleep(20)
        val now = counters.snapshot()
        if (now != last) { last = now; stableSince = System.currentTimeMillis() }
        if (ready() && System.currentTimeMillis() - stableSince >= SETTLE_MS) {
            return Settled(true, System.currentTimeMillis() - t0, now)
        }
    }
    return Settled(false, System.currentTimeMillis() - t0, last)
}

/** One held measurement of [c] at bound [n]; returns the live histogram so the caller can diff two bounds. */
fun heldMeasurement(c: Contestant, n: Int, mode: String, repeat: Int): Map<String, Long>? {
    val counters = Counters()
    val device = if (mode == "hol") HeldDevice { it == 0 } else HeldDevice()
    val workload = Workload(latencyMicros = 0)
    val items = n * 2
    val sum = LongAdder()
    val job = Job(counted(items, counters), n, DeviceIo(device, workload, counters),
        sink = { y -> sum.add(y.toLong()); counters.consumed.increment() })
    val (baseline, _) = liveHistogram()
    val baseRss = Jvm.rssMb()
    var failure: Throwable? = null
    val runner = Thread.ofVirtual().start { try { c.run(job) } catch (t: Throwable) { failure = t } }
    val settled = settle(counters) {
        if (mode == "hol") counters.started.sum() - counters.consumed.sum() >= n else device.holding >= n
    }
    val (live, byClass) = liveHistogram()
    val heapUsed = ManagementFactory.getMemoryMXBean().heapMemoryUsage.used
    val rss = Jvm.rssMb()
    val nmt = nativeMemory()
    val threads = platformThreads()
    val pending = device.holding.toLong()
    device.release()
    runner.join()
    val expected = items.toLong() * (items - 1) / 2
    val valid = failure == null && sum.sum() == expected && settled.ok
    emit(
        linkedMapOf<String, Any?>(
            "kind" to "memory", "mode" to mode, "contestant" to c.name, "model" to c.model, "ordered" to c.ordered,
            "concurrency" to n, "repeat" to repeat, "settled" to settled.ok, "settleMs" to settled.waitedMs,
            "valid" to valid, "error" to failure?.toString(),
            "pending" to pending,
            "buffered" to (settled.counters.getValue("ioDone") - settled.counters.getValue("consumed")),
            "queued" to (settled.counters.getValue("produced") - settled.counters.getValue("started")),
            "liveBytes" to live, "baselineLiveBytes" to baseline, "retainedBytes" to live - baseline,
            "heapUsedBytes" to heapUsed, "rssMb" to rss, "rssDeltaMb" to rss - baseRss,
            "platformThreads" to threads,
            "nmtCommittedKb" to nmt,
        ) + environment(),
    )
    return if (valid) byClass else null
}

/**
 * `memory <contestant> <mode> <low> <high> [repeats]`: [repeats] rounds of a held measurement at [low] then at
 * [high], each followed by the classes whose live bytes grew most between the two.
 */
fun memoryMain(args: List<String>) {
    val c = contestants.getValue(args[0])
    val mode = args[1]
    val low = args[2].toInt()
    val high = args[3].toInt()
    val repeats = args.getOrNull(4)?.toInt() ?: 3
    // A first, unmeasured round loads every class and compiles the paths the measured rounds take.
    heldMeasurement(c, low, mode, repeat = -1)
    var ok = true
    repeat(repeats) { r ->
        val a = heldMeasurement(c, low, mode, r)
        val b = heldMeasurement(c, high, mode, r)
        if (a == null || b == null) { ok = false; return@repeat }
        val growth = (b.keys + a.keys).associateWith { (b[it] ?: 0) - (a[it] ?: 0) }
            .entries.sortedByDescending { it.value }.take(12)
            .map { mapOf("class" to it.key, "bytesPerOp" to it.value.toDouble() / (high - low)) }
        emit(linkedMapOf<String, Any?>("kind" to "memory-classes", "mode" to mode, "contestant" to c.name,
            "low" to low, "high" to high, "repeat" to r, "classes" to growth) + environment())
    }
    if (!ok) System.err.println("INVALID: ${c.name} $mode did not settle or did not answer correctly")
    System.exit(if (ok) 0 else 3)
}

val memorySuites: Map<String, List<Pair<String, String>>> = mapOf(
    "memory" to bounded.map { "pending" to it } + contestants.values.filter { it.ordered }.map { "hol" to it.name },
)
