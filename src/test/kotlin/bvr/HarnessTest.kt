package bvr

import io.github.matthewjones372.surefold.Limit
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import java.net.ServerSocket
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.LongAdder
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class HarnessTest {
    companion object {
        @JvmStatic fun all() = contestants.keys.toList() + diagnosticContestants.keys
        @JvmStatic fun net() = netContestants
        @JvmStatic fun overload() = overloadContestants.keys.toList()

        /** A server on a free port for the whole class, as a run of the net suite has one. */
        private val port = ServerSocket(0).use { it.localPort }
        private val server by lazy { Thread.ofPlatform().daemon().start { NetServer(port, 2).serve() }.also { Thread.sleep(200) } }
    }

    @Test
    fun `a histogram reports quantiles within its bucket precision`() {
        val h = Histogram()
        for (v in 1..100_000L) h.record(v * 1_000)
        assertEquals(100_000, h.count())
        for ((q, exact) in listOf(0.5 to 50_000_000.0, 0.99 to 99_000_000.0)) {
            val got = h.quantile(q)
            assertTrue(kotlin.math.abs(got - exact) / exact < 0.04, "q$q: $got against $exact")
        }
        assertTrue(Histogram().quantile(0.5).isNaN(), "an empty histogram has no median")
    }

    @Test
    fun `every element's latency and failure are fixed by the element alone`() {
        val w = Workload(5_000, Dist.LOGNORMAL, failEvery = 10)
        val a = (0 until 10_000).map(w::latencyNanos)
        val b = (0 until 10_000).map(w::latencyNanos)
        assertEquals(a, b)
        val median = a.sorted()[a.size / 2]
        assertTrue(median in 4_500_000..5_500_000, "lognormal median $median should be near 5 ms")
        assertEquals(1_000, (0 until 10_000).count(w::fails))
        assertEquals(17, Workload(1, cpuIters = 1_000).transform(17), "the CPU step leaves the answer alone")
    }

    @Test
    fun `the wheel device fires no call early, and late only by about a tick when idle`() {
        for (inline in listOf(true, false)) {
            val d = WheelDevice(inline)
            val firedAt = LongArray(200)
            val t0 = System.nanoTime()
            val calls = (0 until 200).map { x -> d.call(x, 2_000_000L + x * 10_000L, false).thenApply { firedAt[x] = System.nanoTime(); it } }
            CompletableFuture.allOf(*calls.toTypedArray()).get(5, TimeUnit.SECONDS)
            d.close()
            for (x in 0 until 200) assertTrue(firedAt[x] - t0 >= 2_000_000L + x * 10_000L, "call $x fired early")
            assertTrue(d.lag.quantile(0.5) < 2_000_000, "median lag ${d.lag.quantile(0.5)} ns on an idle device")
        }
    }

    @Test
    fun `the held device answers and fails exactly the calls chosen`() {
        val d = HeldDevice()
        val calls = (0 until 10).map { d.call(it, 0, false) }
        assertEquals(10, d.holding)
        d.answer { it < 3 }
        d.fail { it == 5 }
        assertEquals(6, d.holding)
        assertEquals(listOf(0, 1, 2), calls.take(3).map { it.get() })
        assertTrue(calls[5].isCompletedExceptionally)
        d.release()
        assertEquals(0, d.holding)
        assertEquals(9, d.call(9, 0, false).get(), "after release a call answers at once")
    }

    @Test
    fun `the ported limit grants a thousand waiters whose calls fail at once without overflowing the stack`() {
        // The regression behind the trampoline in AsyncLimit.release: each grant ran the next waiter's call, which
        // failed and released, which granted the next, on one stack.
        val limit = AsyncLimit.fixed(1)
        val gate = CompletableFuture<Unit>()
        val first = limit.withPermitAsync { gate }
        val rest = (1..5_000).map { limit.withPermitAsync { CompletableFuture.failedFuture<Unit>(IllegalStateException("boom")) } }
        gate.complete(Unit)
        first.get(5, TimeUnit.SECONDS)
        rest.forEach { f -> runCatching { f.get(5, TimeUnit.SECONDS) } }
        assertTrue(rest.all { it.isDone }, "every waiter got a place")
        assertEquals(CompletableFuture.completedFuture(Unit).get(), limit.acquire().get(1, TimeUnit.SECONDS), "and the place came back")
    }

    @Test
    fun `the ported limit and Surefold's own limit both bound blocking callers`() {
        val port = AsyncLimit.fixed(3)
        val surefold = Limit.fixed(3)
        for (withPermit in listOf<(() -> Unit) -> Unit>({ port.withPermit(it) }, { surefold.withPermit(it) })) {
            val inside = AtomicInteger()
            val most = AtomicInteger()
            Executors.newVirtualThreadPerTaskExecutor().use { threads ->
                repeat(50) { threads.submit { withPermit { most.accumulateAndGet(inside.incrementAndGet(), ::maxOf); Thread.sleep(2); inside.decrementAndGet() } } }
            }
            assertEquals(3, most.get())
        }
    }

    @Test
    fun `every suite names scenarios and contestants that exist`() {
        val known = ioScenarios.keys + memorySuites.values.flatten().map { it.first } + patterns.keys + netScenarios.keys
        for ((suite, cells) in allSuites()) {
            assertTrue(cells.isNotEmpty(), "suite $suite is empty")
            for ((s, c) in cells) {
                assertTrue(s in known, "$suite names unknown scenario $s")
                // A diagnostic contestant only in a diagnostic suite: none may reach a published one.
                val diagnostic = suite.endsWith("-diag") && c in diagnosticContestants
                assertTrue(c in contestants || c in overloadContestants || diagnostic, "$suite names unknown contestant $c")
            }
        }
    }

    @ParameterizedTest @MethodSource("all")
    fun `every contestant gets the smoke scenario's answer on every device`(name: String) {
        val s = ioScenarios.getValue("smoke")
        for (device in listOf("shared", "wheel", "dispatched", "native")) {
            val counters = Counters()
            val setup = ioSetup(device, s.workload, counters)
            val sum = LongAdder()
            onVirtualThread {
                contestant(name).run(
                    Job(counted(s.items, counters), s.concurrency, setup.io, recover = true, sink = summingSink(s.workload, sum, counters)),
                )
            }
            setup.close()
            assertEquals(s.expectedSum, sum.sum(), "$name on $device")
            assertEquals(s.items.toLong(), counters.consumed.sum(), "$name on $device")
        }
    }

    @ParameterizedTest @MethodSource("all")
    fun `every contestant gets the same answer with the CPU step and a slow consumer`(name: String) {
        val counters = Counters()
        val w = Workload(200, Dist.BIMODAL, cpuIters = 2_000, sinkIters = 2_000)
        val setup = ioSetup("dispatched", w, counters)
        val sum = LongAdder()
        onVirtualThread { contestant(name).run(Job(counted(2_000, counters), 64, setup.io, sink = summingSink(w, sum, counters))) }
        setup.close()
        assertEquals(2_000L * 1_999 / 2, sum.sum())
    }

    @ParameterizedTest @MethodSource("net")
    fun `every socket contestant gets the answer from a real server, on both clients`(name: String) {
        server
        for (client in listOf("raw", "jdk")) {
            val s = NetScenario("test", 500, 16, 200, 1_000, client)
            val counters = Counters()
            NetIo(s, port, counters).use { io ->
                val sum = LongAdder()
                onVirtualThread { contestant(name).run(Job(counted(s.items, counters), s.concurrency, io, sink = { y -> sum.add(y.toLong()) })) }
                assertEquals(500L * 499 / 2, sum.sum(), "$name on $client")
            }
        }
        val stats = controlRequest(port, "/__stats")
        assertTrue("\"served\"" in stats, stats)
    }

    @Test
    fun `the request and response parsers find what they need`() {
        val buf = java.nio.ByteBuffer.allocate(256)
        buf.put("GET /?d=1500&s=128 HTTP/1.1\r\nHost: x\r\n\r\nGET /?d".toByteArray())
        val request = takeRequest(buf)!!
        assertEquals(1500L, param(request, "d"))
        assertEquals(128L, param(request, "s"))
        assertEquals(7, buf.position(), "the start of the next request is kept")
        val response = "HTTP/1.1 200 OK\r\nContent-Length: 42\r\n\r\nabc".toByteArray()
        assertEquals(42 to response.size - 3, responseHead(response, response.size))
        assertEquals(null, responseHead(response, 10))
    }

    @ParameterizedTest @MethodSource("overload")
    fun `every overload contestant answers or accounts for every element`(name: String) {
        val p = Pattern("test", 800, listOf(400), ratePerSec = 20_000, deadlineMs = 50, queueLimit = 300) { t -> if (t < 400) 200 else 50 }
        val r = OverloadRun(p)
        onVirtualThread { overloadContestants.getValue(name)(r) }
        r.dependency.close()
        val n = r.pulled.get()
        assertTrue(n > 1_000, "only $n elements pulled")
        assertEquals(0, (0 until n).count { r.outcome[it].toInt() == 0 }, "$name left elements unanswered")
    }
}
