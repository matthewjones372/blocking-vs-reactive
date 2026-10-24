package bvr

import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import java.io.File
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * What each contestant does, beyond its answer. Where models differ, the expectation says which way each goes and
 * why, so a change in a library's behaviour fails here before it changes a published claim. docs/semantics.md is
 * the same observations as a table.
 */
@Execution(ExecutionMode.SAME_THREAD)
class ContestantSemanticsTest {
    companion object {
        @JvmStatic fun bounded() = contestants.values.filter { it.model != Model.UNBOUNDED }.map { it.name }

        /** Waits for every call in flight before it returns, because nothing in it keeps the futures to cancel. */
        val waitsForInFlight = setOf("cf-plain")

        /**
         * A failure reaches the caller in source order, after every earlier answer: with the head held, it waits.
         * Reactor's `flatMapSequential` and Pekko's `mapAsync` fail as soon as any call fails.
         */
        val failsInOrder = setOf("vt-ordered", "lark-mappar", "lark-mapasync")

        /**
         * After a stop, calls in flight keep a callback registered until they complete: they are not cancelled. For
         * blocking contestants the waiting threads are interrupted, and Reactor cancels each future.
         */
        val leavesCallbacksOnStop = setOf("lark-mapasync-unordered", "pekko-mapasync", "pekko-mapasync-unordered", "cf-plain")

        /**
         * Interrupting the caller returns control, but the run goes on and its calls stay awaited: Lark's `await()`
         * cancels the exit future, which does not stop a Forks run (docs/lark-issues.md); Pekko's stream is not tied
         * to the thread that waits for it; cf-plain has nothing to cancel.
         */
        val abandonedOnInterrupt = setOf(
            "lark-mappar", "lark-mappar-unordered", "lark-mapasync", "lark-mapasync-unordered",
            "pekko-mapasync", "pekko-mapasync-unordered", "cf-plain",
        )

        /** The sink runs inside each call's own task, so one slow sink call holds back only that task. */
        val sinkPerTask = setOf("vt-plain", "vt-structured", "cf-plain")

        private val out = System.getProperty("bvr.semantics.out")?.let(::File)?.also { it.delete() }

        private fun record(check: String, c: String, seen: Map<String, Any?>) {
            out?.appendText(Json.write(linkedMapOf<String, Any?>("check" to check, "contestant" to c) + seen) + "\n")
        }
    }

    private fun observe(check: String, name: String, f: (Contestant) -> Map<String, Any?>) =
        f(contestants.getValue(name)).also { record(check, name, it) }

    @ParameterizedTest @MethodSource("bounded")
    fun `answers every element exactly once, in source order when ordered`(name: String) {
        val seen = observe("answers", name, Semantics::answers)
        assertEquals(true, seen["finished"], "$seen")
        assertEquals(true, seen["allOnce"], "$seen")
        if (contestants.getValue(name).ordered) assertEquals(true, seen["inOrder"], "$seen")
    }

    @ParameterizedTest @MethodSource("bounded")
    fun `with nothing answering, starts exactly its bound of calls`(name: String) {
        val seen = observe("bound", name) { Semantics.bound(it) }
        assertEquals(32, seen["pending"], "$seen")
        assertTrue((seen["readAhead"] as Long) <= 16, "read too far ahead of its calls: $seen")
        assertEquals(true, seen["correct"], "$seen")
    }

    @ParameterizedTest @MethodSource("bounded")
    fun `while flowing, never has more than its bound between start and sink`(name: String) {
        val most = Semantics.mostInFlight(contestants.getValue(name))
        record("mostInFlight", name, mapOf("most" to most, "bound" to 32))
        // One more than the bound is an answer that has left the operator and not yet reached the sink.
        assertTrue(most <= 33, "$name had $most in flight against a bound of 32")
    }

    @ParameterizedTest @MethodSource("bounded")
    fun `take stops reading the source and returns, or waits for its calls`(name: String) {
        val seen = observe("take", name, Semantics::take)
        assertEquals(10L, seen["answers"], "$seen")
        assertTrue((seen["produced"] as Long) <= 32 + 10 + 16, "kept reading after take: $seen")
        assertEquals(name !in waitsForInFlight, seen["returned"], "$seen")
        if (seen["returned"] == true) {
            assertEquals(name in leavesCallbacksOnStop, (seen["stillAwaited"] as Int) > 0, "$seen")
        }
    }

    @ParameterizedTest @MethodSource("bounded")
    fun `a failed call fails the run with that failure`(name: String) {
        val seen = observe("failure", name, Semantics::failure)
        assertEquals(true, seen["failedWithIoFailure"], "$seen")
        val waits = name in failsInOrder || name in waitsForInFlight
        assertEquals(!waits, seen["returnedWhileHeld"], "$seen")
        assertTrue((seen["produced"] as Long) <= 32 + 16, "kept reading after the failure: $seen")
    }

    @ParameterizedTest @MethodSource("bounded")
    fun `a call that times out is let go of and the run completes`(name: String) {
        val seen = observe("timeouts", name, Semantics::timeouts)
        assertEquals(true, seen["finished"], "$seen")
        assertEquals(true, seen["correct"], "$seen")
        assertEquals(true, seen["heldCallsReleased"], "a held call was still pending after its timeout: $seen")
    }

    @ParameterizedTest @MethodSource("bounded")
    fun `a slow consumer holds the source back to the bound`(name: String) {
        val seen = observe("slowConsumer", name) { Semantics.slowConsumer(it) }
        assertEquals(true, seen["finished"], "$seen")
        assertTrue((seen["mostAheadOfConsumer"] as Long) <= 32 + 2, "$seen")
    }

    @ParameterizedTest @MethodSource("bounded")
    fun `a paused consumer pauses the source, unless the sink runs per task`(name: String) {
        val seen = observe("demandPause", name) { Semantics.demandPauseGrowth(it) }
        assertEquals(true, seen["correct"], "$seen")
        if (name !in sinkPerTask) assertEquals(0L, seen["readDuringPause"], "$seen")
    }

    @ParameterizedTest @MethodSource("bounded")
    fun `a slow source's elements are not held for a batch`(name: String) {
        val seen = observe("slowProducer", name, Semantics::slowProducer)
        assertEquals(true, seen["finished"], "$seen")
        // Pekko runs a blocking iterator on the stream's own actor, so the stream waits with it; its documented
        // remedy is a blocking-I/O dispatcher behind an async boundary, which this source does not have.
        // Holding elements for a batch of the window (16) would take at least 75 ms at one element per 5 ms; 60 ms
        // leaves room for a loaded machine without letting batching through.
        val limit = if (name.startsWith("pekko")) 200.0 else 60.0
        assertTrue((seen["worstProducedToConsumedMs"] as Double) < limit, "$seen")
    }

    @ParameterizedTest @MethodSource("bounded")
    fun `interrupting the caller returns control, and stops the run or abandons it`(name: String) {
        val seen = observe("interrupt", name, Semantics::interrupt)
        assertEquals(true, seen["callerReturned"], "$seen")
        assertEquals(name in abandonedOnInterrupt, (seen["stillAwaited"] as Int) > 0, "$seen")
    }
}
