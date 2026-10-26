package bvr

import io.github.matthewjones372.lark.await
import io.github.matthewjones372.lark.stream.Exit
import io.github.matthewjones372.lark.stream.Forks
import io.github.matthewjones372.lark.stream.Stream
import io.github.matthewjones372.lark.stream.from
import io.github.matthewjones372.lark.stream.mapAsync
import io.github.matthewjones372.lark.stream.mapPar
import io.github.matthewjones372.lark.stream.run
import io.github.matthewjones372.lark.stream.runFold
import io.github.matthewjones372.lark.stream.start
import org.apache.pekko.stream.javadsl.Sink
import org.apache.pekko.stream.javadsl.Source
import org.junit.jupiter.api.Test
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * The minimal reproductions behind docs/surefold-issues.md, with nothing of the harness in them. Each asserts what Surefold
 * does at the commit in surefold.commit; if Surefold changes, the test fails and the document is out of date.
 */
class SurefoldIssuesTest {
    /** Four bodies blocked on a latch inside `mapPar(4)`, counted while they are inside. */
    private class Blocked {
        val inside = AtomicInteger()
        val started = CountDownLatch(4)
        val release = CountDownLatch(1)

        fun body(x: Int): Int {
            inside.incrementAndGet()
            started.countDown()
            try {
                release.await()
            } finally {
                inside.decrementAndGet()
            }
            return x
        }
    }

    @Test
    fun `issue 1 - interrupting a caller in await() leaves the Forks run going`() {
        val b = Blocked()
        val caller = Thread.ofVirtual().start {
            try {
                Stream.from(1..100).mapPar(4) { b.body(it) }.runFold(0) { s, x -> s + x }.run(Forks()).await()
            } catch (_: InterruptedException) {
                // The caller gives up.
            }
        }
        b.started.await()
        caller.interrupt()
        caller.join()
        Thread.sleep(300)
        assertEquals(4, b.inside.get(), "the run's bodies are still blocked after the caller returned")
        b.release.countDown()
    }

    @Test
    fun `issue 1, for contrast - closing the Running stops the run and interrupts its bodies`() {
        val b = Blocked()
        val running = Stream.from(1..100).mapPar(4) { b.body(it) }.runFold(0) { s, x -> s + x }.start(Forks())
        b.started.await()
        running.close()
        assertEquals(0, b.inside.get())
        assertTrue(running.exit.toCompletableFuture().get() is Exit.Done)
    }

    @Test
    fun `issue 2 - an ordered mapAsync reports a failure only once every earlier stage has answered`() {
        val never = CompletableFuture<Int>()
        val run = Stream.from(1..10)
            .mapAsync(4) { x -> if (x == 1) never else if (x == 2) CompletableFuture.failedFuture(IllegalStateException("x=2")) else CompletableFuture.completedFuture(x) }
            .runFold(0) { s, x -> s + x }.start(Forks())
        Thread.sleep(500)
        assertFalse(run.exit.toCompletableFuture().isDone, "element 2 failed, but the run waits on element 1")
        run.close()
    }

    @Test
    fun `issue 2, for contrast - Pekko's mapAsync fails the stream as soon as any stage fails`() {
        val never = CompletableFuture<Int>()
        val done = Source.range(1, 10)
            .mapAsync(4) { x -> if (x == 1) never else if (x == 2) CompletableFuture.failedFuture(IllegalStateException("x=2")) else CompletableFuture.completedFuture(x) }
            .runWith(Sink.fold(0) { s, x -> s + x }, pekkoSystem).toCompletableFuture()
        val failure = runCatching { done.get(2, TimeUnit.SECONDS) }.exceptionOrNull()
        assertTrue(failure is ExecutionException && failure.cause is IllegalStateException, "$failure")
    }
}
