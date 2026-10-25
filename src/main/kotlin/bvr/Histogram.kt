package bvr

import java.util.concurrent.atomic.AtomicLongArray
import java.util.concurrent.atomic.LongAdder

/**
 * A log-linear histogram of non-negative longs (nanoseconds, here): 32 linear sub-buckets per power of two, so a
 * reported percentile is within about 3% of the true value. Any thread may record; reads are approximate while
 * recording continues, and exact once it has stopped.
 */
class Histogram {
    private val counts = AtomicLongArray(BUCKETS)
    private val total = LongAdder()
    private val sum = LongAdder()

    fun record(value: Long) {
        val v = if (value < 0) 0 else value
        counts.incrementAndGet(index(v))
        total.increment()
        sum.add(v)
    }

    fun count(): Long = total.sum()

    fun mean(): Double = if (count() == 0L) Double.NaN else sum.sum().toDouble() / count()

    /** The value at quantile [q] in 0..1, as the midpoint of its bucket; NaN when nothing was recorded. */
    fun quantile(q: Double): Double {
        val n = count()
        if (n == 0L) return Double.NaN
        val rank = maxOf(1L, kotlin.math.ceil(q * n).toLong())
        var seen = 0L
        for (i in 0 until BUCKETS) {
            seen += counts.get(i)
            if (seen >= rank) return midpoint(i)
        }
        return midpoint(BUCKETS - 1)
    }

    fun max(): Double {
        for (i in BUCKETS - 1 downTo 0) if (counts.get(i) > 0) return midpoint(i)
        return Double.NaN
    }

    /** Count of recorded values strictly above [threshold], to bucket precision. */
    fun countAbove(threshold: Long): Long {
        var n = 0L
        for (i in index(threshold) + 1 until BUCKETS) n += counts.get(i)
        return n
    }

    companion object {
        private const val SUB_BITS = 5
        private const val SUB = 1 shl SUB_BITS
        private const val BUCKETS = 64 * SUB

        internal fun index(v: Long): Int {
            if (v < SUB) return v.toInt()
            val exp = 63 - java.lang.Long.numberOfLeadingZeros(v)
            val shift = exp - SUB_BITS
            val mantissa = (v ushr shift).toInt() - SUB
            return (shift + 1) * SUB + mantissa
        }

        internal fun midpoint(i: Int): Double {
            if (i < SUB) return i.toDouble()
            val shift = i / SUB - 1
            val m = (i % SUB + SUB).toLong()
            val lower = m shl shift
            val upper = ((m + 1) shl shift) - 1
            return (lower + upper) / 2.0
        }
    }
}
