package bvr.arena;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.VarHandle;

import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;
import static java.lang.foreign.ValueLayout.JAVA_SHORT;

/**
 * Receive buffers the kernel picks from itself: a multishot receive takes the next free one for each arrival, and
 * the completion names it. A buffer goes back with [recycle] once its bytes are copied out.
 */
final class BufferRing {
    static final int GROUP = 0;
    private static final int REGISTER_PBUF_RING = 22;
    private static final VarHandle SHORT = JAVA_SHORT.varHandle();

    private final MemorySegment ring;
    private final MemorySegment slab;
    private final int mask;
    final int size;
    private int tail;

    BufferRing(Ring owner, int count, int size) {
        this.size = size;
        this.mask = count - 1;
        // Global: lives as long as the carrier, which never ends, and global memory skips the per-access liveness check.
        Arena arena = Arena.global();
        ring = arena.allocate(16L * count, 4096);
        slab = Sys.hugeSlab((long) size * count);
        MemorySegment reg = arena.allocate(40); // struct io_uring_buf_reg
        reg.set(JAVA_LONG, 0, ring.address());
        reg.set(JAVA_INT, 8, count);
        owner.register(REGISTER_PBUF_RING, reg, 1);
        for (int bid = 0; bid < count; bid++) recycle(bid);
    }

    /** Where buffer [bid]'s bytes are, within [slab()]. */
    long offset(int bid) {
        return (long) bid * size;
    }

    MemorySegment slab() {
        return slab;
    }

    void recycle(int bid) {
        // struct io_uring_buf: addr, len, bid. The ring's tail is the first entry's last two bytes.
        long e = 16L * (tail & mask);
        ring.set(JAVA_LONG, e, slab.address() + (long) bid * size);
        ring.set(JAVA_INT, e + 8, size);
        ring.set(JAVA_SHORT, e + 12, (short) bid);
        tail++;
        SHORT.setRelease(ring, 14L, (short) tail);
    }
}
