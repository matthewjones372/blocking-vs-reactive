package bvr.arena;

import java.lang.foreign.MemorySegment;

/**
 * A carrier's small send buffers, carved from huge-page slabs: every connection's first two send buffers come from
 * here, packed together, instead of a separate allocation each scattered over the address space. A response that
 * outgrows one moves to a buffer of its own.
 */
final class SendSlots {
    static final int SIZE = 2048;
    private static final int PER_SLAB = (2 << 20) / SIZE;

    private MemorySegment[] free = new MemorySegment[PER_SLAB];
    private int freeCount;

    MemorySegment take() {
        if (freeCount == 0) {
            MemorySegment slab = Sys.hugeSlab((long) SIZE * PER_SLAB);
            if (free.length < PER_SLAB) free = new MemorySegment[PER_SLAB];
            // Highest first, so slots are handed out in address order.
            for (int i = PER_SLAB - 1; i >= 0; i--) give(slab.asSlice((long) i * SIZE, SIZE));
        }
        return free[--freeCount];
    }

    /** Takes back [buffer] if it is one of these slots; anything else is left to the GC. */
    void give(MemorySegment buffer) {
        if (buffer == null || buffer.byteSize() != SIZE) return;
        if (freeCount == free.length) free = java.util.Arrays.copyOf(free, freeCount * 2);
        free[freeCount++] = buffer;
    }
}
