package bvr.arena;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

/**
 * Blocking socket calls for code running in a [Carrier]'s fiber. The caller sees ordinary calls; underneath, a
 * connection's receive is one multishot request into the carrier's [BufferRing], and a write is queued and returns,
 * waiting only if the previous write has not gone yet.
 */
public final class Io {
    private Io() {}

    static final int MSG_NOSIGNAL = 0x4000;
    private static final int IOSQE_BUFFER_SELECT = 1 << 5;
    private static final int RECV_MULTISHOT = 1 << 1;
    private static final int IOSQE_IO_HARDLINK = 1 << 3;
    private static final int FILE_INDEX_ALLOC = -1;

    /** A connection accepted on [listener], or a negative errno. */
    public static int accept(int listener) {
        Carrier c = Carrier.current();
        Fiber f = c.current;
        // Into a free slot of the ring's file table when it has one: the result is then that slot.
        c.ring.prep(Ring.OP_ACCEPT, 0, 0, listener, 0L, 0, 0, 0, c.fileFlag != 0 ? FILE_INDEX_ALLOC : 0,
            Carrier.WAKE | f.id);
        f.outstanding++;
        Fiber.park();
        return f.result;
    }

    /** Bytes read from [fd] into [into] at [at], at most [max]: zero at end of stream, negative on error. */
    public static int read(int fd, byte[] into, int at, int max) {
        while (true) {
            int n = readNow(fd, into, at, max);
            if (n != 0) return n < 0 ? 0 : n;
            awaitData(fd);
        }
    }

    /** Like [read] without waiting: zero when nothing has arrived yet, negative at end of stream or on error. */
    public static int readNow(int fd, byte[] into, int at, int max) {
        Carrier c = Carrier.current();
        Fiber f = c.current;
        while (true) {
            if (f.bufferId >= 0) {
                int n = Math.min(max, f.bufferEnd - f.bufferAt);
                MemorySegment.copy(c.buffers.slab(), JAVA_BYTE, c.buffers.offset(f.bufferId) + f.bufferAt, into, at, n);
                f.bufferAt += n;
                if (f.bufferAt == f.bufferEnd) {
                    c.buffers.recycle(f.bufferId);
                    f.bufferId = -1;
                }
                return n;
            }
            if (!f.hasArrival()) return 0;
            long a = f.nextArrival();
            int bid = (int) (a >> 32);
            int result = (int) a;
            if (result <= 0 || bid < 0) {
                if (bid >= 0) c.buffers.recycle(bid);
                return -1;
            }
            f.bufferId = bid;
            f.bufferAt = 0;
            f.bufferEnd = result;
        }
    }

    /** Parks until something has arrived on [fd] (data, end of stream or an error); always true. */
    public static boolean awaitData(int fd) {
        Carrier c = Carrier.current();
        Fiber f = c.current;
        f.fd = fd;
        while (f.bufferId < 0 && !f.hasArrival()) {
            if (!f.recvArmed) {
                c.ring.prep(Ring.OP_RECV, IOSQE_BUFFER_SELECT | c.fileFlag, RECV_MULTISHOT, fd, 0L, 0, 0,
                    BufferRing.GROUP, 0, Carrier.RECV | f.id);
                f.recvArmed = true;
                f.outstanding++;
            }
            f.waitingRecv = true;
            Fiber.park();
        }
        return true;
    }

    /**
     * Writes [length] bytes of [bytes] to [fd] without waiting: sent now if nothing is in flight, else collected and
     * sent when the send in flight completes. [bytes] is free for reuse as soon as this returns.
     */
    public static void write(int fd, byte[] bytes, int length) throws IOException {
        Carrier c = Carrier.current();
        Fiber f = c.current;
        f.fd = fd;
        if (f.sendError < 0) throw new IOException("send failed: " + f.sendError);
        if (!f.sendInFlight) {
            if (c.ringSends == 0) c.sendRingHead = 0;
            int at = c.sendRingHead;
            if (length <= Carrier.SEND_RING - at) {
                MemorySegment.copy(bytes, 0, c.sendRing, JAVA_BYTE, at, length);
                // Eight-byte aligned, so no two sends share a word.
                c.sendRingHead = (at + length + 7) & ~7;
                c.ringSends++;
                f.ringSend = true;
                send(c, f, c.sendRingAddress + at, length);
            } else {
                f.out = fit(c, f.out, 0, length);
                MemorySegment.copy(bytes, 0, f.out, JAVA_BYTE, 0, length);
                send(c, f, f.out.address(), length);
            }
        } else {
            f.next = fit(c, f.next, f.nextLength, f.nextLength + length);
            MemorySegment.copy(bytes, 0, f.next, JAVA_BYTE, f.nextLength, length);
            f.nextLength += length;
        }
    }

    /** Called by the carrier when [f]'s send has completed and writes collected meanwhile. */
    static void sendNext(Carrier c, Fiber f) {
        c.endRingSend(f);
        MemorySegment sent = f.out;
        f.out = f.next;
        f.next = sent;
        int length = f.nextLength;
        f.nextLength = 0;
        send(c, f, f.out.address(), length);
    }

    private static void send(Carrier c, Fiber f, long address, int length) {
        f.sendAddress = address;
        f.sendLeft = length;
        f.sendInFlight = true;
        c.ring.prep(Ring.OP_SEND, c.fileFlag, 0, f.fd, f.sendAddress, length, MSG_NOSIGNAL, 0, 0, Carrier.SEND | f.id);
        f.outstanding++;
    }

    /**
     * [buffer] if it holds [needed] bytes, else a larger one with its first [keep] bytes copied over: a carrier's
     * slot while that is large enough, then one of its own. Never called on a buffer a send is reading.
     */
    private static MemorySegment fit(Carrier c, MemorySegment buffer, int keep, int needed) {
        if (buffer != null && buffer.byteSize() >= needed) return buffer;
        MemorySegment grown = buffer == null && needed <= SendSlots.SIZE ? c.slots.take()
            : Arena.ofAuto().allocate(Math.max(4096, Integer.highestOneBit(needed - 1) << 1));
        if (keep > 0) MemorySegment.copy(buffer, 0, grown, 0, keep);
        c.slots.give(buffer);
        return grown;
    }


    /**
     * Waits for the last write, then closes [fd], ending a receive still armed on it, through the ring: a direct
     * descriptor has no number the close system call knows. Nothing waits for either; the fiber's id stays taken
     * until both have completed.
     */
    static void finish(int fd) {
        Carrier c = Carrier.current();
        Fiber f = c.current;
        awaitSend(f);
        if (f.recvArmed) {
            // Hard-linked, so the close runs after the shutdown even when the shutdown fails.
            c.ring.prep(Ring.OP_SHUTDOWN, c.fileFlag | IOSQE_IO_HARDLINK, 0, fd, 0L, 2 /* SHUT_RDWR */, 0, 0, 0,
                Carrier.CLOSE | f.id);
            f.outstanding++;
        }
        boolean fixed = c.fileFlag != 0;
        c.ring.prep(Ring.OP_CLOSE, 0, 0, fixed ? 0 : fd, 0L, 0, 0, 0, fixed ? fd + 1 : 0, Carrier.CLOSE | f.id);
        f.outstanding++;
    }

    private static void awaitSend(Fiber f) {
        while (f.sendInFlight || f.nextLength > 0) {
            f.waitingSend = true;
            Fiber.park();
        }
    }
}
