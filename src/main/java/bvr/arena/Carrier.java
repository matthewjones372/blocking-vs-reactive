package bvr.arena;

import java.lang.foreign.MemorySegment;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.function.IntConsumer;

/**
 * One core's runtime: a platform thread with its own io_uring and its own listening socket. It runs every fiber that
 * is ready, then sends all their queued I/O in one system call and collects what has completed, and again. A fiber
 * never leaves the carrier that started it, so nothing here is shared between threads.
 */
public final class Carrier extends Thread implements Ring.Completions {
    // A request's user_data: the fiber's id in the low 32 bits, what the request was above it.
    static final long WAKE = 0L;
    static final long RECV = 1L << 32;
    static final long SEND = 2L << 32;
    static final long CLOSE = 3L << 32;

    private static final int CQE_F_BUFFER = 1;
    private static final int CQE_F_MORE = 2;
    /** Direct descriptors per carrier; the kernel caps a table at RLIMIT_NOFILE. */
    private static final int FILES = 16384;
    static final int SEND_RING = 2 << 20;

    private final int port;
    private final IntConsumer onConnection;
    private final ArrayDeque<Fiber> ready = new ArrayDeque<>();
    private Fiber[] fibers = new Fiber[1024];
    private int[] free = new int[1024];
    private int freeCount;
    private int nextId;

    Ring ring;
    final SendSlots slots = new SendSlots();
    /**
     * Where a write that can go out at once is copied to: bump-allocated, and started again from the beginning
     * whenever no send from it is in flight, which is after nearly every pass, as a send of a few hundred bytes
     * completes in the system call that submits it. So the bytes every connection sends pass through the same few
     * cache lines instead of a buffer per connection. While sends from it are still in flight it only fills up, and
     * once full, writes go to the connection's own buffers until it empties.
     */
    final MemorySegment sendRing = Sys.hugeSlab(SEND_RING);
    final long sendRingAddress = sendRing.address();
    int sendRingHead;
    int ringSends;
    /** IOSQE_FIXED_FILE when connections are direct descriptors in the ring's file table, else 0. */
    int fileFlag;
    BufferRing buffers;
    Fiber current;

    /** A carrier that answers each connection it accepts on [port] by calling [onConnection] with its descriptor. */
    public Carrier(String name, int port, IntConsumer onConnection) {
        super(name);
        this.port = port;
        this.onConnection = onConnection;
    }

    @Override
    public void run() {
        // Made here: a single-issuer ring belongs to the thread that creates it.
        ring = new Ring(4096, 32768);
        buffers = new BufferRing(ring, 8192, 4096);
        if (ring.registerFiles(FILES)) fileFlag = 1;
        int listener = Sys.listen(port);
        spawn(() -> {
            while (true) {
                int fd = Io.accept(listener);
                if (fd < 0) continue;
                spawn(() -> {
                    try {
                        onConnection.accept(fd);
                    } finally {
                        Io.finish(fd);
                    }
                });
            }
        });
        while (true) {
            Fiber f;
            while ((f = ready.poll()) != null) {
                current = f;
                boolean done;
                try {
                    done = f.resume();
                } catch (Throwable t) {
                    done = true;
                }
                current = null;
                if (done) ended(f);
            }
            ring.enter(true);
            ring.reap(this);
        }
    }

    @Override
    public void complete(long userData, int result, int flags) {
        Fiber f = fibers[(int) userData];
        long kind = userData & ~0xffffffffL;
        boolean more = kind == RECV && (flags & CQE_F_MORE) != 0;
        if (!more) f.outstanding--;
        if (kind == RECV) {
            if (!more) f.recvArmed = false;
            int bid = (flags & CQE_F_BUFFER) != 0 ? flags >>> 16 : -1;
            if (f.zombie) {
                if (bid >= 0) buffers.recycle(bid);
            } else {
                // Out of buffers (-ENOBUFS) ends the receive without data: the reader re-arms it when it next reads.
                if (result != -105) f.arrived(bid, result);
                if (f.waitingRecv) {
                    f.waitingRecv = false;
                    ready.add(f);
                }
            }
        } else if (kind == SEND) {
            if (f.zombie) {
                endRingSend(f);
            } else {
                if (result < 0) {
                    endRingSend(f);
                    f.sendError = result;
                    f.sendInFlight = false;
                } else if (result < f.sendLeft) {
                    f.sendAddress += result;
                    f.sendLeft -= result;
                    ring.prep(Ring.OP_SEND, fileFlag, 0, f.fd, f.sendAddress, f.sendLeft, Io.MSG_NOSIGNAL, 0, 0, SEND | f.id);
                    f.outstanding++;
                    return;
                } else if (f.nextLength > 0) {
                    Io.sendNext(this, f);
                    return;
                } else {
                    endRingSend(f);
                    f.sendInFlight = false;
                }
                if (f.waitingSend && !f.sendInFlight) {
                    f.waitingSend = false;
                    ready.add(f);
                }
            }
        } else if (kind == WAKE) {
            f.result = result;
            ready.add(f);
        }
        if (f.zombie && f.outstanding == 0) freeId(f);
    }

    /** The send in flight for [f], if it was from [sendRing], has finished with it. */
    void endRingSend(Fiber f) {
        if (f.ringSend) {
            f.ringSend = false;
            ringSends--;
        }
    }

    void spawn(Runnable body) {
        int id = freeCount > 0 ? free[--freeCount] : nextId++;
        if (id == fibers.length) fibers = Arrays.copyOf(fibers, id * 2);
        Fiber f = new Fiber(id, body);
        fibers[id] = f;
        ready.add(f);
    }

    private void ended(Fiber f) {
        if (f.bufferId >= 0) buffers.recycle(f.bufferId);
        while (f.hasArrival()) {
            int bid = (int) (f.nextArrival() >> 32);
            if (bid >= 0) buffers.recycle(bid);
        }
        if (f.outstanding > 0) f.zombie = true;
        else freeId(f);
    }

    private void freeId(Fiber f) {
        // Nothing is in flight any more, so its send buffers can go to the next connection.
        slots.give(f.out);
        slots.give(f.next);
        f.out = f.next = null;
        fibers[f.id] = null;
        if (freeCount == free.length) free = Arrays.copyOf(free, freeCount * 2);
        free[freeCount++] = f.id;
    }

    static Carrier current() {
        return (Carrier) Thread.currentThread();
    }
}
