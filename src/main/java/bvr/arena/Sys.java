package bvr.arena;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;

import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;
import static java.lang.foreign.ValueLayout.JAVA_SHORT;

/** The libc calls and io_uring system calls the per-core runtime needs, over FFM. Linux x86-64 only. */
final class Sys {
    private Sys() {}

    private static final Linker LINKER = Linker.nativeLinker();

    private static MethodHandle libc(String name, FunctionDescriptor descriptor, Linker.Option... options) {
        return LINKER.downcallHandle(LINKER.defaultLookup().find(name).orElseThrow(), descriptor, options);
    }

    // syscall(long number, ...): every variadic argument goes as a long, which is what glibc reads them as.
    static final MethodHandle SYSCALL3 = libc("syscall",
        FunctionDescriptor.of(JAVA_LONG, JAVA_LONG, JAVA_LONG, JAVA_LONG), Linker.Option.firstVariadicArg(1));
    private static final FunctionDescriptor SYSCALL7_TYPE = FunctionDescriptor.of(
        JAVA_LONG, JAVA_LONG, JAVA_LONG, JAVA_LONG, JAVA_LONG, JAVA_LONG, JAVA_LONG, JAVA_LONG);
    static final MethodHandle SYSCALL7 = libc("syscall", SYSCALL7_TYPE, Linker.Option.firstVariadicArg(1));
    // For an io_uring_enter that never waits: no thread-state transition, which a call that may block must have.
    static final MethodHandle SYSCALL7_CRITICAL = libc("syscall", SYSCALL7_TYPE,
        Linker.Option.firstVariadicArg(1), Linker.Option.critical(false));

    private static final MethodHandle SOCKET = libc("socket", FunctionDescriptor.of(JAVA_INT, JAVA_INT, JAVA_INT, JAVA_INT));
    private static final MethodHandle SETSOCKOPT = libc("setsockopt",
        FunctionDescriptor.of(JAVA_INT, JAVA_INT, JAVA_INT, JAVA_INT, ADDRESS, JAVA_INT));
    private static final MethodHandle BIND = libc("bind", FunctionDescriptor.of(JAVA_INT, JAVA_INT, ADDRESS, JAVA_INT));
    private static final MethodHandle LISTEN = libc("listen", FunctionDescriptor.of(JAVA_INT, JAVA_INT, JAVA_INT));
    private static final MethodHandle MMAP = libc("mmap",
        FunctionDescriptor.of(ADDRESS, ADDRESS, JAVA_LONG, JAVA_INT, JAVA_INT, JAVA_INT, JAVA_LONG));

    private static final MethodHandle MADVISE = libc("madvise", FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_LONG, JAVA_INT));

    /**
     * Native memory of [size] bytes that never moves or ends, on transparent huge pages where the kernel allows it:
     * memory the kernel copies into and out of for every request, so one TLB entry covering 2 MiB of it instead of
     * 4 KiB matters.
     */
    static MemorySegment hugeSlab(long size) {
        long huge = 2L << 20;
        long rounded = (size + huge - 1) & -huge;
        try {
            // Anonymous and untouched, so the advice comes before the first fault; over-sized to align it.
            MemorySegment m = (MemorySegment) MMAP.invokeExact(MemorySegment.NULL, rounded + huge,
                3 /* PROT_READ | PROT_WRITE */, 0x22 /* MAP_PRIVATE | MAP_ANONYMOUS */, -1, 0L);
            if (m.address() == -1L) throw new IllegalStateException("mmap failed");
            MemorySegment aligned = MemorySegment.ofAddress((m.address() + huge - 1) & -huge).reinterpret(rounded);
            int ignored = (int) MADVISE.invokeExact(aligned, rounded, 14 /* MADV_HUGEPAGE */);
            return aligned.asSlice(0, size);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    static final long IO_URING_SETUP = 425;
    static final long IO_URING_ENTER = 426;

    private static final MemorySegment ONE = Arena.global().allocate(JAVA_INT);

    static {
        ONE.set(JAVA_INT, 0, 1);
    }

    /** A TCP socket listening on every IPv4 address at [port], sharing the port with others through SO_REUSEPORT. */
    static int listen(int port) {
        try (Arena arena = Arena.ofConfined()) {
            int fd = (int) SOCKET.invokeExact(2 /* AF_INET */, 1 /* SOCK_STREAM */, 0);
            check(fd, "socket");
            check((int) SETSOCKOPT.invokeExact(fd, 1 /* SOL_SOCKET */, 2 /* SO_REUSEADDR */, ONE, 4), "SO_REUSEADDR");
            check((int) SETSOCKOPT.invokeExact(fd, 1, 15 /* SO_REUSEPORT */, ONE, 4), "SO_REUSEPORT");
            // Accepted sockets inherit it, which spares a system call per connection.
            check((int) SETSOCKOPT.invokeExact(fd, 6 /* IPPROTO_TCP */, 1 /* TCP_NODELAY */, ONE, 4), "TCP_NODELAY");
            MemorySegment address = arena.allocate(16); // sockaddr_in, zeroed: INADDR_ANY
            address.set(JAVA_SHORT, 0, (short) 2);
            address.set(JAVA_BYTE, 2, (byte) (port >> 8));
            address.set(JAVA_BYTE, 3, (byte) port);
            check((int) BIND.invokeExact(fd, address, 16), "bind");
            check((int) LISTEN.invokeExact(fd, 65_535), "listen");
            return fd;
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    static MemorySegment mmap(long size, int fd, long offset) {
        try {
            MemorySegment m = (MemorySegment) MMAP.invokeExact(MemorySegment.NULL, size,
                3 /* PROT_READ | PROT_WRITE */, 0x8001 /* MAP_SHARED | MAP_POPULATE */, fd, offset);
            if (m.address() == -1L) throw new IllegalStateException("mmap failed at offset " + offset);
            return m.reinterpret(size);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    private static void check(int result, String what) {
        if (result < 0) throw new IllegalStateException(what + " failed");
    }

    static RuntimeException rethrow(Throwable t) {
        if (t instanceof RuntimeException r) return r;
        if (t instanceof Error e) throw e;
        return new RuntimeException(t);
    }
}
