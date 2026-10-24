package bvr

/**
 * `Main <command> ...`. Each measuring command runs one contestant in one fresh JVM, so neither JIT profile nor heap
 * carries over from another, and prints `RESULT {json}` lines that the scripts collect.
 *
 * - `io <scenario> <contestant> [warmups] [iterations]`: IoBench.kt
 * - `memory <contestant> <pending|hol> <low> <high> [repeats]`: Memory.kt
 * - `overload <pattern> <contestant>`: Overload.kt
 * - `net-server <port> [selectors]`, `net-server-netty <port> [threads]` and
 *   `net <scenario> <contestant> [warmups] [iterations]`: Net*.kt, NettyServer.kt
 * - `stacktraces`, `threaddump`, `semantics`: the operational experiments (StackTraces.kt, Semantics.kt)
 * - `handoff-threads`: whether HandOffBenchmark's hand-offs cross threads (HandOffCheck.kt)
 * - `cells <suite>`: the cells of a suite, for scripts/run-suite.py
 */
fun main(args: Array<String>) {
    val rest = args.drop(1)
    when (args.firstOrNull()) {
        "io" -> ioMain(rest)
        "memory" -> memoryMain(rest)
        "overload" -> overloadMain(rest)
        "net-server" -> netServerMain(rest)
        "net-server-netty" -> nettyServerMain(rest)
        "net" -> netMain(rest)
        "stacktraces" -> stackTraces()
        "threaddump" -> threadDumps()
        "handoff-threads" -> handOffThreads()
        "semantics" -> semanticsMain()
        // A third field, when present, is JVM options that cell's client runs with.
        "cells" -> allSuites().getValue(rest[0]).forEach { (s, c) -> println("$s $c ${netScenarios[s]?.jvmOpts ?: ""}".trim()) }
        else -> error("unknown command ${args.firstOrNull()}; see Main.kt")
    }
    System.exit(0)
}

fun allSuites(): Map<String, List<Pair<String, String>>> = suites + memorySuites + overloadSuites + netSuites
