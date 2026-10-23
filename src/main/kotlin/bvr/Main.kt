package bvr

/**
 * `Main <command> ...`. Each measuring command runs one contestant in one fresh JVM, so neither JIT profile nor heap
 * carries over from another, and prints `RESULT {json}` lines that the scripts collect.
 *
 * - `io <scenario> <contestant> [warmups] [iterations]`: IoBench.kt
 * - `memory <contestant> <pending|hol> <low> <high> [repeats]`: Memory.kt
 * - `overload <pattern> <contestant>`: Overload.kt
 * - `net-server <port> [selectors]` and `net <scenario> <contestant> [warmups] [iterations]`: Net*.kt
 * - `stacktraces`, `threaddump`, `semantics`: the operational experiments (StackTraces.kt, Semantics.kt)
 * - `cells <suite>`: the cells of a suite, for scripts/run-suite.py
 */
fun main(args: Array<String>) {
    val rest = args.drop(1)
    when (args.firstOrNull()) {
        "io" -> ioMain(rest)
        "memory" -> memoryMain(rest)
        "overload" -> overloadMain(rest)
        "net-server" -> netServerMain(rest)
        "net" -> netMain(rest)
        "stacktraces" -> stackTraces()
        "threaddump" -> threadDumps()
        "semantics" -> semanticsMain()
        "cells" -> allSuites().getValue(rest[0]).forEach { (s, c) -> println("$s $c") }
        else -> error("unknown command ${args.firstOrNull()}; see Main.kt")
    }
    System.exit(0)
}

fun allSuites(): Map<String, List<Pair<String, String>>> = suites + memorySuites + overloadSuites + netSuites
