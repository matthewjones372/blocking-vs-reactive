plugins {
    kotlin("jvm") version "2.4.10"
    id("me.champeau.jmh") version "0.7.3"
    application
}

repositories { mavenCentral() }

val larkVersion = "0.10.0"

dependencies {
    // The contestants. Each is the latest stable release on the day the hypotheses were written.
    implementation("io.github.matthewjones372:lark-stream-forks:$larkVersion")
    implementation("io.projectreactor:reactor-core:3.8.8")
    implementation("org.apache.pekko:pekko-stream_2.13:1.2.1")
}

kotlin { jvmToolchain(25) }

application { mainClass.set("bvr.MainKt") }

jmh {
    warmupIterations.set(3)
    iterations.set(5)
    fork.set(2)
    timeOnIteration.set("2s")
    warmup.set("2s")
    profilers.add("gc")
    resultFormat.set("JSON")
    jvmArgs.addAll("-Xms2g", "-Xmx2g")
}
