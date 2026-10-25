plugins {
    kotlin("jvm") version "2.4.10"
    id("me.champeau.jmh") version "0.7.3"
    application
}

// mavenLocal for a Lark snapshot under test: `-PlarkVersion=0.0.0-cand-SNAPSHOT` after `publishToMavenLocal`.
repositories {
    mavenLocal()
    mavenCentral()
}

val larkVersion = providers.gradleProperty("larkVersion").getOrElse("0.10.0")

dependencies {
    // The contestants. Each is the latest stable release on the day the hypotheses were written.
    implementation("io.github.matthewjones372:lark-stream-forks:$larkVersion")
    implementation("io.projectreactor:reactor-core:3.8.8")
    implementation("org.apache.pekko:pekko-stream_2.13:1.2.1")
    // The real-network scenario: Reactor Netty's client, and the Netty it brings serves the delayed answers.
    implementation("io.projectreactor.netty:reactor-netty-http:1.3.7")
    // The server round: a hand-written Jetty handler, the floor under Pelican's Jetty backend.
    implementation("org.eclipse.jetty:jetty-server:12.1.14")
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

// The server round's own runtime (Fiber.java) runs on the JDK's internal continuation.
tasks.withType<JavaCompile>().configureEach {
    options.compilerArgs.addAll(listOf("--add-exports", "java.base/jdk.internal.vm=ALL-UNNAMED"))
}
