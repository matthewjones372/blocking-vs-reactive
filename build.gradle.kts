plugins {
    kotlin("jvm") version "2.4.10"
    id("me.champeau.jmh") version "0.7.3"
    application
}

// mavenLocal for a Surefold snapshot under test: `-PsurefoldVersion=0.0.0-cand-SNAPSHOT` after `publishToMavenLocal`.
repositories {
    mavenLocal()
    mavenCentral()
}

// Surefold is built from the commit in surefold.commit by scripts/build-surefold.sh: the overload experiment needs `Limit`,
// which no release has yet.
val surefoldVersion = providers.gradleProperty("surefoldVersion").getOrElse("0.0.0-cand-SNAPSHOT")

dependencies {
    // The contestants. Each is the latest stable release on the day the hypotheses were written.
    implementation("io.github.matthewjones372:lark-stream-forks:$surefoldVersion")
    implementation("io.projectreactor:reactor-core:3.8.8")
    implementation("org.apache.pekko:pekko-stream_2.13:1.2.1")
    // The socket suite's Reactor Netty client, and the Netty its second server runs on (ported from main).
    implementation("io.projectreactor.netty:reactor-netty-http:1.3.7")

    testImplementation(kotlin("test"))
    testImplementation("org.junit.jupiter:junit-jupiter-params:5.13.4")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.test {
    useJUnitPlatform()
    jvmArgs("--enable-preview", "-Xmx2g")
    testLogging { events("failed"); exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL }
    // The characterisation tests print what each model did; keep it in the report.
    systemProperty("bvr.semantics.out", layout.buildDirectory.file("semantics.jsonl").get().asFile.path)
}

kotlin { jvmToolchain(25) }

application {
    mainClass.set("bvr.MainKt")
    // StructuredTaskScope is a preview API in JDK 25.
    applicationDefaultJvmArgs = listOf("--enable-preview")
}

// Five forks, so the error the summary shows is spread across JVMs and not only across iterations of one. Warm-up
// and measurement are set on each benchmark class, since settings here would override them.
// `-Pjmh.includes=<regex>` runs a subset.
jmh {
    fork.set(5)
    profilers.add("gc")
    resultFormat.set("JSON")
    jvmArgs.addAll("-Xms2g", "-Xmx2g", "-XX:+UseG1GC", "--enable-preview")
    providers.gradleProperty("jmh.includes").orNull?.let { includes.add(it) }
}

// Pekko's configuration is one reference.conf per jar; a fat jar keeps only one of them and Pekko then fails to
// start. The JMH jar gets every reference.conf on the runtime classpath, concatenated, in place of whichever won.
val mergedReferenceConf by tasks.registering {
    val runtime = configurations.named("jmhRuntimeClasspath")
    val out = layout.buildDirectory.file("merged-conf/reference.conf")
    inputs.files(runtime)
    outputs.file(out)
    doLast {
        val text = runtime.get().filter { it.name.endsWith(".jar") }
            .flatMap { jar -> zipTree(jar).matching { include("reference.conf") }.files.map { it.readText() } }
            .joinToString("\n")
        out.get().asFile.apply { parentFile.mkdirs(); writeText(text) }
    }
}

tasks.named<Jar>("jmhJar") {
    dependsOn(mergedReferenceConf)
    eachFile { if (path == "reference.conf" && "merged-conf" !in file.path) exclude() }
    from(layout.buildDirectory.dir("merged-conf"))
}
