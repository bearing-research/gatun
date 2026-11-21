plugins {
    `java-library`
    `application` // Adds the 'run' task
    id("com.gradleup.shadow") version "9.6.1"
}

repositories {
    mavenCentral()
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(22))
    }
}

dependencies {
    implementation("com.google.flatbuffers:flatbuffers-java:25.2.10")
    // The oldest Arrow the server runs on. On a classpath with an Arrow of its own the
    // server is the thin JAR (below) and uses that Arrow, so what is compiled here must be
    // API that every supported one has: Spark 4.1 ships 18.3.0 and Spark 4.3 ships 19.0.0.
    // Raising this drops Spark 4.1.
    implementation("org.apache.arrow:arrow-vector:18.3.0")
    implementation("org.apache.arrow:arrow-memory-netty:18.3.0")

    // Arrow's Jackson, in the fat JAR only and relocated there
    implementation(platform("com.fasterxml.jackson:jackson-bom:2.20.1"))
}

val runtimeJvmArgs = listOf(
    "--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED"
)

application {
    mainClass.set("org.gatun.server.GatunServer")
    applicationDefaultJvmArgs = runtimeJvmArgs
}

tasks.withType<JavaCompile> {
    options.release.set(22)
}

// Apply to ALL executions (run, custom JavaExec tasks, etc.)
tasks.withType<JavaExec>().configureEach {
    jvmArgs(runtimeJvmArgs)
}

// Apply to tests too (common source of “it still fails”)
tasks.withType<Test>().configureEach {
    jvmArgs(runtimeJvmArgs)
}

tasks {
    shadowJar {
        archiveBaseName.set("gatun-server")
        archiveClassifier.set("all")
        archiveVersion.set("")
        mergeServiceFiles()

        // Relocate Netty to avoid version conflicts when Spark is on classpath.
        // Arrow uses Netty for memory allocation, Spark uses a different Netty version.
        // By shading Gatun's Netty, both can coexist without conflicts.
        relocate("io.netty", "gatun.shaded.io.netty")

        // Jackson is Arrow's, for schemas as JSON, and nothing of it shows in what the server
        // or Arrow hands to other code. Relocated, it is never in front of the Jackson of a
        // classpath that has one and no Arrow. (With Arrow there, the thin JAR runs.)
        relocate("com.fasterxml.jackson", "gatun.shaded.com.fasterxml.jackson")

    }
}

// The server's own classes and nothing else, for a classpath that brings its own Arrow
// (Spark's). The launcher starts this one in place of the fat JAR when it finds Arrow there.
//
// Arrow cannot be relocated like Netty and Jackson are: Java code on the classpath has to
// receive the VectorSchemaRoot that arrow_to_java() makes, so the server and that code must
// use one org.apache.arrow. For both to hold the server uses the classpath's Arrow, and with
// it the Netty, Jackson and FlatBuffers that Arrow was built for. FlatBuffers cannot be a
// relocated copy either: the server reads Arrow's own messages (org.apache.arrow.flatbuf),
// which are that library's tables.
val thinJar by tasks.registering(Jar::class) {
    archiveBaseName.set("gatun-server")
    archiveClassifier.set("thin")
    archiveVersion.set("")
    from(sourceSets.main.get().output)
    manifest {
        attributes["Main-Class"] = "org.gatun.server.GatunServer"
    }
}
