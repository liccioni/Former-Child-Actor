// JMH benchmark suite (TASK-301, TASK-302, TASK-307, TASK-308).
//
// TASK-301 (the first benchmarks to land here) measures the M1 dispatch strategy itself
// (ADR-002: one dedicated virtual thread per actor) — per-message round-trip cost for a single
// actor, and how that cost holds up as the number of live actors grows. This is the real
// measurement data TASK-303 (dispatcher alternatives) and TASK-306 (mailbox bounds) are gated on
// before either is revisited.

plugins {
    id("me.champeau.jmh") version "0.7.3"
}

description = "JMH benchmarks for the actor runtime (M3)."

dependencies {
    implementation(project(":framework-core"))
    jmh(project(":framework-core"))
}

jmh {
    // Iteration/warmup/fork counts are set per-benchmark via @Warmup/@Measurement/@Fork so each
    // benchmark can tune for its own cost (a single round trip vs. spawning up to 10,000 actors).
    jmhVersion.set("1.37")
    resultFormat.set("JSON")
    resultsFile.set(layout.buildDirectory.file("results/jmh/results.json"))
}
