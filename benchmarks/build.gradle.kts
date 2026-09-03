// JMH benchmark suite (TASK-301, TASK-302, TASK-307, TASK-308). Not yet implemented — M1 only
// scaffolds the module so M3 has a home to land benchmarks in without restructuring the build.

description = "JMH benchmarks for the actor runtime (M3)."

dependencies {
    implementation(project(":framework-core"))
}
