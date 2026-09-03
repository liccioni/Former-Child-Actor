// Minimal test support for the actor runtime (TASK-110).
// Grows in M5 without replacing this slice.

description = "Minimal test support for asserting on actor behavior."

dependencies {
    api(project(":framework-core"))
    implementation(platform("org.junit:junit-bom:5.11.4"))
    implementation("org.junit.jupiter:junit-jupiter-api")
}
