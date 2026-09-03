plugins {
    application
}

description = "Two actors exchanging messages, each holding its own state (TASK-109)."

dependencies {
    implementation(project(":framework-core"))
}

application {
    mainClass.set("dev.actorframework.examples.pingpong.PingPongApp")
}
