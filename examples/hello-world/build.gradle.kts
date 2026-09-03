plugins {
    application
}

description = "The smallest complete application: spawn one actor, send it one message (TASK-108)."

dependencies {
    implementation(project(":framework-core"))
}

application {
    mainClass.set("dev.actorframework.examples.helloworld.HelloWorldApp")
}
