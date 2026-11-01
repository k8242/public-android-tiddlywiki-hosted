plugins {
    java
    application
}

group = "com.example.tiddlywiki"
version = "1.0.0"

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("io.javalin:javalin:6.3.0")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.18.1")
    implementation("org.xerial:sqlite-jdbc:3.47.0.0")
    implementation("org.slf4j:slf4j-simple:2.0.16")
}

application {
    mainClass.set("com.example.tiddlywiki.server.TiddlyServer")
}
