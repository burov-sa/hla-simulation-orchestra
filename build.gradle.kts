import com.github.benmanes.gradle.versions.updates.DependencyUpdatesTask

plugins {
    `java-library`
    `java-test-fixtures`
    `maven-publish`
    alias(libs.plugins.protobuf)
    alias(libs.plugins.lombok)
    alias(libs.plugins.jmh)
    alias(libs.plugins.jcstress)
}

dependencies {
    implementation(libs.gson)
    implementation(libs.kafka.clients)
    implementation(libs.protobuf.java)

    implementation(platform(libs.log4j2.bom))
    implementation(libs.log4j2.api)

    testFixturesImplementation(platform(libs.log4j2.bom))
    testFixturesImplementation(libs.log4j2.api)
}

group = "ru.itmo"
description = "Library for a distributed simulation system based on the HLA architecture"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(25)
    }
}

tasks.withType<JavaCompile>().configureEach {
    options.release = 25
}

protobuf {
    protoc {
        artifact = libs.protoc.get().toString()
    }
}

testing {
    @Suppress("UnstableApiUsage")
    suites.named<JvmTestSuite>("test") {
        useJUnitJupiter(libs.versions.junit)

        dependencies {
            implementation(platform(libs.testcontainers.bom))
            implementation(libs.testcontainers.kafka)
            implementation(libs.testcontainers.junitJupiter)

            runtimeOnly(platform(libs.log4j2.bom))
            runtimeOnly(libs.log4j2.slf4j2Impl)
        }
    }
}

publishing {
    publications.create<MavenPublication>("maven") {
        from(components["java"])
    }
}

tasks.named<DependencyUpdatesTask>("dependencyUpdates") {
    checkConstraints = true
}
