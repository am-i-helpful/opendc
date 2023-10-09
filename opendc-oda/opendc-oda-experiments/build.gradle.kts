//import org.jetbrains.kotlin.gradle.dsl.kotlinExtension

plugins {
    id("java")
    kotlin("jvm")
//    application
}

//group = "org.opendc"
//version = "3.0-SNAPSHOT"

repositories {
    mavenCentral()
}

sourceSets {
    main {
        java.srcDir("src/main/java")
        kotlin.srcDir("src/main/kotlin")
        resources.srcDirs("src/main/resources")
    }
    test {
        java.srcDir("test")
    }
}

//application {
//    mainClass.set("org.opendc.oda.experimentrunner.SchedulingAlgorithmComparatorExperiment")
//}

dependencies {
    implementation(libs.kotlin.gradle)
    implementation(libs.kotlin.logging)
    implementation(libs.jackson.core)
    implementation(libs.jackson.annotations)
    implementation(libs.jackson.databind)
    implementation(libs.kafka.client)
    implementation(platform(libs.junit.bom))
    implementation(libs.junit.jupiter)
    implementation(libs.junit.jupiter.api)
    implementation(libs.junit.jupiter.engine)
    implementation(libs.jackson.module.kotlin)
    implementation(kotlin("stdlib-jdk8"))

    implementation(project(mapOf("path" to ":opendc-experiments:opendc-experiments-compute")))
    implementation(project(mapOf("path" to ":opendc-workflow:opendc-workflow-service")))
    implementation(project(mapOf("path" to ":opendc-trace:opendc-trace-api")))
    implementation(project(mapOf("path" to ":opendc-experiments:opendc-experiments-workflow")))
    implementation(project(mapOf("path" to ":opendc-simulator:opendc-simulator-core")))

    implementation(project(mapOf("path" to ":opendc-simulator:opendc-simulator-compute")))
//    implementation(project(mapOf("path" to ":opendc-web:opendc-web-proto")))
//    implementation(project(mapOf("path" to ":opendc-experiments:opendc-experiments-compute")))
//    implementation(project(mapOf("path" to ":opendc-simulator:opendc-simulator-core")))
//    implementation(project(mapOf("path" to ":opendc-experiments:opendc-experiments-workflow")))
//    implementation(project(mapOf("path" to ":opendc-workflow:opendc-workflow-service")))
//    implementation(project(mapOf("path" to ":opendc-trace:opendc-trace-api")))
    runtimeOnly(projects.opendcTrace.opendcTraceWtf)
//    testImplementation(projects.opendcTrace.opendcTraceApi)
//    testRuntimeOnly(projects.opendcTrace.opendcTraceWtf)
//    testRuntimeOnly(libs.log4j.core)
//    testRuntimeOnly(libs.log4j.slf4j)
}
//
//tasks.withType<Test> {
//    useJUnitPlatform()
//}
//kotlin {
//    jvmToolchain(17)
//}
