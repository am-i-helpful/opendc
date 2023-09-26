plugins {
    id("java")
    `kotlin-library-conventions`
    application
}

group = "org.opendc"
version = "3.0-SNAPSHOT"

repositories {
    mavenCentral()
}

sourceSets {
    main {
        java.srcDir("src/main/java")
        kotlin.srcDir("src/main/kotlin")
    }
    test {
        java.srcDir("test")
    }
}

application {
    mainClass.set("org.opendc.oda.experimentlistener.ODAExperimentListener")
}

dependencies {
    implementation(libs.kotlin.logging)
    implementation(libs.kafka.client)
    implementation(project(mapOf("path" to ":opendc-oda:opendc-oda-experiments")))
    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    //implementation(kotlin("stdlib-jdk8"))
}

tasks.test {
    useJUnitPlatform()
}
//kotlin {
//    jvmToolchain(17)
//}
