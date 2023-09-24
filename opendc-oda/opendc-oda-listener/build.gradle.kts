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

application {
    mainClass.set("org.opendc.oda.experimentlistener.ODAExperimentListener")
}

dependencies {
    implementation(libs.kotlin.logging)
    implementation(libs.kafka.client)
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
