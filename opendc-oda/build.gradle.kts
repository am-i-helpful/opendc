plugins {
    id("java")
}

group = "org.opendc"
version = "3.0-SNAPSHOT"

repositories {
    mavenCentral()
}

description = "ODA framework orchestration for OpenDC"

subprojects {
    group = "org.opendc.oda"
}
