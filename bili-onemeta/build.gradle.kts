/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
plugins {
  `maven-publish`
  id("java")
  id("idea")
}

dependencies {
  implementation(project(":api"))
  implementation(project(":common"))
  implementation(project(":server-common"))
  implementation(project(":core"))
  implementation(project(":server"))
  implementation(libs.jackson.databind)
  implementation(libs.jackson.annotations)
  implementation(libs.jackson.datatype.jdk8)
  implementation(libs.jackson.datatype.jsr310)
  implementation(libs.guava)
  implementation(libs.bundles.log4j)
  implementation(libs.bundles.jetty)
  implementation(libs.bundles.jersey)
  implementation(libs.metrics.jersey2)

  compileOnly(libs.lombok)
  annotationProcessor(libs.lombok)
  testCompileOnly(libs.lombok)
  testAnnotationProcessor(libs.lombok)

  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.params)
  testRuntimeOnly(libs.junit.jupiter.engine)
  testImplementation(libs.jersey.test.framework.core) {
    exclude(group = "org.junit.jupiter")
  }
  testImplementation(libs.jersey.test.framework.provider.jetty) {
    exclude(group = "org.junit.jupiter")
  }
  testImplementation(libs.mockito.core)
  testImplementation(libs.commons.io)
}

tasks.build {
  dependsOn("javadoc")
}

tasks.javadoc {
  dependsOn(":api:javadoc", ":common:javadoc")
  source =
    sourceSets["main"].allJava +
    project(":api").sourceSets["main"].allJava +
    project(":common").sourceSets["main"].allJava

  classpath = configurations["compileClasspath"] +
    project(":api").configurations["runtimeClasspath"] +
    project(":common").configurations["runtimeClasspath"]
}
