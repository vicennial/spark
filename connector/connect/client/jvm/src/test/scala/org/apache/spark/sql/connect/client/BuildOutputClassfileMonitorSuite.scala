/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.connect.client

import java.io.File
import java.nio.file.{Files, Path, Paths, StandardOpenOption}

import org.apache.spark.sql.connect.client.util.ConnectFunSuite

class BuildOutputClassfileMonitorSuite extends ConnectFunSuite {

  private val classResourcePath = commonResourcePath.resolve("artifact-tests")

  private def createBuildSystemStructure(
      buildSystem: BuildSystem.BuildSystem,
      parentDir: File): Unit = {

    Files.createDirectories(Paths.get(parentDir.toString, "src", "main", "scala"))
    Files.createDirectories(Paths.get(parentDir.toString, "lib"))

    buildSystem match {
      case org.apache.spark.sql.connect.client.BuildSystem.SBT =>
        // Create a dummy default output directory
        val classfileDir = Files.createDirectories(
          Paths.get(parentDir.toString, "target", s"scala-$scalaVersion", "classes"))
        Files.copy(classResourcePath.resolve("Hello.class"), classfileDir.resolve("Hello.class"))
        // Create a dummy build.sbt file
        val buildSbtContent =
          """
            |name := "my-app"
            |
            |version := "1.0"
            |
            |scalaVersion := "2.12.14"
            |
            |libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.10" % Test
            |""".stripMargin

        Files.write(
          Paths.get(parentDir.toString, "build.sbt"),
          buildSbtContent.getBytes,
          StandardOpenOption.CREATE)

      case org.apache.spark.sql.connect.client.BuildSystem.Maven =>
        // Create a dummy default output directory
        val classfileDir =
          Files.createDirectories(Paths.get(parentDir.toString, "target", "classes"))
        Files.copy(classResourcePath.resolve("Hello.class"), classfileDir.resolve("Hello.class"))

        // Create a dummy pom.xml file
        // scalastyle:off line.size.limit
        val pomXmlContent =
          """
            |<project xmlns="http://maven.apache.org/POM/4.0.0"
            |         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
            |         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
            |    <modelVersion>4.0.0</modelVersion>
            |
            |    <groupId>com.example</groupId>
            |    <artifactId>my-app</artifactId>
            |    <version>1.0-SNAPSHOT</version>
            |</project>
            |""".stripMargin
        // scalastyle:on line.size.limit

        Files.write(
          Paths.get(parentDir.toString, "pom.xml"),
          pomXmlContent.getBytes,
          StandardOpenOption.CREATE)

      case org.apache.spark.sql.connect.client.BuildSystem.Bazel =>
        // Create a dummy default output directory
        val classfileDir =
          Files.createDirectories(Paths.get(parentDir.toString, "bazel-bin"))
        Files.copy(classResourcePath.resolve("Hello.class"), classfileDir.resolve("Hello.class"))

        // Create a dummy BUILD file
        val buildFileContent =
          """
            |scala_binary(
            |    name = "app",
            |    srcs = glob(["src/main/scala/**/*.scala"]),
            |    deps = [],
            |)
            |""".stripMargin

        Files.write(
          Paths.get(parentDir.toString, "BUILD"),
          buildFileContent.getBytes,
          StandardOpenOption.CREATE)
    }
  }

  def withBuildSystemStructure(buildSystem: BuildSystem.BuildSystem)(f: File => Unit): Unit = {
    withTempDir { dir =>
      createBuildSystemStructure(buildSystem, dir)
      f(dir)
    }
  }

  private def getExpectedClassfileDir(
      buildSystem: BuildSystem.BuildSystem,
      projectRoot: Path): Path = {
    buildSystem match {
      case org.apache.spark.sql.connect.client.BuildSystem.SBT =>
        projectRoot.resolve(s"target/scala-$scalaVersion/classes")
      case org.apache.spark.sql.connect.client.BuildSystem.Maven =>
        projectRoot.resolve("target/classes")
      case org.apache.spark.sql.connect.client.BuildSystem.Bazel =>
        projectRoot.resolve("bazel-bin")
    }
  }

  class TestBuildOutputClassfileMonitor(currentDir: String) extends BuildOutputClassfileMonitor {

    override protected def defaultClassFileDirectory: Option[String] =
      BuildSystem.getDefaultClassDirectoryTest(currentDir)
  }

  BuildSystem.values.foreach { buildSystem =>
    test(s"Build system:${buildSystem.toString} detection") {
      withBuildSystemStructure(buildSystem) { dir =>
        val foundOpt = BuildSystem.findBuildSystemAndProjectRoot(dir.toString)
        assert(foundOpt.isDefined)
        val foundBuildSystem = foundOpt.get._1
        val foundDir = foundOpt.get._2

        assert(foundBuildSystem == buildSystem)
        assert(foundDir == dir.toString)

        val innerPath = dir.toPath.resolve("src/main/scala").toString
        val expectedClassfileDir = getExpectedClassfileDir(buildSystem, dir.toPath).toString
        assert(BuildSystem.getDefaultClassDirectoryTest(innerPath).contains(expectedClassfileDir))

        assert(Files.isDirectory(dir.toPath))
        val monitor = new TestBuildOutputClassfileMonitor(dir.getAbsolutePath)
        val classes = monitor.findClasses().toSeq
        assert(classes.size == 1)
        assert(classes.head.path.getFileName.toString == "Hello.class")
      }
    }
  }

  test("User specified directory") {
    withTempDir { dir =>
      Files.copy(classResourcePath.resolve("Hello.class"), dir.toPath.resolve("Hello.class"))
      val monitor = new BuildOutputClassfileMonitor(Some(dir.getAbsolutePath))
      assert(monitor.findClasses().toSeq.exists(_.path.getFileName.toString == "Hello.class"))

    }
  }
}
