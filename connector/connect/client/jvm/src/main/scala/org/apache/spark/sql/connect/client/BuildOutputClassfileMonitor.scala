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
import java.nio.file.Paths

import scala.annotation.tailrec

import org.apache.spark.sql.connect.client.BuildOutputClassfileMonitor.ENV_VARIABLE

class BuildOutputClassfileMonitor(
    userSpecifiedDirectory: Option[String] = sys.env.get(ENV_VARIABLE))
    extends ClassFinder {

  protected def defaultClassFileDirectory: Option[String] =
    BuildSystem.getDefaultClassFileDirectory

  protected lazy val targetDir: String = {
    (userSpecifiedDirectory, defaultClassFileDirectory) match {
      case (Some(userDir), _) => userDir
      case (None, Some(defaultDir)) => defaultDir
      case (None, None) =>
        val message =
          s"""
          |Build output directory for classfiles was neither provided nor automatically detected!
          |
          |Please set the environment variable $ENV_VARIABLE to point to the absolute
          |path where classfiles are generated from code compilation!
          |""".stripMargin
        throw new RuntimeException(message)
    }
  }

  private lazy val classFileMonitor = new REPLClassDirMonitor(targetDir)

  override def findClasses(): Iterator[Artifact] = classFileMonitor.findClasses()
}

object BuildOutputClassfileMonitor {
  val ENV_VARIABLE = "CONNECT_SYNC_DIRECTORY"
}

object BuildSystem extends Enumeration {
  type BuildSystem = Value
  val SBT, Maven, Bazel = Value

  val defaultBuildConfigFiles = Map(SBT -> "build.sbt", Maven -> "pom.xml", Bazel -> "BUILD")

  val scalaVersion: String = scala.util.Properties.versionNumberString
    .split("\\.")
    .take(2)
    .mkString(".")

  val classFileDefaultDirectories = Map(
    SBT -> Paths.get(s"target/scala-$scalaVersion/classes"),
    Maven -> Paths.get("target/classes"),
    Bazel -> Paths.get("bazel-bin"))

  @tailrec
  def findBuildSystemAndProjectRoot(dir: String): Option[(BuildSystem, String)] = {
    val dirFile = new java.io.File(dir)
    val foundConfig = defaultBuildConfigFiles.find { case (_, file) =>
      new File(dirFile, file).exists()
    }

    if (foundConfig.isDefined) {
      Some(foundConfig.get._1, dir)
    } else if (dir == "/") {
      None
    } else {
      findBuildSystemAndProjectRoot(Paths.get(dir).getParent.toString)
    }
  }

  private def internalGetDefaultClassFileDirectory(currentDir: String): Option[String] = {
    val foundBuildAndProject = findBuildSystemAndProjectRoot(currentDir)
    foundBuildAndProject.map { case (build, projectRoot) =>
      val defaultClassFileDir = classFileDefaultDirectories(build)
      Paths.get(projectRoot).resolve(defaultClassFileDir).toString
    }
  }

  def getDefaultClassFileDirectory: Option[String] = {
    internalGetDefaultClassFileDirectory(Paths.get(".").toAbsolutePath.normalize.toString)
  }

  private[client] def getDefaultClassDirectoryTest(currentDir: String): Option[String] = {
    internalGetDefaultClassFileDirectory(currentDir)
  }
}
