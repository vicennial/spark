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

package org.apache.spark.sql.connect.service

import java.net.{URL, URLClassLoader}
import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.CopyOnWriteArrayList

import scala.collection.JavaConverters._

import org.apache.spark.SparkEnv
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.Utils


class SparkConnectArtifactManager private {

  private[connect] val artifactRootPath = Utils.createTempDir("artifacts").toPath
  private[connect] val artifactRootURI = {
    val fileServer = SparkEnv.get.rpcEnv.fileServer
    fileServer.addDirectory("artifacts", artifactRootPath.toFile)
  }

  private[connect] val classArtifactDir = {
    val dir = SparkEnv.get.conf.getOption("spark.repl.class.outputDir").map(p => Paths.get(p))
      .getOrElse(artifactRootPath.resolve("classes"))
    Files.createDirectories(dir)
    dir
  }
  private[connect] val classArtifactUri: String = {
    val conf = SparkEnv.get.conf
    // If set, piggyback on the existing repl class uri functionality that the executor uses
    // to load class files.
    conf.getOption("spark.repl.class.uri").getOrElse {
      val fileServer = SparkEnv.get.rpcEnv.fileServer
      fileServer.addDirectory(artifactRootURI + "/classes", classArtifactDir.toFile)
    }
  }

  private val jarsList = new CopyOnWriteArrayList[Path]

  def getSparkConnectAddedJars: Seq[URL] = jarsList.asScala.map(_.toUri.toURL)

  def addArtifact(
      session: SparkSession,
      remoteRelativePath: Path,
      serverLocalStagingPath: Path): Unit = {
    require(!remoteRelativePath.isAbsolute)
    if (remoteRelativePath.startsWith("classes/")) {
      // Move class files to common location (shared among all users)
      val target = classArtifactDir.resolve(remoteRelativePath.toString.stripPrefix("classes/"))
      Files.createDirectories(target.getParent)
      Files.move(serverLocalStagingPath, target)
    } else {
      val target = artifactRootPath.resolve(remoteRelativePath)
      Files.createDirectories(target.getParent)
      Files.move(serverLocalStagingPath, target)
      if (remoteRelativePath.startsWith("jars")) {
        // Adding Jars to the underlying spark context (visible to all users)
        session.sessionState.resourceLoader.addJar(target.toString)
        jarsList.add(target)
      }
    }
  }
}

object SparkConnectArtifactManager {

  private var _activeArtifactManager: SparkConnectArtifactManager = _
  def getOrCreateArtifactManager: SparkConnectArtifactManager = {
    if (_activeArtifactManager == null) {
      _activeArtifactManager = new SparkConnectArtifactManager
    }
    _activeArtifactManager
  }

  private lazy val artifactManager = getOrCreateArtifactManager

  def classLoaderWithArtifacts: ClassLoader = {
    val urls = artifactManager.getSparkConnectAddedJars :+
      artifactManager.classArtifactDir.toUri.toURL
    new URLClassLoader(urls.toArray, Utils.getContextOrSparkClassLoader)
  }

  def withArtifactClassLoader[T](thunk: => T): T = {
    Utils.withContextClassLoader(classLoaderWithArtifacts) {
      thunk
    }
  }
}
