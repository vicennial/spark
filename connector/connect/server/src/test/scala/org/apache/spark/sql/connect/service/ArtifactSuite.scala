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

import java.nio.file.Paths

import org.apache.commons.io.FileUtils

import org.apache.spark.SparkEnv
import org.apache.spark.sql.connect.ResourceHelper
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils


class ArtifactSuite extends SharedSparkSession with ResourceHelper {

  private val artifactPath = commonResourcePath.resolve("artifact-tests")

  test("Jar artifacts are added to spark session") {
    val copyDir = Utils.createTempDir().toPath
    FileUtils.copyDirectory(artifactPath.toFile, copyDir.toFile)
    val stagingPath = copyDir.resolve("smallJar.jar")
    val remotePath = Paths.get("jars/smallJar.jar")
    SparkConnectService.Artifacts.addArtifact(spark, remotePath, stagingPath)

    val jarList = spark.sparkContext.listJars()
    assert(jarList.exists(_.contains(remotePath.toString)))
  }

  test("Class artifacts are added to repl directory.") {
    val copyDir = Utils.createTempDir().toPath
    FileUtils.copyDirectory(artifactPath.toFile, copyDir.toFile)
    val stagingPath = copyDir.resolve("smallClassFile.class")
    val remotePath = Paths.get("classes/smallClassFile.class")
    assert(stagingPath.toFile.exists())
    SparkConnectService.Artifacts.addArtifact(spark, remotePath, stagingPath)

    val replClassFileDirectory = Paths.get(SparkEnv.get.conf.get("spark.repl.class.uri"))
    val movedClassFile = replClassFileDirectory.resolve("smallClassFile.class").toFile
    assert(movedClassFile.exists())
  }
}
