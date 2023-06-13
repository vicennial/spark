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

package org.apache.spark

/**
 * Artifact set for a job.
 * This class is used to store session (i.e [[SparkSession]]) specific resources/artifacts.
 *
 * When Spark Connect is used, this job-set points towards session-specific jars and class files.
 * Note that Spark Connect is not a requirement for using this class.
 */
class JobArtifactSet(
  val uuid: Option[String],
  val replClassDirUri: Option[String],
  val jars: Map[String, Long],
  val files: Map[String, Long],
  val archives: Map[String, Long]) {
  def withActive[T](f: => T): T = JobArtifactSet.withActive(this)(f)
}

object JobArtifactSet {

  private[this] val current = new ThreadLocal[Option[JobArtifactSet]] {
    override def initialValue(): Option[JobArtifactSet] = None
  }

  /**
   * When Spark Connect isn't used, we default back to the shared resources.
   */
  def apply(sc: SparkContext): JobArtifactSet = {
    new JobArtifactSet(
      uuid = None,
      replClassDirUri = sc.conf.getOption("spark.repl.class.uri"),
      jars = sc.addedJars.toMap,
      files = sc.addedFiles.toMap,
      archives = sc.addedArchives.toMap)
  }

  /**
   * Execute a block of code with the currently active [[JobArtifactSet]].
   * @param active
   * @param block
   * @tparam T
   */
  def withActive[T](active: JobArtifactSet)(block: => T): T = {
    val old = current.get()
    current.set(Option(active))
    try block finally {
      current.set(old)
    }
  }

  /**
   * Optionally returns the active [[JobArtifactSet]].
   */
  def active: Option[JobArtifactSet] = current.get()

  /**
   * Return the active [[JobArtifactSet]] or creates the default set using the [[SparkContext]].
   * @param sc
   */
  def getActiveOrDefault(sc: SparkContext): JobArtifactSet = active.getOrElse(JobArtifactSet(sc))
}
