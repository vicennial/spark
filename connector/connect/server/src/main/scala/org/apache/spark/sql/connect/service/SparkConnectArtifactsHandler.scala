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

import com.google.common.io.CountingOutputStream
import com.google.protobuf.ByteString
import io.grpc.stub.StreamObserver
import java.nio.file.{Files, Path, Paths}
import java.util.zip.{CheckedOutputStream, CRC32}
import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.{AddArtifactsRequest, AddArtifactsResponse}
import org.apache.spark.connect.proto.AddArtifactsResponse.ArtifactSummary
import org.apache.spark.util.Utils

class SparkConnectArtifactsHandler(val responseObserver: StreamObserver[AddArtifactsResponse])
  extends StreamObserver[AddArtifactsRequest] {

  private val stagingDir = Utils.createTempDir().toPath
  private val stagedArtifacts = mutable.Buffer.empty[StagedArtifact]
  private var chunkedArtifact: StagedArtifact = _
  private var holder: SessionHolder = _

  override def onNext(v: AddArtifactsRequest): Unit = {
    val holder = SparkConnectService.getOrCreateIsolatedSession(
      v.getUserContext.getUserId,
      v.getClientId)
    if (this.holder == null || this.holder != holder) {
      this.holder = holder
    }

    // Handle in progress chunked upload
    if (chunkedArtifact != null) {
      if (v.hasChunk && (v.getChunk.getName.isEmpty
        || v.getChunk.getName == chunkedArtifact.name)) {
        chunkedArtifact.write(v.getChunk.getData)
        return
      } else {
        chunkedArtifact.close()
        chunkedArtifact = null
      }
    }

    // Handle new chunk
    if (v.hasChunk) {
      val chunk = v.getChunk
      chunkedArtifact = writeDepToFile(chunk)
    } else if (v.hasBatch) {
      v.getBatch.getArtifactsList.forEach { dep =>
        val out = writeDepToFile(dep)
        out.close()
      }
    }
  }

  override def onError(throwable: Throwable): Unit = {
    if (chunkedArtifact != null) {
      chunkedArtifact.close()
    }
    Utils.deleteRecursively(stagingDir.toFile)
    responseObserver.onError(throwable)
  }

  override def onCompleted(): Unit = {
    // Complete an outstanding chunked upload.
    if (chunkedArtifact != null) {
      chunkedArtifact.close()
    }

    // Add the artifacts to the session.
    val builder = proto.AddArtifactsResponse.newBuilder()
    stagedArtifacts.foreach { dep =>
      holder.addArtifact(dep.path, dep.stagedPath)
      builder.addArtifacts(dep.summary())
    }

    // Delete temp dir
    Utils.deleteRecursively(stagingDir.toFile)

    // Send the summaries and close
    responseObserver.onNext(builder.build())
    responseObserver.onCompleted()
  }

  /**
   * Create a (temp) file for the dependency and write the initial chunk. This returns the open
   * [[OutputStream]] so further chunks can be appended to the file.
   */
  private def writeDepToFile(dep: proto.AddArtifactsRequest.Artifact): StagedArtifact = {
    val stagedDep = new StagedArtifact(dep.getName)
    stagedArtifacts += stagedDep
    stagedDep.write(dep.getData)
    stagedDep
  }

  class StagedArtifact(val name: String) {
    val path: Path = Paths.get(name)
    val stagedPath: Path = stagingDir.resolve(path)

    Files.createDirectories(stagedPath.getParent)

    private var fileOut = Files.newOutputStream(stagedPath)
    private var countingOut = new CountingOutputStream(fileOut)
    private var checksumOut = new CheckedOutputStream(countingOut, new CRC32)

    private var builder = ArtifactSummary.newBuilder().setName(name)
    private var artifactSummary: ArtifactSummary = _

    def write(data: ByteString): Unit = {
      try data.writeTo(checksumOut) catch {
        case NonFatal(e) =>
          close()
          throw e
      }
    }

    def close(): Unit = {
      if (artifactSummary == null) {
        checksumOut.close()
        artifactSummary = builder
          .setSize(countingOut.getCount)
          .setCrc(checksumOut.getChecksum.getValue)
          .build()
        builder = null
        fileOut = null
        countingOut = null
        checksumOut = null
      }
    }

    def summary(): ArtifactSummary = {
      require(artifactSummary != null)
      artifactSummary
    }
  }
}
