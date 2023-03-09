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
import io.grpc.stub.StreamObserver
import java.nio.file.{Files, Path, Paths}
import java.util.zip.{CheckedOutputStream, CRC32}
import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.{AddArtifactsRequest, AddArtifactsResponse}
import org.apache.spark.connect.proto.AddArtifactsResponse.ArtifactSummary
import org.apache.spark.util.Utils

class SparkConnectAddArtifactsHandler(val responseObserver: StreamObserver[AddArtifactsResponse])
    extends StreamObserver[AddArtifactsRequest] {

  private val stagingDir = Utils.createTempDir().toPath
  private val stagedArtifacts = mutable.Buffer.empty[StagedArtifact]
  private var chunkedArtifact: StagedChunkedArtifact = _
  private var holder: SessionHolder = _
  private lazy val artifactManager = SparkConnectArtifactManager.getOrCreateArtifactManager

  override def onNext(req: AddArtifactsRequest): Unit = {
    if (this.holder == null) {
      val holder = SparkConnectService.getOrCreateIsolatedSession(
        req.getUserContext.getUserId,
        req.getSessionId)
      this.holder = holder
    }

    if (req.hasBeginChunk) {
      require(chunkedArtifact == null)
      chunkedArtifact = writeDepToFile(req.getBeginChunk)
    } else if (req.hasChunk) {
      // We are currently processing a multi-chunk artifact
      require(chunkedArtifact != null && chunkedArtifact.getRemainingChunks > 0)
      chunkedArtifact.write(req.getChunk)

      if (chunkedArtifact.isFinished) {
        chunkedArtifact.close()
        chunkedArtifact = null
      }
    } else if (req.hasBatch) {
      req.getBatch.getArtifactsList.forEach { artifact =>
        val out = writeDepToFile(artifact)
        out.close()
      }
    } else {
      throw new UnsupportedOperationException(
        s"$req could not be processed due to unsupported" +
          s" data transfer mechanism")
    }
  }

  override def onError(throwable: Throwable): Unit = {
    Utils.deleteRecursively(stagingDir.toFile)
    responseObserver.onError(throwable)
  }

  override def onCompleted(): Unit = {
    // Add the artifacts to the session.
    val builder = proto.AddArtifactsResponse.newBuilder()
    stagedArtifacts.foreach { artifact =>
      artifactManager.addArtifact(
        holder.session,
        artifact.path,
        artifact.stagedPath)
      builder.addArtifacts(artifact.summary())
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
  private def writeDepToFile(
      artifact: proto.AddArtifactsRequest.SingleChunkArtifact): StagedArtifact = {
    val stagedDep = new StagedArtifact(artifact.getName)
    stagedArtifacts += stagedDep
    stagedDep.write(artifact.getData)
    stagedDep
  }

  /**
   * Create a (temp) file for the dependency and write the initial chunk. This returns the open
   * [[OutputStream]] so further chunks can be appended to the file.
   */
  private def writeDepToFile(
      artifact: proto.AddArtifactsRequest.BeginChunkedArtifact): StagedChunkedArtifact = {
    val stagedChunkedArtifact =
      new StagedChunkedArtifact(artifact.getName, artifact.getNumChunks, artifact.getTotalBytes)
    stagedArtifacts += stagedChunkedArtifact
    stagedChunkedArtifact.write(artifact.getInitialChunk)
    stagedChunkedArtifact
  }

  class StagedArtifact(val name: String) {
    val path: Path = Paths.get(name)
    val stagedPath: Path = stagingDir.resolve(path)

    Files.createDirectories(stagedPath.getParent)

    private val fileOut = Files.newOutputStream(stagedPath)
    private val countingOut = new CountingOutputStream(fileOut)
    private val checksumOut = new CheckedOutputStream(countingOut, new CRC32)

    private val builder = ArtifactSummary.newBuilder().setName(name)
    private var artifactSummary: ArtifactSummary = _
    protected var isCrcSuccess: Boolean = _

    protected def updateCrc(isSuccess: Boolean): Unit = {
      isCrcSuccess = isSuccess
    }

    def getCrcStatus: Option[Boolean] = Option(isCrcSuccess)

    def write(dataChunk: proto.AddArtifactsRequest.ArtifactChunk): Unit = {
      try dataChunk.getData.writeTo(checksumOut)
      catch {
        case NonFatal(e) =>
          close()
          throw e
      }
      updateCrc(checksumOut.getChecksum.getValue == dataChunk.getCrc)
    }

    def close(): Unit = {
      if (artifactSummary == null) {
        checksumOut.close()
        artifactSummary = builder
          .setName(name)
          .setIsCrcSuccessful(getCrcStatus.getOrElse(false))
          .build()
      }
    }

    def summary(): ArtifactSummary = {
      require(artifactSummary != null)
      artifactSummary
    }
  }

  class StagedChunkedArtifact(name: String, numChunks: Long, totalBytes: Long)
      extends StagedArtifact(name) {

    private var remainingChunks = numChunks
    private var totalBytesProcessed = 0L

    def getRemainingChunks: Long = remainingChunks

    def isFinished: Boolean = remainingChunks == 0

    override protected def updateCrc(isSuccess: Boolean): Unit = {
      isCrcSuccess = isCrcSuccess && isSuccess
    }

    override def write(dataChunk: proto.AddArtifactsRequest.ArtifactChunk): Unit = {
      if (remainingChunks == 0) {
        throw new RuntimeException(
          s"Excessive data chunks for artifact: $name, " +
            s"expected $numChunks chunks in total. Processed $totalBytesProcessed bytes out of" +
            s" $totalBytes bytes.")
      }
      super.write(dataChunk)
      totalBytesProcessed += dataChunk.getData.size()
      remainingChunks -= 1
    }

    override def close(): Unit = {
      if (remainingChunks != 0 || totalBytesProcessed != totalBytes) {
        throw new RuntimeException(
          s"Missing data chunks for artifact: $name. Expected " +
            s"$numChunks chunks and received ${numChunks - remainingChunks} chunks. Processed" +
            s" $totalBytesProcessed bytes out of $totalBytes bytes.")
      }
      super.close()
    }
  }
}
