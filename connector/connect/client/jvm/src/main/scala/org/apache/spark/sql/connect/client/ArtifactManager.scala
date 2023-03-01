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

import java.io.{ByteArrayInputStream, InputStream}
import java.net.URI
import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.CopyOnWriteArrayList
import java.util.zip.{CheckedInputStream, CRC32}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.Promise
import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

import Artifact._
import com.google.protobuf.ByteString
import io.grpc.ManagedChannel
import io.grpc.stub.StreamObserver

import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.AddArtifactsResponse
import org.apache.spark.connect.proto.AddArtifactsResponse.ArtifactSummary
import org.apache.spark.util.{DependencyUtils, ThreadUtils, Utils}

class ArtifactManager(userContext: proto.UserContext, channel: ManagedChannel) {
  // Using the midpoint recommendation of 32KiB for chunk size as specified in
  // https://github.com/grpc/grpc.github.io/issues/371.
  private val chunkSize: Int = 32 * 1024

  private[this] val stub = proto.SparkConnectServiceGrpc.newStub(channel)
  private[this] val classFinders = new CopyOnWriteArrayList[ClassFinder]

  /**
   * Register a [[ClassFinder]] for dynamically generated classes.
   */
  def register(finder: ClassFinder): Unit = classFinders.add(finder)

  /**
   * Add a single artifact to the session.
   *
   * Currently this supports local files and ivy coordinates.
   */
  def addArtifact(path: String): Unit = {
    addArtifact(Utils.resolveURI(path))
  }

  /**
   * Add a single artifact to the session.
   *
   * Currently this supports local files and ivy coordinates.
   */
  def addArtifact(uri: URI): Unit = {
    uri.getScheme match {
      case "file" =>
        val path = Paths.get(uri)
        val artifact = path.getFileName.toString match {
          case jar if jar.endsWith(".jar") => newJarArtifact(path.getFileName, new LocalFile(path))
          case cf if cf.endsWith(".class") =>
            newClassArtifact(path.getFileName, new LocalFile(path))
          case other =>
            throw new UnsupportedOperationException(s"Unsuppoted file format: $other")
        }
        addArtifacts(artifact :: Nil)

      case "ivy" =>
        val artifacts = DependencyUtils.resolveMavenDependencies(uri).map { pathString =>
          val path = Paths.get(pathString)
          newJarArtifact(path.getFileName, new LocalFile(path))
        }
        addArtifacts(artifacts)

      case other =>
        throw new UnsupportedOperationException(s"Unsupported scheme: $other")
    }
  }

  /**
   * Ensure that all artifacts added to this point have been uploaded to the server, and are ready
   * for use. This method will fail if it finds any problem with the outstanding requests.
   */
  private[client] def ensureAllArtifactsUploaded(): Unit = {
    addArtifacts(classFinders.asScala.flatMap(_.findClasses()))
  }

  /**
   * Add a number of artifacts to the session.
   */
  private[client] def addArtifacts(artifacts: Iterable[Artifact]): Unit = {
    val promise = Promise[Seq[ArtifactSummary]]
    val responseHandler = new StreamObserver[proto.AddArtifactsResponse] {
      private val summaries = mutable.Buffer.empty[ArtifactSummary]
      override def onNext(v: AddArtifactsResponse): Unit = {
        v.getArtifactsList.forEach { summary =>
          summaries += summary
        }
      }
      override def onError(throwable: Throwable): Unit = {
        promise.failure(throwable)
      }
      override def onCompleted(): Unit = {
        promise.success(summaries)
      }
    }
    val stream = stub.addArtifacts(responseHandler)
    val currentBatch = mutable.Buffer.empty[Artifact]
    var currentBatchSize = 0L

    def addToBatch(dep: Artifact, size: Long): Unit = {
      currentBatch += dep
      currentBatchSize += size
    }

    def writeBatch(): Unit = {
      addBatchedArtifacts(currentBatch, stream)
      currentBatch.clear()
      currentBatchSize = 0
    }

    artifacts.iterator.foreach { artifact =>
      val data = artifact.storage.asInstanceOf[LocalData]
      val size = data.size
      if (size > chunkSize) {
        // Payload can either be a batch OR a single chunked artifact. Write batch if non-empty
        // before chunking current artifact.
        if (currentBatch.nonEmpty) {
          writeBatch()
        }
        addChunkedArtifact(artifact, stream)
      } else {
        if (currentBatchSize + size > chunkSize) {
          writeBatch()
        }
        addToBatch(artifact, size)
      }
    }
    if (currentBatch.nonEmpty) {
      writeBatch()
    }
    stream.onCompleted()
    ThreadUtils.awaitResult(promise.future, Duration.Inf)
  }

  /**
   * Add a batch of artifacts to the stream. All the artifacts in this call are packaged into a
   * single [[proto.AddArtifactsRequest]].
   */
  private def addBatchedArtifacts(
      artifacts: Seq[Artifact],
      stream: StreamObserver[proto.AddArtifactsRequest]): Unit = {
    val builder = proto.AddArtifactsRequest
      .newBuilder()
      .setUserContext(userContext)
    artifacts.foreach { artifact =>
      val in = new CheckedInputStream(artifact.storage.asInstanceOf[LocalData].stream, new CRC32)
      try {
        val data = proto.AddArtifactsRequest.ArtifactChunk
          .newBuilder()
          .setData(ByteString.readFrom(in))
          .setCrc(in.getChecksum.getValue)

        builder.getBatchBuilder
          .addArtifactsBuilder()
          .setName(artifact.path.toString)
          .setData(data)
          .build()
      } catch {
        case NonFatal(e) =>
          stream.onError(e)
          throw e
      } finally {
        in.close()
      }
    }
    stream.onNext(builder.build())
  }

  /**
   * Add a artifact in chunks to the stream. The artifact's data is spread out over multiple
   * [[proto.AddArtifactsRequest requests]].
   */
  private def addChunkedArtifact(
      artifact: Artifact,
      stream: StreamObserver[proto.AddArtifactsRequest]): Unit = {
    val builder = proto.AddArtifactsRequest
      .newBuilder()
      .setUserContext(userContext)

    val in = new CheckedInputStream(artifact.storage.asInstanceOf[LocalData].stream, new CRC32)
    try {
      // First RPC contains the `BeginChunkedArtifact` payload (`begin_chunk`).
      // Subsequent RPCs contains the `ArtifactChunk` payload (`chunk`).
      val artifactChunkBuilder = proto.AddArtifactsRequest.ArtifactChunk.newBuilder()
      var dataChunk = ByteString.readFrom(in, chunkSize)
      def getNumChunks(size: Long): Long = (size + (chunkSize - 1)) / chunkSize
      builder.getBeginChunkBuilder
        .setName(artifact.path.toString)
        .setTotalBytes(artifact.size)
        .setNumChunks(getNumChunks(artifact.size))
        .setInitialChunk(
          artifactChunkBuilder
            .setData(dataChunk)
            .setCrc(in.getChecksum.getValue))
      stream.onNext(builder.build())

      while ({ dataChunk = ByteString.readFrom(in, chunkSize); !dataChunk.isEmpty }) {
        artifactChunkBuilder.setData(dataChunk).setCrc(in.getChecksum.getValue)
        stream.onNext(builder.build())
      }
    } catch {
      case NonFatal(e) =>
        stream.onError(e)
        throw e
    } finally {
      in.close()
    }
  }
}

trait ClassFinder {
  def findClasses(): Iterator[Artifact]
}

class Artifact private (val path: Path, val storage: Storage) {
  require(!path.isAbsolute, s"Bad path: $path")

  lazy val size: Long = storage match {
    case localData: LocalData => localData.size
  }
}

object Artifact {
  val CLASS_PREFIX: Path = Paths.get("classes")
  val JAR_PREFIX: Path = Paths.get("jars")

  def newJarArtifact(fileName: Path, storage: Storage): Artifact = {
    newArtifact(JAR_PREFIX, ".jar", fileName, storage)
  }

  def newClassArtifact(fileName: Path, storage: Storage): Artifact = {
    newArtifact(CLASS_PREFIX, ".class", fileName, storage)
  }

  private def newArtifact(
      prefix: Path,
      requiredSuffix: String,
      fileName: Path,
      storage: Storage): Artifact = {
    require(!fileName.isAbsolute)
    require(fileName.toString.endsWith(requiredSuffix))
    new Artifact(prefix.resolve(fileName), storage)
  }

  /**
   * A pointer to the stored bytes of an artifact.
   */
  sealed trait Storage

  /**
   * Payload stored on this machine.
   */
  sealed trait LocalData extends Storage {
    def stream: InputStream
    def size: Long
  }

  /**
   * Payload stored in memory.
   */
  class InMemory(bytes: Array[Byte]) extends LocalData {
    override def size: Long = bytes.length
    override def stream: InputStream = new ByteArrayInputStream(bytes)
  }

  /**
   * Payload stored in a local file.
   */
  class LocalFile(val path: Path) extends LocalData {
    override def size: Long = Files.size(path)
    override def stream: InputStream = Files.newInputStream(path)
  }
}
