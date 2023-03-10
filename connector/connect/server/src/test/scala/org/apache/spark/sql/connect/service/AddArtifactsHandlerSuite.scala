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

import java.io.InputStream
import java.nio.file.{Files, Path}

import collection.JavaConverters._
import com.google.protobuf.ByteString
import io.grpc.stub.StreamObserver
import scala.collection.mutable
import scala.concurrent.Promise
import scala.concurrent.duration._

import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.{AddArtifactsRequest, AddArtifactsResponse}
import org.apache.spark.sql.connect.ResourceHelper
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.ThreadUtils


class AddArtifactsHandlerSuite extends SharedSparkSession with ResourceHelper {

  private val CHUNK_SIZE: Int = 32 * 1024

  class DummyStreamObserver(p: Promise[AddArtifactsResponse])
      extends StreamObserver[AddArtifactsResponse] {
    override def onNext(v: AddArtifactsResponse): Unit = p.success(v)
    override def onError(throwable: Throwable): Unit = throw throwable
    override def onCompleted(): Unit = {}
  }

  class TestAddArtifactsHandler(responseObserver: StreamObserver[AddArtifactsResponse])
      extends SparkConnectAddArtifactsHandler(responseObserver) {

    override protected def cleanUpStagedArtifacts(): Unit = {}

    override protected def flushStagedArtifacts(): Seq[AddArtifactsResponse.ArtifactSummary] = {
      stagedArtifacts.map(_.summary())
    }

    def stagingDirectory: Path = this.stagingDir
    def forceCleanUp(): Unit = super.cleanUpStagedArtifacts()
  }

  protected val inputFilePath: Path = commonResourcePath.resolve("artifact-tests")
  protected val crcPath: Path = inputFilePath.resolve("crc")

  private def readNextChunk(in: InputStream): ByteString = {
    val buf = new Array[Byte](CHUNK_SIZE)
    var bytesRead = 0
    var count = 0
    while (count != -1 && bytesRead < CHUNK_SIZE) {
      count = in.read(buf, bytesRead, CHUNK_SIZE - bytesRead)
      if (count != -1) {
        bytesRead += count
      }
    }
    if (bytesRead == 0) ByteString.empty()
    else ByteString.copyFrom(buf, 0, bytesRead)
  }

  private def getDataChunks(filePath: Path): Seq[ByteString] = {
    val in = Files.newInputStream(filePath)
    var chunkData: ByteString = readNextChunk(in)
    val dataChunks = mutable.ListBuffer.empty[ByteString]
    while(chunkData != ByteString.empty()) {
      dataChunks.append(chunkData)
      chunkData = readNextChunk(in)
    }
    dataChunks.toSeq
  }

  private def getCrcValues(filePath: Path): Seq[Long] = {
    val fileName = filePath.getFileName.toString
    val crcFileName = fileName.split('.').head + ".txt"
    Files
      .readAllLines(crcPath.resolve(crcFileName))
      .asScala
      .map(_.toLong)
      .toSeq
  }


  test("single chunk artifact") {
    val promise = Promise[AddArtifactsResponse]
    val handler = new TestAddArtifactsHandler(new DummyStreamObserver(promise))
    try {
      val dataChunks = getDataChunks(inputFilePath.resolve("smallClassFile.class"))
      assert(dataChunks.size == 1)
      val bytes = dataChunks.head
      val context = proto.UserContext
        .newBuilder()
        .setUserId("c1")
        .build()
      val singleChunkArtifact = proto.AddArtifactsRequest.SingleChunkArtifact.newBuilder()
        .setName("classes/smallClassFile.class")
        .setData(proto.AddArtifactsRequest.ArtifactChunk.newBuilder()
          .setData(bytes)
          .setCrc(getCrcValues(crcPath.resolve("smallClassFile.txt")).head)
          .build()
        )
        .build()

      val singleChunkArtifactRequest = AddArtifactsRequest.newBuilder()
        .setSessionId("abc")
        .setUserContext(context)
        .setBatch(
          proto.AddArtifactsRequest.Batch.newBuilder().addArtifacts(singleChunkArtifact).build()
        )
        .build()

      handler.onNext(singleChunkArtifactRequest)
      handler.onCompleted()
      val response = ThreadUtils.awaitResult(promise.future, 5.seconds)
      val summaries = response.getArtifactsList.asScala.toSeq
      assert(summaries.size == 1)
      assert(summaries.head.getName == "classes/smallClassFile.class")
      assert(summaries.head.getIsCrcSuccessful)

      val writtenFile = handler.stagingDirectory.resolve("classes/smallClassFile.class")
      assert(writtenFile.toFile.exists())
      val writtenBytes = ByteString.readFrom(Files.newInputStream(writtenFile))
      assert(writtenBytes == bytes)
    } finally {
      handler.forceCleanUp()
    }
  }

  test("Multi chunk artifact") {
    val promise = Promise[AddArtifactsResponse]
    val handler = new TestAddArtifactsHandler(new DummyStreamObserver(promise))
    try {
      val artifactPath = inputFilePath.resolve("junitLargeJar.jar")
      val dataChunks = getDataChunks(artifactPath)
      val crcs = getCrcValues(artifactPath)
      assert(dataChunks.size == crcs.size)
      val artifactChunks = dataChunks.zip(crcs).map { case (chunk, crc) =>
        proto.AddArtifactsRequest.ArtifactChunk.newBuilder().setData(chunk).setCrc(crc).build()
      }

      val context = proto.UserContext
        .newBuilder()
        .setUserId("c1")
        .build()
      val beginChunkedArtifact = proto.AddArtifactsRequest.BeginChunkedArtifact.newBuilder()
        .setName("jars/junitLargeJar.jar")
        .setNumChunks(artifactChunks.size)
        .setTotalBytes(Files.size(artifactPath))
        .setInitialChunk(artifactChunks.head)
        .build()

      val requestBuilder = AddArtifactsRequest.newBuilder()
        .setSessionId("abc")
        .setUserContext(context)
        .setBeginChunk(beginChunkedArtifact)

      handler.onNext(requestBuilder.build())
      requestBuilder.clearBeginChunk()
      artifactChunks.drop(1).foreach { dataChunk =>
        requestBuilder.setChunk(dataChunk)
        handler.onNext(requestBuilder.build())
      }
      handler.onCompleted()
      val response = ThreadUtils.awaitResult(promise.future, 5.seconds)
      val summaries = response.getArtifactsList.asScala.toSeq
      assert(summaries.size == 1)
      assert(summaries.head.getName == "jars/junitLargeJar.jar")
      assert(summaries.head.getIsCrcSuccessful)

      val writtenFile = handler.stagingDirectory.resolve("jars/junitLargeJar.jar")
      assert(writtenFile.toFile.exists())
      val writtenBytes = ByteString.readFrom(Files.newInputStream(writtenFile))
      val expectedByes = ByteString.readFrom(Files.newInputStream(artifactPath))
      assert(writtenBytes == expectedByes)
    } finally {
      handler.forceCleanUp()
    }
  }
}
