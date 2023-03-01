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

import java.io.InputStream
import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.TimeUnit
import java.util.zip.{CheckedInputStream, CRC32}

import com.google.protobuf.ByteString
import io.grpc.{ManagedChannel, Server}
import io.grpc.inprocess.{InProcessChannelBuilder, InProcessServerBuilder}
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.AddArtifactsRequest
import org.apache.spark.sql.connect.client.util.ConnectFunSuite

class ArtifactSuite extends ConnectFunSuite with BeforeAndAfterEach {

  private var client: SparkConnectClient = _
  private var service: DummySparkConnectService = _
  private var server: Server = _
  private var artifactManager: ArtifactManager = _
  private var channel: ManagedChannel = _

  private def startDummyServer(): Unit = {
    service = new DummySparkConnectService()
    server = InProcessServerBuilder
      .forName(getClass.getName)
      .addService(service)
      .build()
    server.start()
  }

  private def createArtifactManager(): Unit = {
    channel = InProcessChannelBuilder.forName(getClass.getName).directExecutor().build()
    artifactManager = new ArtifactManager(proto.UserContext.newBuilder().build(), channel)
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    startDummyServer()
    createArtifactManager()
    client = null
  }

  override def afterEach(): Unit = {
    if (server != null) {
      server.shutdownNow()
      assert(server.awaitTermination(5, TimeUnit.SECONDS), "server failed to shutdown")
    }

    if (channel != null) {
      channel.shutdownNow()
    }

    if (client != null) {
      client.shutdown()
    }
  }

  private val chunkSize: Int = 32 * 1024
  protected def artifactFilePath: Path = baseResourcePath.resolve("artifact-tests")

  private def assertFileDataEquality(
      artifactChunk: AddArtifactsRequest.ArtifactChunk,
      localPath: Path): Unit = {
    val in = new CheckedInputStream(Files.newInputStream(localPath), new CRC32)
    val localData = ByteString.readFrom(in)
    assert(artifactChunk.getData == localData)
    assert(artifactChunk.getCrc == in.getChecksum.getValue)
  }

  private def singleChunkArtifactTest(path: String): Unit = {
    test(s"Single Chunk Artifact - $path") {
      val artifactPath = artifactFilePath.resolve(path)
      artifactManager.addArtifact(artifactPath.toString)

      val receivedRequests = service.getAndClearLatestAddArtifactRequests()
      assert(receivedRequests.size == 1)

      val request = receivedRequests.head
      assert(request.hasBatch)

      val batch = request.getBatch
      assert(batch.getArtifactsList.size() == 1)

      val singleChunkArtifact = batch.getArtifacts(0)
      val namePrefix = artifactPath.getFileName.toString match {
        case jar if jar.endsWith(".jar") => "jars"
        case cf if cf.endsWith(".class") => "classes"
      }
      assert(singleChunkArtifact.getName.equals(namePrefix + "/" + path))
      assertFileDataEquality(singleChunkArtifact.getData, artifactPath)
    }
  }

  singleChunkArtifactTest("smallClassFile.class")

  singleChunkArtifactTest("smallJar.jar")

  private def readNextChunk(in: InputStream): ByteString = {
    val buf = new Array[Byte](chunkSize)
    var bytesRead = 0
    var count = 0
    while (count != -1 && bytesRead < chunkSize) {
      count = in.read(buf, bytesRead, chunkSize - bytesRead)
      if (count != -1) {
        bytesRead += count
      }
    }
    if (bytesRead == 0) ByteString.empty()
    else ByteString.copyFrom(buf, 0, bytesRead)
  }

  private def checkChunkDataAndCrc(
      in: CheckedInputStream,
      chunk: AddArtifactsRequest.ArtifactChunk): Boolean = {
    val expectedData = readNextChunk(in)
    val expectedCRC = in.getChecksum.getValue
    chunk.getData == expectedData && chunk.getCrc == expectedCRC
  }

  test("Chunked Artifact - junitLargeJar.jar") {
    val artifactPath = artifactFilePath.resolve("junitLargeJar.jar")
    artifactManager.addArtifact(artifactPath.toString)
    val in = new CheckedInputStream(Files.newInputStream(artifactPath), new CRC32)

    val expectedChunks = (384581 + (chunkSize - 1)) / chunkSize
    val receivedRequests = service.getAndClearLatestAddArtifactRequests()
    assert(384581 == Files.size(artifactPath))
    assert(receivedRequests.size == expectedChunks)
    assert(receivedRequests.head.hasBeginChunk)
    val beginChunkRequest = receivedRequests.head.getBeginChunk
    assert(beginChunkRequest.getName == "jars/junitLargeJar.jar")
    assert(beginChunkRequest.getTotalBytes == 384581)
    assert(beginChunkRequest.getNumChunks == expectedChunks)
    checkChunkDataAndCrc(in, beginChunkRequest.getInitialChunk)

    receivedRequests.drop(1).forall(r => r.hasChunk && checkChunkDataAndCrc(in, r.getChunk))
  }

  test("Batched SingleChunkArtifacts") {
    val file1 = artifactFilePath.resolve("smallClassFile.class").toUri
    val file2 = artifactFilePath.resolve("smallJar.jar").toUri
    artifactManager.addArtifacts(file1, file2)
    val receivedRequests = service.getAndClearLatestAddArtifactRequests()
    assert(receivedRequests.size == 1)

    val request = receivedRequests.head
    assert(request.hasBatch)

    val batch = request.getBatch
    assert(batch.getArtifactsList.size() == 2)

    val artifacts = batch.getArtifactsList
    assert(artifacts.get(0).getName == "classes/smallClassFile.class")
    assert(artifacts.get(1).getName == "jars/smallJar.jar")

    assertFileDataEquality(artifacts.get(0).getData, Paths.get(file1))
    assertFileDataEquality(artifacts.get(1).getData, Paths.get(file2))
  }
}
