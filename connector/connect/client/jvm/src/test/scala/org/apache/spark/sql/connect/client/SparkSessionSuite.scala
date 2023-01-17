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

import io.grpc.Server
import io.grpc.netty.NettyServerBuilder
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import java.util.concurrent.TimeUnit // scalastyle:ignore funsuite

class SparkSessionSuite
  extends AnyFunSuite // scalastyle:ignore funsuite
  with BeforeAndAfterEach{

  private var client: SparkConnectClient = _
  private var server: Server = _

  private def startDummyServer(port: Int): Unit = {
    val sb = NettyServerBuilder
      .forPort(port)
      .addService(new DummySparkConnectService())

    server = sb.build
    server.start()
  }
  override def beforeEach(): Unit = {
    super.beforeEach()
    client = null
    server = null
  }

  override def afterEach(): Unit = {
    if (server != null) {
      server.shutdownNow()
      assert(server.awaitTermination(5, TimeUnit.SECONDS), "server failed to shutdown")
    }

    if (client != null) {
      client.shutdown()
    }
  }

  test("SparkSession is") {
    
  }

}
