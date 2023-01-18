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
package org.apache.spark.sql

import org.apache.arrow.memory.RootAllocator

import org.apache.spark.connect.proto
import org.apache.spark.sql.connect.client.{SparkConnectClient, SparkResult}
import org.apache.spark.sql.connect.client.util.Cleaner

/**
 * The entry point to programming Spark with the Dataset and DataFrame API.
 *
 * In environments that this has been created upfront (e.g. REPL, notebooks), use the builder to
 * get an existing session:
 *
 * {{{
 *   SparkSession.builder().getOrCreate()
 * }}}
 *
 * The builder can also be used to create a new session:
 *
 * {{{
 *   SparkSession.builder
 *     .master("local")
 *     .appName("Word Count")
 *     .config("spark.some.config.option", "some-value")
 *     .getOrCreate()
 * }}}
 */
class SparkSession(private val client: SparkConnectClient, private val cleaner: Cleaner)
    extends AutoCloseable {

  private[this] val allocator = new RootAllocator()

  /**
   * Executes a SQL query using Spark, returning the result as a `DataFrame`. This API eagerly
   * runs DDL/DML commands, but not for SELECT queries.
   *
   * @since 3.4.0
   */
  def sql(query: String): Dataset = newDataset { builder =>
    builder.setSql(proto.SQL.newBuilder().setQuery(query))
  }

  /**
   * Creates a [[Dataset]] with a single `LongType` column named `id`, containing elements
   * in a range from 0 to `end` (exclusive) with step value 1.
   *
   * @since 3.4.0
   */
  def range(end: Long): Dataset = range(0, end)

  /**
   * Creates a [[Dataset]] with a single `LongType` column named `id`, containing elements
   * in a range from `start` to `end` (exclusive) with step value 1.
   *
   * @since 3.4.0
   */
  def range(start: Long, end: Long): Dataset = {
    range(start, end, step = 1)
  }

  /**
   * Creates a [[Dataset]] with a single `LongType` column named `id`, containing elements
   * in a range from `start` to `end` (exclusive) with a step value.
   *
   * @since 3.4.0
   */
  def range(start: Long, end: Long, step: Long): Dataset = {
    range(start, end, step, None)
  }

  /**
   * Creates a [[Dataset]] with a single `LongType` column named `id`, containing elements
   * in a range from `start` to `end` (exclusive) with a step value, with partition number
   * specified.
   *
   * @since 3.4.0
   */
  def range(start: Long, end: Long, step: Long, numPartitions: Int): Dataset = {
    range(start, end, step, Option(numPartitions))
  }

  private def range(start: Long, end: Long, step: Long, numPartitions: Option[Int]): Dataset = {
    newDataset { builder =>
      val rangeBuilder = builder.getRangeBuilder
        .setStart(start)
        .setEnd(end)
        .setStep(step)
      numPartitions.foreach(rangeBuilder.setNumPartitions)
    }
  }

  private[sql] def newDataset(f: proto.Relation.Builder => Unit): Dataset = {
    val builder = proto.Relation.newBuilder()
    f(builder)
    val plan = proto.Plan.newBuilder().setRoot(builder).build()
    new Dataset(this, plan)
  }

  private[sql] def analyze(plan: proto.Plan): proto.AnalyzePlanResponse =
    client.analyze(plan)

  private[sql] def execute(plan: proto.Plan): SparkResult = {
    val value = client.execute(plan)
    val result = new SparkResult(value, allocator)
    cleaner.register(result)
    result
  }

  override def close(): Unit = {
    client.shutdown()
    allocator.close()
  }
}

object SparkSession {
  def builder(): Builder = new Builder()

  private lazy val cleaner = {
    val cleaner = new Cleaner
    cleaner.start()
    cleaner
  }

  def apply(client: SparkConnectClient): SparkSession =
    new SparkSession(client, cleaner)

  /** Helper class to create a [[SparkSession]] instance */
  class Builder() {
    private var userId: String = _
    private var host: String = _
    private var port: Int = -1
    private var connectionString: Option[String] = None

    def userId(id: String): Builder = {
      userId = id
      this
    }

    def host(inputHost: String): Builder = {
      require(inputHost != null)
      host = inputHost
      this
    }

    def port(inputPort: Int): Builder = {
      port = inputPort
      this
    }

    def connectionString(inputString: String): Builder = {
      require(inputString != null)
      connectionString = Some(inputString)
      this
    }

    def build(): SparkSession = {
      val client = if (connectionString.isDefined) {
        val builder = SparkConnectClient.builder().connectionString(connectionString.get)
        if (userId != null) builder.userId(userId)
        builder.build()
      } else {
        val builder = SparkConnectClient.builder()
        if (host != null) builder.host(host)
        if (port != -1) builder.port(port)
        if (userId != null) builder.userId(userId)
        builder.build()
      }
      new SparkSession(client, cleaner)
    }
  }
}
