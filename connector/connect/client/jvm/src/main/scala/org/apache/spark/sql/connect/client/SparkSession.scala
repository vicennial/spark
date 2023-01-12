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

import org.apache.spark.connect.proto

class SparkSession private[client](val client: SparkConnectClient) {

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

  private[client] def newDataset(f: proto.Relation.Builder => Unit): Dataset = {
    val builder = proto.Relation.newBuilder()
    f(builder)
    val plan = proto.Plan.newBuilder().setRoot(builder).build()
    new Dataset(this, plan)
  }
}

object SparkSession {
  def apply(client: SparkConnectClient): SparkSession =
    new SparkSession(client)

  def apply(connectionString: String): SparkSession = {
    val client = SparkConnectClient.builder().connectionString(connectionString).build()
    new SparkSession(client)
  }
}
