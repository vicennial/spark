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

class Column private[client](private[client] val expr: proto.Expression) {
  /**
   * Gives the column a name (alias).
   * {{{
   *   // Renames colA to colB in select output.
   *   df.select($"colA".name("colB"))
   * }}}
   *
   * If the current column has metadata associated with it, this metadata will be propagated
   * to the new column. If this not desired, use the API `as(alias: String, metadata: Metadata)`
   * with explicit metadata.
   *
   * @group expr_ops
   * @since 3.4.0
   */
  def name(alias: String): Column = Column { builder =>
    builder.getAliasBuilder.addName(alias).setExpr(expr)
  }
}

object Column {

  def apply(name: String): Column = Column { builder =>
    name match {
      case "*" =>
        builder.getUnresolvedStarBuilder
      case _ if name.endsWith(".*") =>
        throw new UnsupportedOperationException("* with prefix is not supported yet.")
      case _ =>
        builder.getUnresolvedAttributeBuilder.setUnparsedIdentifier(name)
    }
  }

  def apply(f: proto.Expression.Builder => Unit): Column = {
    val builder = proto.Expression.newBuilder()
    f(builder)
    new Column(builder.build())
  }
}
