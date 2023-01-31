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
package org.apache.spark.sql.connect.common

import com.google.protobuf.ByteString
import java.io.{InputStream, ObjectInputStream, ObjectOutputStream, OutputStream}

import org.apache.spark.sql.catalyst.encoders.AgnosticEncoder

case class UdfPacket(
  function: AnyRef,
  inputEncoders: Seq[AgnosticEncoder[_]],
  outputEncoder: AgnosticEncoder[_]) {
  def writeTo(out: OutputStream): Unit = {
    val oos = new ObjectOutputStream(out)
    oos.writeObject(this)
    oos.flush()
  }

  def toByteString: ByteString = {
    val out = ByteString.newOutput()
    writeTo(out)
    out.toByteString
  }
}

object UdfPacket {
  def apply(in: InputStream): UdfPacket = {
    val ois = new ObjectInputStream(in)
    ois.readObject().asInstanceOf[UdfPacket]
  }

  def apply(bytes: ByteString): UdfPacket = {
    val in = bytes.newInput()
    try UdfPacket(in) finally {
      in.close()
    }
  }
}
