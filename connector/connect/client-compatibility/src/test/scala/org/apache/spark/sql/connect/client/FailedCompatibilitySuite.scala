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

import java.io.File
import java.net.URLClassLoader

import org.apache.spark.SparkFunSuite

// This test fail to run as there is no jar, nor classes generated in the classpath.
class FailedCompatibilitySuite extends SparkFunSuite {
  test("compatibility API tests") {
    // Read the client compatibility jar from the classpath
    val classpath = System.getProperty("java.class.path")
    val jars = classpath.split(":")
    val clientJar = jars.filter(jar => jar.contains("client-compatibility/target/classes"))
    assert(clientJar.length == 1, "Classpath:\n" + jars.mkString("\n"))
    val classLoader: URLClassLoader =
      new URLClassLoader(Seq(new File(clientJar(0)).toURI.toURL).toArray)

    // Cannot find the target
    // scalastyle:off classforname
    assertThrows[ClassNotFoundException] {
      Class.forName(
        "org.apache.spark.sql.Dataset",
        true,
        classLoader)
    }
    // scalastyle:on classforname
  }
}
