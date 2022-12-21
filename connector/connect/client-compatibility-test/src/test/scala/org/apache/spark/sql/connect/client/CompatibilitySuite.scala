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

// Pros: The test is both accessible by SBT and Maven
// Cons: Added a new module just for testing, will we end up release a empty module?
class CompatibilitySuite extends SparkFunSuite {
  test("compatibility API tests") {
    // Read the client compatibility jar from the classpath
    val classpath = System.getProperty("java.class.path")
    val jars = classpath.split(":")
    val clientJar = jars.filter(jar => jar.contains("spark-connect-client-compatibility"))
    assert(clientJar.length == 1)
    val classLoader: URLClassLoader =
      new URLClassLoader(Seq(new File(clientJar(0)).toURI.toURL).toArray)

    // Do something with the loaded jar
    // scalastyle:off classforname
    val classToLoad = Class.forName(
      "org.apache.spark.sql.Dataset",
      true,
      classLoader)
    // scalastyle:on classforname
    val methods = classToLoad.getMethods
    assert(methods.map(m => m.toString).contains(
      "public org.apache.spark.sql.Row[] org.apache.spark.sql.Dataset.collect()"
    ))
  }
}
