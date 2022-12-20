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
package org.apache.spark.sql.connect.client;

import org.junit.Test;

import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;

// Run with "mvn failsafe:integration-test"
// This test cannot run with SBT, because the test is defined to run at Maven verify stage
// (The full Maven build cycle: compile, test, package, verify, install).
// Calling this test via SBT may not recompile any changes added to this test and result in
// undefined behavior. Thus this test is skipped in SBT tests, but can be run as part of mvn verify.
@ConnectCompatibilityTest
public class CompatibilityIT {
    // Path for maven
    File mvnShadedJarPath = new File("./connector/connect/client-compatibility/target/" +
            "spark-connect-client-compatibility_2.12-3.4.0-SNAPSHOT.jar");
    File sbtShadedJarPath = new File("./target/" +
            "scala-2.12/spark-connect-client-compatibility-assembly-3.4.0-SNAPSHOT.jar");
    // Path for sbt

    @Test
    public void testAPICompatibility() throws Throwable {
        if(!mvnShadedJarPath.exists()) {
            throw new RuntimeException("path: " + mvnShadedJarPath.getAbsolutePath());
        }
        URLClassLoader classLoader = new URLClassLoader(new URL[]{
                mvnShadedJarPath.toURI().toURL(),
                sbtShadedJarPath.toURI().toURL()
        });
        // Compare shaded and sql jar
        // Shaded:
//        Class<?> classToLoad = classLoader.loadClass("org.apache.spark.sql.Dataset");
        Class<?> classToLoad = Class.forName(
                "org.apache.spark.sql.Dataset",
                true,
                classLoader);
        Method[] methods = classToLoad.getMethods();
        assertThat(Arrays.stream(methods).map(Method::toString).collect(Collectors.toList()),
                hasItem("public org.apache.spark.sql.Row[] org.apache.spark.sql.Dataset.collect()"));
    }
}
