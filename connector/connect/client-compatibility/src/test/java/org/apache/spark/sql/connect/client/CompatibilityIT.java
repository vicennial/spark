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
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;

// Pros: No new module added.
// Cons: The test has to be an IT as it can only run after the jar is generated using 'mvn package'.
//       The test is mostly only usable for maven users.
//       As when running with SBT, SBT will not be able to compile the test code before running.
public class CompatibilityIT {

    @Test
    public void testAPICompatibility() throws Throwable {
        String classpath = System.getProperty("java.class.path");
        String[] split = classpath.split(":");
        List<String> clientJar = Arrays.stream(split)
                .filter(jar -> jar.contains("spark-connect-client-compatibility"))
                .collect(Collectors.toList());

        assertThat(clientJar.size(), equalTo(1));

        URLClassLoader classLoader = new URLClassLoader(new URL[]{
                new File(clientJar.get(0)).toURI().toURL()
        });
        Class<?> classToLoad = Class.forName(
                "org.apache.spark.sql.Dataset",
                true,
                classLoader);
        Method[] methods = classToLoad.getMethods();
        assertThat(Arrays.stream(methods).map(Method::toString).collect(Collectors.toList()),
                hasItem("public org.apache.spark.sql.Row[] org.apache.spark.sql.Dataset.collect()"));
    }
}
