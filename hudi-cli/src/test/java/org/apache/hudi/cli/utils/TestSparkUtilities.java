/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.cli.utils;

import org.apache.spark.api.java.JavaSparkContext;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Test class for {@link SparkUtil}.
 */
public class TestSparkUtilities {

  @Rule
  public final EnvironmentVariables env = new EnvironmentVariables();

  @Rule
  public final TemporaryFolder folder = new TemporaryFolder();

  @Before
  public void setUp() throws IOException {
    File sparkConfFile = folder.newFile("spark-defaults.conf");
    try (BufferedWriter bw = new BufferedWriter(new FileWriter(sparkConfFile))) {
      bw.write("spark.master=local[1]\n"
          + "spark.driver.extraClassPath=/driver\n"
          + "spark.eventLog.dir=" + folder.getRoot().getAbsolutePath());
    }
    env.set("SPARK_CONF_DIR", folder.getRoot().getAbsolutePath());
  }

  @Test
  public void testJavaSparkContextInitialization() {
    try (JavaSparkContext jsc = SparkUtil.initJavaSparkConf("test-app")) {
      Assert.assertEquals(jsc.getConf().get("spark.driver.extraClassPath"), "/driver");
    }
  }
}
