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

package org.apache.hudi.client;

import java.io.IOException;
import java.util.Arrays;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;

public class TestBootstrap extends TestHoodieClientBase {

  public String generateNewDataSet(int numRecords, List<String> partitionPaths) throws IOException {
    JavaRDD rdd = jsc.parallelize(TestRecordGenerator.generateTestRecords(numRecords, partitionPaths));
    Dataset<Row> df = sqlContext.createDataFrame(rdd, TestRecord.class);
    TemporaryFolder tf = new TemporaryFolder();
    tf.create();
    String sourcePath = tf.getRoot().getAbsolutePath() + "/data";
    df.printSchema();
    df.write().partitionBy(TestRecord.PARTITION_PATH_COLUMN).format("parquet").save(sourcePath);
    return sourcePath;
  }

  @Test
  public void testMetadataBootstrapOnly() throws IOException {
    int totalRecords = 1000;
    String srcPath = generateNewDataSet(totalRecords, Arrays.asList("2020/04/01", "2020/04/02", "2020/04/03"));
    System.out.println("Source Path :" + srcPath);

  }

  public static class TestRecord implements Serializable {
    public String partitionPath;
    public int id;
    public String name;
    public float salary;

    private static final String PARTITION_PATH_COLUMN = "partitionPath";

    public TestRecord() {
    }

    public TestRecord(String partitionPath, int id, String name, float salary) {
      this.partitionPath = partitionPath;
      this.id = id;
      this.name = name;
      this.salary = salary;
    }

    public String getPartitionPath() {
      return partitionPath;
    }

    public void setPartitionPath(String partitionPath) {
      this.partitionPath = partitionPath;
    }

    public int getId() {
      return id;
    }

    public void setId(int id) {
      this.id = id;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public float getSalary() {
      return salary;
    }

    public void setSalary(float salary) {
      this.salary = salary;
    }

    public static String getPartitionPathColumn() {
      return PARTITION_PATH_COLUMN;
    }
  }

  private static class TestRecordGenerator {
    private static int genId = 0;
    private static Random rand = new Random();

    private static List<TestRecord> generateTestRecords(int numRecords, List<String> partitionPaths) {
      final List<TestRecord> records = new ArrayList<>();
      IntStream.range(0, numRecords).forEach(i -> {
        genId++;
        records.add(new TestRecord(partitionPaths.get(rand.nextInt(partitionPaths.size())),
            genId, "name_" + genId, rand.nextFloat()));
      });
      return records;
    }
  }
}