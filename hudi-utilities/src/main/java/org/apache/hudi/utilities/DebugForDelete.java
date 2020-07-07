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

package org.apache.hudi.utilities;

import java.util.HashMap;
import java.util.Map;
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;

public class DebugForDelete {

  public static void main(String[] args) {

    Map<String, String> additionalSparkConfigs = new HashMap<>();
    JavaSparkContext jssc =
        UtilHelpers.buildSparkContext("bootstrap", "local[2]", additionalSparkConfigs);
    SparkSession ss = SparkSession.builder().config(jssc.getConf()).getOrCreate();

    String bootstrapTableName = "bootstrap_test1_1";
    String sourceTableName = "test1";
    String sourcePath = "hdfs:///user/hive/" + sourceTableName + "/";
    String tablePath = "hdfs:///user/hive/" + bootstrapTableName + "/";
    String tableType = HoodieTableType.MERGE_ON_READ.name();

    Dataset<Row> originDf = ss.read().format("parquet").load(sourcePath);
    Dataset<Row> df2 = originDf.limit(1);

    df2.write().format("org.apache.hudi").option(DataSourceWriteOptions.OPERATION_OPT_KEY(), "delete")
        .option(DataSourceWriteOptions.STORAGE_TYPE_OPT_KEY(), tableType)
        .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY(), "event_id")
        .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), "event_id")
        .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY(), "event_type")
        .option(DataSourceWriteOptions.KEYGENERATOR_CLASS_OPT_KEY(), "org.apache.hudi.keygen.ComplexKeyGenerator")
        .option(HoodieWriteConfig.TABLE_NAME, bootstrapTableName)
        .option(DataSourceWriteOptions.HIVE_STYLE_PARTITIONING_OPT_KEY(), "true")
        .option("hoodie.compact.inline", "false")
        .mode(SaveMode.Append)
        .save(tablePath);
  }
}
