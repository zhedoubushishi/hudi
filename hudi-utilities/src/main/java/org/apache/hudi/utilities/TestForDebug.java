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
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class TestForDebug {

  public static void main(String[] args) throws Exception {

    Map<String, String> additionalSparkConfigs = new HashMap<>();
    JavaSparkContext jssc =
        UtilHelpers.buildSparkContext("bootstrap", "local[2]", additionalSparkConfigs);
    SparkSession ss = SparkSession.builder().config(jssc.getConf()).getOrCreate();

    Dataset<Row> df = ss.emptyDataFrame();
    String tableType = HoodieTableType.MERGE_ON_READ.name();
    String bootstrapTableName = "bootstrap_test1_1";
    String sourceTableName = "test1";
    String sourcePath = "hdfs:///user/hive/" + sourceTableName + "/";
    String tablePath = "hdfs:///user/hive/" + bootstrapTableName + "/";

    DataFrameWriter<Row> writer = df.write()
        .format("org.apache.hudi")
        .option(HoodieWriteConfig.TABLE_NAME, bootstrapTableName)
        .option(DataSourceWriteOptions.OPERATION_OPT_KEY(), DataSourceWriteOptions.BOOTSTRAP_OPERATION_OPT_VAL())
        .option("hoodie.bootstrap.source.base.path", sourcePath)
        .option("hoodie.bootstrap.recordkey.columns", "event_id")
        .option("hoodie.bootstrap.keygen.class", "org.apache.hudi.keygen.ComplexKeyGenerator")
        .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY(), tableType)
        .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), "event_id")
        .option(DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY(), "true")
        .option(DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY(), "default")
        .option(DataSourceWriteOptions.HIVE_TABLE_OPT_KEY(), bootstrapTableName)
        .option(DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY(), "event_type")
        .option(DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY(), "org.apache.hudi.hive.MultiPartKeysValueExtractor")
        .mode(SaveMode.Overwrite);

    writer.save(tablePath);
  }
}
