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

package org.apache.hudi

import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession

object WenningdTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Bootstrap")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val df = spark.emptyDataFrame
    val tableType = HoodieTableType.MERGE_ON_READ.name
    val bootstrapTableName = "bootstrap_inventory_mor_14"
    val sourceTableName = "inventory"
    val sourcePath = "s3://wenningd-emr-dev/tcpds/wenningd_tpcds_partitioned30M_3TB_parquet_2000tasks_default/" + sourceTableName + "/"
    val tablePath = "s3://wenningd-emr-dev/bootstrap/" + bootstrapTableName + "/"

    df.write
      .format("hudi")
      .option(HoodieWriteConfig.TABLE_NAME, bootstrapTableName)
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.BOOTSTRAP_OPERATION_OPT_VAL)
      .option("hoodie.bootstrap.source.base.path", sourcePath)
      .option("hoodie.bootstrap.recordkey.columns", "inv_item_sk,inv_warehouse_sk")
      .option("hoodie.bootstrap.keygen.class", "org.apache.hudi.keygen.ComplexKeyGenerator")
      .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY, tableType)
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "inv_item_sk,inv_warehouse_sk")
      .option(DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY, "true")
      .option(DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY, "default")
      .option(DataSourceWriteOptions.HIVE_TABLE_OPT_KEY, bootstrapTableName)
      .option(DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY, "inv_date_sk")
      .option(DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY, "org.apache.hudi.hive.MultiPartKeysValueExtractor")
      .mode(SaveMode.Overwrite)
      .save(tablePath)
  }
}
