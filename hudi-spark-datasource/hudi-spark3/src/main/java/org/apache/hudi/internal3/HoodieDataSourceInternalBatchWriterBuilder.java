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

package org.apache.hudi.internal3;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;

/**
 * Implementation of {@link WriteBuilder} for datasource "hudi.internal3" to be used in datasource implementation
 * of bulk insert.
 */
public class HoodieDataSourceInternalBatchWriterBuilder implements WriteBuilder {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LogManager.getLogger(HoodieDataSourceInternalBatchWriterBuilder.class);
  public static final String INSTANT_TIME_OPT_KEY = "hoodie.instant.time";

  private final String instantTime;
  private final HoodieTableMetaClient metaClient;
  private final HoodieWriteConfig writeConfig;
  private final StructType structType;
  private final SparkRDDWriteClient writeClient;
  private final HoodieTable hoodieTable;

  public HoodieDataSourceInternalBatchWriterBuilder(String instantTime, HoodieWriteConfig writeConfig, StructType structType,
      SparkRDDWriteClient writeClient, HoodieTableMetaClient metaClient, HoodieTable hoodieTable) {
    this.instantTime = instantTime;
    this.writeConfig = writeConfig;
    this.structType = structType;
    this.writeClient = writeClient;
    this.metaClient = metaClient;
    this.hoodieTable = hoodieTable;
  }

  @Override
  public BatchWrite buildForBatch() {
    return new HoodieDataSourceInternalBatchWriter(instantTime, writeConfig, structType, writeClient,
        metaClient, hoodieTable);
  }
}
