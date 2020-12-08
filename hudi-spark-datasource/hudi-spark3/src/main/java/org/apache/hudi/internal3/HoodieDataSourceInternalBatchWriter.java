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

import org.apache.hudi.DataSourceUtils;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.table.HoodieTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Implementation of {@link BatchWrite} for datasource "hudi.internal" to be used in datasource implementation
 * of bulk insert.
 */
public class HoodieDataSourceInternalBatchWriter implements BatchWrite {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LogManager.getLogger(HoodieDataSourceInternalBatchWriter.class);
  public static final String INSTANT_TIME_OPT_KEY = "hoodie.instant.time";

  private final String instantTime;
  private final HoodieTableMetaClient metaClient;
  private final HoodieWriteConfig writeConfig;
  private final StructType structType;
  private final SparkRDDWriteClient writeClient;
  private final HoodieTable hoodieTable;
  private final WriteOperationType operationType = WriteOperationType.BULK_INSERT;

  public HoodieDataSourceInternalBatchWriter(String instantTime, HoodieWriteConfig writeConfig, StructType structType,
      SparkRDDWriteClient writeClient, HoodieTableMetaClient metaClient, HoodieTable hoodieTable) {
    this.instantTime = instantTime;
    this.writeConfig = writeConfig;
    this.structType = structType;
    this.writeClient = writeClient;
    this.metaClient = metaClient;
    this.hoodieTable = hoodieTable;
  }

  @Override
  public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
    metaClient.getActiveTimeline().transitionRequestedToInflight(
        new HoodieInstant(State.REQUESTED, HoodieTimeline.COMMIT_ACTION, instantTime), Option.empty());
    if (WriteOperationType.BULK_INSERT == operationType) {
      return new HoodieBulkInsertDataInternalWriterFactory(hoodieTable, writeConfig, instantTime, structType);
    } else {
      throw new IllegalArgumentException("Write Operation Type + " + operationType + " not supported ");
    }
  }

  @Override
  public boolean useCommitCoordinator() {
    return true;
  }

  @Override
  public void onDataWriterCommit(WriterCommitMessage message) {
    LOG.info("Received commit of a data writer =" + message);
  }

  @Override
  public void commit(WriterCommitMessage[] messages) {
    List<HoodieWriteStat> writeStatList = Arrays.stream(messages).map(m -> (HoodieWriterCommitMessage) m)
        .flatMap(m -> m.getWriteStatuses().stream().map(m2 -> m2.getStat())).collect(Collectors.toList());

    try {
      writeClient.commitStats(instantTime, writeStatList, Option.empty(),
          DataSourceUtils.getCommitActionType(operationType, metaClient.getTableType()));
    } catch (Exception ioe) {
      throw new HoodieException(ioe.getMessage(), ioe);
    } finally {
      writeClient.close();
    }
  }

  @Override
  public void abort(WriterCommitMessage[] messages) {
    LOG.error("Commit " + instantTime + " aborted ");
    writeClient.rollback(instantTime);
    writeClient.close();
  }
}
