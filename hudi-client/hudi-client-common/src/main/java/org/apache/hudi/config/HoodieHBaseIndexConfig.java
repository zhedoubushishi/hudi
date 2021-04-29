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

package org.apache.hudi.config;

import org.apache.hudi.common.config.ConfigOption;
import org.apache.hudi.common.config.DefaultHoodieConfig;
import org.apache.hudi.index.hbase.DefaultHBaseQPSResourceAllocator;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class HoodieHBaseIndexConfig extends DefaultHoodieConfig {

  public static final ConfigOption<String> HBASE_ZKQUORUM_PROP = ConfigOption
      .key("hoodie.index.hbase.zkquorum")
      .noDefaultValue()
      .withDescription("Only applies if index type is HBASE. HBase ZK Quorum url to connect to");

  public static final ConfigOption<String> HBASE_ZKPORT_PROP = ConfigOption
      .key("hoodie.index.hbase.zkport")
      .noDefaultValue()
      .withDescription("Only applies if index type is HBASE. HBase ZK Quorum port to connect to");

  public static final ConfigOption<String> HBASE_TABLENAME_PROP = ConfigOption
      .key("hoodie.index.hbase.table")
      .noDefaultValue()
      .withDescription("Only applies if index type is HBASE. HBase Table name to use as the index. "
          + "Hudi stores the row_key and [partition_path, fileID, commitTime] mapping in the table");

  public static final ConfigOption<Integer> HBASE_GET_BATCH_SIZE_PROP = ConfigOption
      .key("hoodie.index.hbase.get.batch.size")
      .defaultValue(100)
      .withDescription("");

  public static final ConfigOption<String> HBASE_ZK_ZNODEPARENT = ConfigOption
      .key("hoodie.index.hbase.zknode.path")
      .noDefaultValue()
      .withDescription("Only applies if index type is HBASE. This is the root znode that will contain "
          + "all the znodes created/used by HBase");

  public static final ConfigOption<Integer> HBASE_PUT_BATCH_SIZE_PROP = ConfigOption
      .key("hoodie.index.hbase.put.batch.size")
      .defaultValue(100)
      .withDescription("");

  public static final ConfigOption<String> HBASE_INDEX_QPS_ALLOCATOR_CLASS = ConfigOption
      .key("hoodie.index.hbase.qps.allocator.class")
      .defaultValue(DefaultHBaseQPSResourceAllocator.class.getName())
      .withDescription("Property to set which implementation of HBase QPS resource allocator to be used");

  public static final ConfigOption<String> HBASE_PUT_BATCH_SIZE_AUTO_COMPUTE_PROP = ConfigOption
      .key("hoodie.index.hbase.put.batch.size.autocompute")
      .defaultValue("false")
      .withDescription("Property to set to enable auto computation of put batch size");

  public static final ConfigOption<Float> HBASE_QPS_FRACTION_PROP = ConfigOption
      .key("hoodie.index.hbase.qps.fraction")
      .defaultValue(0.5f)
      .withDescription("Property to set the fraction of the global share of QPS that should be allocated to this job. Let's say there are 3"
          + " jobs which have input size in terms of number of rows required for HbaseIndexing as x, 2x, 3x respectively. Then"
          + " this fraction for the jobs would be (0.17) 1/6, 0.33 (2/6) and 0.5 (3/6) respectively."
          + " Default is 50%, which means a total of 2 jobs can run using HbaseIndex without overwhelming Region Servers.");

  public static final ConfigOption<Integer> HBASE_MAX_QPS_PER_REGION_SERVER_PROP = ConfigOption
      .key("hoodie.index.hbase.max.qps.per.region.server")
      .defaultValue(1000)
      .withDescription("Property to set maximum QPS allowed per Region Server. This should be same across various jobs. This is intended to\n"
          + " limit the aggregate QPS generated across various jobs to an Hbase Region Server. It is recommended to set this\n"
          + " value based on global indexing throughput needs and most importantly, how much the HBase installation in use is\n"
          + " able to tolerate without Region Servers going down.");

  public static final ConfigOption<Boolean> HOODIE_INDEX_COMPUTE_QPS_DYNAMICALLY = ConfigOption
      .key("hoodie.index.hbase.dynamic_qps")
      .defaultValue(false)
      .withDescription("Property to decide if HBASE_QPS_FRACTION_PROP is dynamically calculated based on volume");

  public static final ConfigOption<String> HBASE_MIN_QPS_FRACTION_PROP = ConfigOption
      .key("hoodie.index.hbase.min.qps.fraction")
      .noDefaultValue()
      .withDescription("Min for HBASE_QPS_FRACTION_PROP to stabilize skewed volume workloads");

  public static final ConfigOption<String> HBASE_MAX_QPS_FRACTION_PROP = ConfigOption
      .key("hoodie.index.hbase.max.qps.fraction")
      .noDefaultValue()
      .withDescription("Max for HBASE_QPS_FRACTION_PROP to stabilize skewed volume workloads");

  public static final ConfigOption<Integer> HOODIE_INDEX_DESIRED_PUTS_TIME_IN_SECS = ConfigOption
      .key("hoodie.index.hbase.desired_puts_time_in_secs")
      .defaultValue(600)
      .withDescription("");

  public static final ConfigOption<String> HBASE_SLEEP_MS_PUT_BATCH_PROP = ConfigOption
      .key("hoodie.index.hbase.sleep.ms.for.put.batch")
      .noDefaultValue()
      .withDescription("");

  public static final ConfigOption<String> HBASE_SLEEP_MS_GET_BATCH_PROP = ConfigOption
      .key("hoodie.index.hbase.sleep.ms.for.get.batch")
      .noDefaultValue()
      .withDescription("");

  public static final ConfigOption<Integer> HOODIE_INDEX_HBASE_ZK_SESSION_TIMEOUT_MS = ConfigOption
      .key("hoodie.index.hbase.zk.session_timeout_ms")
      .defaultValue(60 * 1000)
      .withDescription("");

  public static final ConfigOption<Integer> HOODIE_INDEX_HBASE_ZK_CONNECTION_TIMEOUT_MS = ConfigOption
      .key("hoodie.index.hbase.zk.connection_timeout_ms")
      .defaultValue(15 * 1000)
      .withDescription("");

  public static final ConfigOption<String> HBASE_ZK_PATH_QPS_ROOT = ConfigOption
      .key("hoodie.index.hbase.zkpath.qps_root")
      .defaultValue("/QPS_ROOT")
      .withDescription("");

  public static final ConfigOption<Boolean> HBASE_INDEX_UPDATE_PARTITION_PATH = ConfigOption
      .key("hoodie.hbase.index.update.partition.path")
      .defaultValue(false)
      .withDescription("Only applies if index type is HBASE. "
          + "When an already existing record is upserted to a new partition compared to whats in storage, "
          + "this config when set, will delete old record in old paritition "
          + "and will insert it as new record in new partition.");

  public static final ConfigOption<Boolean> HBASE_INDEX_ROLLBACK_SYNC = ConfigOption
      .key("hoodie.index.hbase.rollback.sync")
      .defaultValue(false)
      .withDescription("When set to true, the rollback method will delete the last failed task index. "
          + "The default value is false. Because deleting the index will add extra load on the Hbase cluster for each rollback");

  public HoodieHBaseIndexConfig(final Properties props) {
    super(props);
  }

  public static HoodieHBaseIndexConfig.Builder newBuilder() {
    return new HoodieHBaseIndexConfig.Builder();
  }

  public static class Builder {

    private final Properties props = new Properties();

    public HoodieHBaseIndexConfig.Builder fromFile(File propertiesFile) throws IOException {
      try (FileReader reader = new FileReader(propertiesFile)) {
        this.props.load(reader);
        return this;
      }
    }

    public HoodieHBaseIndexConfig.Builder fromProperties(Properties props) {
      this.props.putAll(props);
      return this;
    }

    public HoodieHBaseIndexConfig.Builder hbaseZkQuorum(String zkString) {
      props.setProperty(HBASE_ZKQUORUM_PROP.key(), zkString);
      return this;
    }

    public HoodieHBaseIndexConfig.Builder hbaseZkPort(int port) {
      props.setProperty(HBASE_ZKPORT_PROP.key(), String.valueOf(port));
      return this;
    }

    public HoodieHBaseIndexConfig.Builder hbaseTableName(String tableName) {
      props.setProperty(HBASE_TABLENAME_PROP.key(), tableName);
      return this;
    }

    public Builder hbaseZkZnodeQPSPath(String zkZnodeQPSPath) {
      props.setProperty(HBASE_ZK_PATH_QPS_ROOT.key(), zkZnodeQPSPath);
      return this;
    }

    public Builder hbaseIndexGetBatchSize(int getBatchSize) {
      props.setProperty(HBASE_GET_BATCH_SIZE_PROP.key(), String.valueOf(getBatchSize));
      return this;
    }

    public Builder hbaseIndexPutBatchSize(int putBatchSize) {
      props.setProperty(HBASE_PUT_BATCH_SIZE_PROP.key(), String.valueOf(putBatchSize));
      return this;
    }

    public Builder hbaseIndexPutBatchSizeAutoCompute(boolean putBatchSizeAutoCompute) {
      props.setProperty(HBASE_PUT_BATCH_SIZE_AUTO_COMPUTE_PROP.key(), String.valueOf(putBatchSizeAutoCompute));
      return this;
    }

    public Builder hbaseIndexDesiredPutsTime(int desiredPutsTime) {
      props.setProperty(HOODIE_INDEX_DESIRED_PUTS_TIME_IN_SECS.key(), String.valueOf(desiredPutsTime));
      return this;
    }

    public Builder hbaseIndexShouldComputeQPSDynamically(boolean shouldComputeQPsDynamically) {
      props.setProperty(HOODIE_INDEX_COMPUTE_QPS_DYNAMICALLY.key(), String.valueOf(shouldComputeQPsDynamically));
      return this;
    }

    public Builder hbaseIndexQPSFraction(float qpsFraction) {
      props.setProperty(HBASE_QPS_FRACTION_PROP.key(), String.valueOf(qpsFraction));
      return this;
    }

    public Builder hbaseIndexMinQPSFraction(float minQPSFraction) {
      props.setProperty(HBASE_MIN_QPS_FRACTION_PROP.key(), String.valueOf(minQPSFraction));
      return this;
    }

    public Builder hbaseIndexMaxQPSFraction(float maxQPSFraction) {
      props.setProperty(HBASE_MAX_QPS_FRACTION_PROP.key(), String.valueOf(maxQPSFraction));
      return this;
    }

    public Builder hbaseIndexSleepMsBetweenPutBatch(int sleepMsBetweenPutBatch) {
      props.setProperty(HBASE_SLEEP_MS_PUT_BATCH_PROP.key(), String.valueOf(sleepMsBetweenPutBatch));
      return this;
    }

    public Builder hbaseIndexSleepMsBetweenGetBatch(int sleepMsBetweenGetBatch) {
      props.setProperty(HBASE_SLEEP_MS_GET_BATCH_PROP.key(), String.valueOf(sleepMsBetweenGetBatch));
      return this;
    }

    public Builder hbaseIndexUpdatePartitionPath(boolean updatePartitionPath) {
      props.setProperty(HBASE_INDEX_UPDATE_PARTITION_PATH.key(), String.valueOf(updatePartitionPath));
      return this;
    }

    public Builder hbaseIndexRollbackSync(boolean rollbackSync) {
      props.setProperty(HBASE_INDEX_ROLLBACK_SYNC.key(), String.valueOf(rollbackSync));
      return this;
    }

    public Builder withQPSResourceAllocatorType(String qpsResourceAllocatorClass) {
      props.setProperty(HBASE_INDEX_QPS_ALLOCATOR_CLASS.key(), qpsResourceAllocatorClass);
      return this;
    }

    public Builder hbaseIndexZkSessionTimeout(int zkSessionTimeout) {
      props.setProperty(HOODIE_INDEX_HBASE_ZK_SESSION_TIMEOUT_MS.key(), String.valueOf(zkSessionTimeout));
      return this;
    }

    public Builder hbaseIndexZkConnectionTimeout(int zkConnectionTimeout) {
      props.setProperty(HOODIE_INDEX_HBASE_ZK_CONNECTION_TIMEOUT_MS.key(), String.valueOf(zkConnectionTimeout));
      return this;
    }

    public Builder hbaseZkZnodeParent(String zkZnodeParent) {
      props.setProperty(HBASE_ZK_ZNODEPARENT.key(), zkZnodeParent);
      return this;
    }

    /**
     * <p>
     * Method to set maximum QPS allowed per Region Server. This should be same across various jobs. This is intended to
     * limit the aggregate QPS generated across various jobs to an HBase Region Server.
     * </p>
     * <p>
     * It is recommended to set this value based on your global indexing throughput needs and most importantly, how much
     * your HBase installation is able to tolerate without Region Servers going down.
     * </p>
     */
    public HoodieHBaseIndexConfig.Builder hbaseIndexMaxQPSPerRegionServer(int maxQPSPerRegionServer) {
      // This should be same across various jobs
      props.setProperty(HoodieHBaseIndexConfig.HBASE_MAX_QPS_PER_REGION_SERVER_PROP.key(),
          String.valueOf(maxQPSPerRegionServer));
      return this;
    }

    public HoodieHBaseIndexConfig build() {
      HoodieHBaseIndexConfig config = new HoodieHBaseIndexConfig(props);
      setDefaultValue(props, HBASE_GET_BATCH_SIZE_PROP);
      setDefaultValue(props, HBASE_PUT_BATCH_SIZE_PROP);
      setDefaultValue(props, HBASE_PUT_BATCH_SIZE_AUTO_COMPUTE_PROP);
      setDefaultValue(props, HBASE_QPS_FRACTION_PROP);
      setDefaultValue(props, HBASE_MAX_QPS_PER_REGION_SERVER_PROP);
      setDefaultValue(props, HOODIE_INDEX_COMPUTE_QPS_DYNAMICALLY);
      setDefaultValue(props, HBASE_INDEX_QPS_ALLOCATOR_CLASS);
      setDefaultValue(props, HOODIE_INDEX_DESIRED_PUTS_TIME_IN_SECS);
      setDefaultValue(props, HBASE_ZK_PATH_QPS_ROOT);
      setDefaultValue(props, HOODIE_INDEX_HBASE_ZK_SESSION_TIMEOUT_MS);
      setDefaultValue(props, HOODIE_INDEX_HBASE_ZK_CONNECTION_TIMEOUT_MS);
      setDefaultValue(props, HBASE_INDEX_QPS_ALLOCATOR_CLASS);
      setDefaultValue(props, HBASE_INDEX_UPDATE_PARTITION_PATH);
      setDefaultValue(props, HBASE_INDEX_ROLLBACK_SYNC);
      return config;
    }

  }
}
