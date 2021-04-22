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

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * Clustering specific configs.
 */
public class HoodieClusteringConfig extends DefaultHoodieConfig {

  public static final ConfigOption<String> CLUSTERING_PLAN_STRATEGY_CLASS = ConfigOption
      .key("hoodie.clustering.plan.strategy.class")
      .defaultValue("org.apache.hudi.client.clustering.plan.strategy.SparkRecentDaysClusteringPlanStrategy")
      .withDescription("Config to provide a strategy class to create ClusteringPlan. Class has to be subclass of ClusteringPlanStrategy");

  public static final ConfigOption<String> CLUSTERING_EXECUTION_STRATEGY_CLASS = ConfigOption
      .key("hoodie.clustering.execution.strategy.class")
      .defaultValue("org.apache.hudi.client.clustering.run.strategy.SparkSortAndSizeExecutionStrategy")
      .withDescription("Config to provide a strategy class to execute a ClusteringPlan. Class has to be subclass of RunClusteringStrategy");

  public static final ConfigOption<String> INLINE_CLUSTERING_PROP = ConfigOption
      .key("hoodie.clustering.inline")
      .defaultValue("false")
      .withDescription("Turn on inline clustering - clustering will be run after write operation is complete");

  public static final ConfigOption<String> INLINE_CLUSTERING_MAX_COMMIT_PROP = ConfigOption
      .key("hoodie.clustering.inline.max.commits")
      .defaultValue("4")
      .withDescription("Config to control frequency of clustering");

  // Any strategy specific params can be saved with this prefix
  public static final String CLUSTERING_STRATEGY_PARAM_PREFIX = "hoodie.clustering.plan.strategy.";

  public static final ConfigOption<String> CLUSTERING_TARGET_PARTITIONS = ConfigOption
      .key(CLUSTERING_STRATEGY_PARAM_PREFIX + "daybased.lookback.partitions")
      .defaultValue("2")
      .withDescription("Number of partitions to list to create ClusteringPlan");

  public static final ConfigOption<String> CLUSTERING_PLAN_SMALL_FILE_LIMIT = ConfigOption
      .key(CLUSTERING_STRATEGY_PARAM_PREFIX + "small.file.limit")
      .defaultValue(String.valueOf(600 * 1024 * 1024L))
      .withDescription("Files smaller than the size specified here are candidates for clustering");

  public static final ConfigOption<String> CLUSTERING_MAX_BYTES_PER_GROUP = ConfigOption
      .key(CLUSTERING_STRATEGY_PARAM_PREFIX + "max.bytes.per.group")
      .defaultValue(String.valueOf(2 * 1024 * 1024 * 1024L))
      .withDescription("Each clustering operation can create multiple groups. Total amount of data processed by clustering operation"
          + " is defined by below two properties (CLUSTERING_MAX_BYTES_PER_GROUP * CLUSTERING_MAX_NUM_GROUPS)."
          + " Max amount of data to be included in one group");

  public static final ConfigOption<String> CLUSTERING_MAX_NUM_GROUPS = ConfigOption
      .key(CLUSTERING_STRATEGY_PARAM_PREFIX + "max.num.groups")
      .defaultValue("30")
      .withDescription("Maximum number of groups to create as part of ClusteringPlan. Increasing groups will increase parallelism");

  public static final ConfigOption<String> CLUSTERING_TARGET_FILE_MAX_BYTES = ConfigOption
      .key(CLUSTERING_STRATEGY_PARAM_PREFIX + "target.file.max.bytes")
      .defaultValue(String.valueOf(1 * 1024 * 1024 * 1024L))
      .withDescription("Each group can produce 'N' (CLUSTERING_MAX_GROUP_SIZE/CLUSTERING_TARGET_FILE_SIZE) output file groups");

  public static final ConfigOption<String> CLUSTERING_SORT_COLUMNS_PROPERTY = ConfigOption
      .key(CLUSTERING_STRATEGY_PARAM_PREFIX + "sort.columns")
      .noDefaultValue()
      .withDescription("Constants related to clustering that may be used by more than 1 strategy");

  public static final ConfigOption<String> CLUSTERING_UPDATES_STRATEGY_PROP = ConfigOption
      .key("hoodie.clustering.updates.strategy")
      .defaultValue("org.apache.hudi.client.clustering.update.strategy.SparkRejectUpdateStrategy")
      .withDescription("When file groups is in clustering, need to handle the update to these file groups. Default strategy just reject the update");

  public static final ConfigOption<String> ASYNC_CLUSTERING_ENABLE_OPT_KEY = ConfigOption
      .key("hoodie.clustering.async.enabled")
      .defaultValue("false")
      .withDescription("Async clustering");

  public HoodieClusteringConfig(Properties props) {
    super(props);
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    private final Properties props = new Properties();

    public Builder fromFile(File propertiesFile) throws IOException {
      try (FileReader reader = new FileReader(propertiesFile)) {
        this.props.load(reader);
        return this;
      }
    }

    public Builder withClusteringPlanStrategyClass(String clusteringStrategyClass) {
      props.setProperty(CLUSTERING_PLAN_STRATEGY_CLASS.key(), clusteringStrategyClass);
      return this;
    }

    public Builder withClusteringExecutionStrategyClass(String runClusteringStrategyClass) {
      props.setProperty(CLUSTERING_EXECUTION_STRATEGY_CLASS.key(), runClusteringStrategyClass);
      return this;
    }

    public Builder withClusteringTargetPartitions(int clusteringTargetPartitions) {
      props.setProperty(CLUSTERING_TARGET_PARTITIONS.key(), String.valueOf(clusteringTargetPartitions));
      return this;
    }

    public Builder withClusteringPlanSmallFileLimit(long clusteringSmallFileLimit) {
      props.setProperty(CLUSTERING_PLAN_SMALL_FILE_LIMIT.key(), String.valueOf(clusteringSmallFileLimit));
      return this;
    }
    
    public Builder withClusteringSortColumns(String sortColumns) {
      props.setProperty(CLUSTERING_SORT_COLUMNS_PROPERTY.key(), sortColumns);
      return this;
    }

    public Builder withClusteringMaxBytesInGroup(long clusteringMaxGroupSize) {
      props.setProperty(CLUSTERING_MAX_BYTES_PER_GROUP.key(), String.valueOf(clusteringMaxGroupSize));
      return this;
    }

    public Builder withClusteringMaxNumGroups(int maxNumGroups) {
      props.setProperty(CLUSTERING_MAX_NUM_GROUPS.key(), String.valueOf(maxNumGroups));
      return this;
    }

    public Builder withClusteringTargetFileMaxBytes(long targetFileSize) {
      props.setProperty(CLUSTERING_TARGET_FILE_MAX_BYTES.key(), String.valueOf(targetFileSize));
      return this;
    }

    public Builder withInlineClustering(Boolean inlineClustering) {
      props.setProperty(INLINE_CLUSTERING_PROP.key(), String.valueOf(inlineClustering));
      return this;
    }

    public Builder withInlineClusteringNumCommits(int numCommits) {
      props.setProperty(INLINE_CLUSTERING_MAX_COMMIT_PROP.key(), String.valueOf(numCommits));
      return this;
    }

    public Builder fromProperties(Properties props) {
      this.props.putAll(props);
      return this;
    }

    public Builder withClusteringUpdatesStrategy(String updatesStrategyClass) {
      props.setProperty(CLUSTERING_UPDATES_STRATEGY_PROP.key(), updatesStrategyClass);
      return this;
    }

    public Builder withAsyncClustering(Boolean asyncClustering) {
      props.setProperty(ASYNC_CLUSTERING_ENABLE_OPT_KEY.key(), String.valueOf(asyncClustering));
      return this;
    }

    public HoodieClusteringConfig build() {
      HoodieClusteringConfig config = new HoodieClusteringConfig(props);

      setDefaultValue(props, CLUSTERING_PLAN_STRATEGY_CLASS);
      setDefaultValue(props, CLUSTERING_EXECUTION_STRATEGY_CLASS);
      setDefaultValue(props, CLUSTERING_MAX_BYTES_PER_GROUP);
      setDefaultValue(props, CLUSTERING_MAX_NUM_GROUPS);
      setDefaultValue(props, CLUSTERING_TARGET_FILE_MAX_BYTES);
      setDefaultValue(props, INLINE_CLUSTERING_PROP);
      setDefaultValue(props, INLINE_CLUSTERING_MAX_COMMIT_PROP);
      setDefaultValue(props, CLUSTERING_TARGET_PARTITIONS);
      setDefaultValue(props, CLUSTERING_PLAN_SMALL_FILE_LIMIT);
      setDefaultValue(props, CLUSTERING_UPDATES_STRATEGY_PROP);
      setDefaultValue(props, ASYNC_CLUSTERING_ENABLE_OPT_KEY);
      return config;
    }
  }
}
