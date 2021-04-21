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
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.table.action.compact.CompactionTriggerStrategy;
import org.apache.hudi.table.action.compact.strategy.CompactionStrategy;
import org.apache.hudi.table.action.compact.strategy.LogFileSizeBasedCompactionStrategy;

import javax.annotation.concurrent.Immutable;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * Compaction related config.
 */
@Immutable
public class HoodieCompactionConfig extends DefaultHoodieConfig {

  public static final ConfigOption<String> CLEANER_POLICY_PROP = ConfigOption
      .key("hoodie.cleaner.policy")
      .defaultValue(HoodieCleaningPolicy.KEEP_LATEST_COMMITS.name())
      .withDescription("Cleaning policy to be used. Hudi will delete older versions of parquet files to re-claim space."
          + " Any Query/Computation referring to this version of the file will fail. "
          + "It is good to make sure that the data is retained for more than the maximum query execution time.");

  public static final ConfigOption<String> AUTO_CLEAN_PROP = ConfigOption
      .key("hoodie.clean.automatic")
      .defaultValue("true")
      .withDescription("Should cleanup if there is anything to cleanup immediately after the commit");

  public static final ConfigOption<String> ASYNC_CLEAN_PROP = ConfigOption
      .key("hoodie.clean.async")
      .defaultValue("false")
      .withDescription("Only applies when #withAutoClean is turned on. When turned on runs cleaner async with writing.");

  public static final ConfigOption<String> INLINE_COMPACT_PROP = ConfigOption
      .key("hoodie.compact.inline")
      .defaultValue("false")
      .withDescription("When set to true, compaction is triggered by the ingestion itself, "
          + "right after a commit/deltacommit action as part of insert/upsert/bulk_insert");

  public static final ConfigOption<String> INLINE_COMPACT_NUM_DELTA_COMMITS_PROP = ConfigOption
      .key("hoodie.compact.inline.max.delta.commits")
      .defaultValue("5")
      .withDescription("Number of max delta commits to keep before triggering an inline compaction");

  public static final ConfigOption<String> INLINE_COMPACT_TIME_DELTA_SECONDS_PROP = ConfigOption
      .key("hoodie.compact.inline.max.delta.seconds")
      .defaultValue(String.valueOf(60 * 60))
      .withDescription("Run a compaction when time elapsed > N seconds since last compaction");

  public static final ConfigOption<String> INLINE_COMPACT_TRIGGER_STRATEGY_PROP = ConfigOption
      .key("hoodie.compact.inline.trigger.strategy")
      .defaultValue(CompactionTriggerStrategy.NUM_COMMITS.name())
      .withDescription("");

  public static final ConfigOption<String> CLEANER_FILE_VERSIONS_RETAINED_PROP = ConfigOption
      .key("hoodie.cleaner.fileversions.retained")
      .defaultValue("3")
      .withDescription("");

  public static final ConfigOption<String> CLEANER_COMMITS_RETAINED_PROP = ConfigOption
      .key("hoodie.cleaner.commits.retained")
      .defaultValue("10")
      .withDescription("");

  public static final ConfigOption<String> CLEANER_INCREMENTAL_MODE = ConfigOption
      .key("hoodie.cleaner.incremental.mode")
      .defaultValue("true")
      .withDescription("");

  public static final ConfigOption<String> MAX_COMMITS_TO_KEEP_PROP = ConfigOption
      .key("hoodie.keep.max.commits")
      .defaultValue("30")
      .withDescription("");

  public static final ConfigOption<String> MIN_COMMITS_TO_KEEP_PROP = ConfigOption
      .key("hoodie.keep.min.commits")
      .defaultValue("20")
      .withDescription("");

  public static final ConfigOption<String> COMMITS_ARCHIVAL_BATCH_SIZE_PROP = ConfigOption
      .key("hoodie.commits.archival.batch")
      .defaultValue(String.valueOf(10))
      .withDescription("");

  public static final ConfigOption<String> CLEANER_BOOTSTRAP_BASE_FILE_ENABLED = ConfigOption
      .key("hoodie.cleaner.delete.bootstrap.base.file")
      .defaultValue("false")
      .withDescription("Set true to clean bootstrap source files when necessary");

  public static final ConfigOption<String> PARQUET_SMALL_FILE_LIMIT_BYTES = ConfigOption
      .key("hoodie.parquet.small.file.limit")
      .defaultValue(String.valueOf(104857600))
      .withDescription("Upsert uses this file size to compact new data onto existing files. "
          + "By default, treat any file <= 100MB as a small file.");

  public static final ConfigOption<String> RECORD_SIZE_ESTIMATION_THRESHOLD_PROP = ConfigOption
      .key("hoodie.record.size.estimation.threshold")
      .defaultValue("1.0")
      .withDescription("Hudi will use the previous commit to calculate the estimated record size by totalBytesWritten/totalRecordsWritten. "
          + "If the previous commit is too small to make an accurate estimation, Hudi will search commits in the reverse order, "
          + "until find a commit has totalBytesWritten larger than (PARQUET_SMALL_FILE_LIMIT_BYTES * RECORD_SIZE_ESTIMATION_THRESHOLD)");

  public static final ConfigOption<String> CLEANER_PARALLELISM = ConfigOption
      .key("hoodie.cleaner.parallelism")
      .defaultValue("200")
      .withDescription("");

  public static final ConfigOption<String> TARGET_IO_PER_COMPACTION_IN_MB_PROP = ConfigOption
      .key("hoodie.compaction.target.io")
      .defaultValue(String.valueOf(500 * 1024))
      .withDescription("500GB of target IO per compaction (both read and write");

  public static final ConfigOption<String> COMPACTION_STRATEGY_PROP = ConfigOption
      .key("hoodie.compaction.strategy")
      .defaultValue(LogFileSizeBasedCompactionStrategy.class.getName())
      .withDescription("");

  public static final ConfigOption<String> PAYLOAD_CLASS_PROP = ConfigOption
      .key("hoodie.compaction.payload.class")
      .defaultValue(OverwriteWithLatestAvroPayload.class.getName())
      .withDescription("");

  public static final ConfigOption<String> COMPACTION_LAZY_BLOCK_READ_ENABLED_PROP = ConfigOption
      .key("hoodie.compaction.lazy.block.read")
      .defaultValue("false")
      .withDescription("Used to choose a trade off between IO vs Memory when performing compaction process."
          + " Depending on output file_size and memory provided, choose true to avoid OOM for large file"
          + " size + small memory");

  public static final ConfigOption<String> COMPACTION_REVERSE_LOG_READ_ENABLED_PROP = ConfigOption
      .key("hoodie.compaction.reverse.log.read")
      .defaultValue("false")
      .withDescription("Used to choose whether to enable reverse log reading (reverse log traversal)");

  public static final ConfigOption<String> FAILED_WRITES_CLEANER_POLICY_PROP = ConfigOption
      .key("hoodie.cleaner.policy.failed.writes")
      .defaultValue(HoodieFailedWritesCleaningPolicy.EAGER.name())
      .withDescription("");

  public static final ConfigOption<String> TARGET_PARTITIONS_PER_DAYBASED_COMPACTION_PROP = ConfigOption
      .key("hoodie.compaction.daybased.target.partitions")
      .defaultValue("10")
      .withDescription("");

  /**
   * Configs related to specific table types.
   */
  public static final ConfigOption<String> COPY_ON_WRITE_TABLE_INSERT_SPLIT_SIZE = ConfigOption
      .key("hoodie.copyonwrite.insert.split.size")
      .defaultValue(String.valueOf(500000))
      .withDescription("Number of inserts, that will be put each partition/bucket for writing. "
          + "The rationale to pick the insert parallelism is the following. Writing out 100MB files, "
          + "with at least 1kb records, means 100K records per file. we just over provision to 500K.");

  public static final ConfigOption<String> COPY_ON_WRITE_TABLE_AUTO_SPLIT_INSERTS = ConfigOption
      .key("hoodie.copyonwrite.insert.auto.split")
      .defaultValue("true")
      .withDescription("Config to control whether we control insert split sizes automatically based on average"
          + " record sizes.");

  public static final ConfigOption<String> COPY_ON_WRITE_TABLE_RECORD_SIZE_ESTIMATE = ConfigOption
      .key("hoodie.copyonwrite.record.size.estimate")
      .defaultValue(String.valueOf(1024))
      .withDescription("This value is used as a guesstimate for the record size, if we can't determine this from\n"
          + "  previous commits");

  private HoodieCompactionConfig(Properties props) {
    super(props);
  }

  public static HoodieCompactionConfig.Builder newBuilder() {
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

    public Builder fromProperties(Properties props) {
      this.props.putAll(props);
      return this;
    }

    public Builder withAutoClean(Boolean autoClean) {
      props.setProperty(AUTO_CLEAN_PROP.key(), String.valueOf(autoClean));
      return this;
    }

    public Builder withAsyncClean(Boolean asyncClean) {
      props.setProperty(ASYNC_CLEAN_PROP.key(), String.valueOf(asyncClean));
      return this;
    }

    public Builder withIncrementalCleaningMode(Boolean incrementalCleaningMode) {
      props.setProperty(CLEANER_INCREMENTAL_MODE.key(), String.valueOf(incrementalCleaningMode));
      return this;
    }

    public Builder withInlineCompaction(Boolean inlineCompaction) {
      props.setProperty(INLINE_COMPACT_PROP.key(), String.valueOf(inlineCompaction));
      return this;
    }

    public Builder withInlineCompactionTriggerStrategy(CompactionTriggerStrategy compactionTriggerStrategy) {
      props.setProperty(INLINE_COMPACT_TRIGGER_STRATEGY_PROP.key(), compactionTriggerStrategy.name());
      return this;
    }

    public Builder withCleanerPolicy(HoodieCleaningPolicy policy) {
      props.setProperty(CLEANER_POLICY_PROP.key(), policy.name());
      return this;
    }

    public Builder retainFileVersions(int fileVersionsRetained) {
      props.setProperty(CLEANER_FILE_VERSIONS_RETAINED_PROP.key(), String.valueOf(fileVersionsRetained));
      return this;
    }

    public Builder retainCommits(int commitsRetained) {
      props.setProperty(CLEANER_COMMITS_RETAINED_PROP.key(), String.valueOf(commitsRetained));
      return this;
    }

    public Builder archiveCommitsWith(int minToKeep, int maxToKeep) {
      props.setProperty(MIN_COMMITS_TO_KEEP_PROP.key(), String.valueOf(minToKeep));
      props.setProperty(MAX_COMMITS_TO_KEEP_PROP.key(), String.valueOf(maxToKeep));
      return this;
    }

    public Builder compactionSmallFileSize(long smallFileLimitBytes) {
      props.setProperty(PARQUET_SMALL_FILE_LIMIT_BYTES.key(), String.valueOf(smallFileLimitBytes));
      return this;
    }

    public Builder compactionRecordSizeEstimateThreshold(double threshold) {
      props.setProperty(RECORD_SIZE_ESTIMATION_THRESHOLD_PROP.key(), String.valueOf(threshold));
      return this;
    }

    public Builder insertSplitSize(int insertSplitSize) {
      props.setProperty(COPY_ON_WRITE_TABLE_INSERT_SPLIT_SIZE.key(), String.valueOf(insertSplitSize));
      return this;
    }

    public Builder autoTuneInsertSplits(boolean autoTuneInsertSplits) {
      props.setProperty(COPY_ON_WRITE_TABLE_AUTO_SPLIT_INSERTS.key(), String.valueOf(autoTuneInsertSplits));
      return this;
    }

    public Builder approxRecordSize(int recordSizeEstimate) {
      props.setProperty(COPY_ON_WRITE_TABLE_RECORD_SIZE_ESTIMATE.key(), String.valueOf(recordSizeEstimate));
      return this;
    }

    public Builder withCleanerParallelism(int cleanerParallelism) {
      props.setProperty(CLEANER_PARALLELISM.key(), String.valueOf(cleanerParallelism));
      return this;
    }

    public Builder withCompactionStrategy(CompactionStrategy compactionStrategy) {
      props.setProperty(COMPACTION_STRATEGY_PROP.key(), compactionStrategy.getClass().getName());
      return this;
    }

    public Builder withPayloadClass(String payloadClassName) {
      props.setProperty(PAYLOAD_CLASS_PROP.key(), payloadClassName);
      return this;
    }

    public Builder withTargetIOPerCompactionInMB(long targetIOPerCompactionInMB) {
      props.setProperty(TARGET_IO_PER_COMPACTION_IN_MB_PROP.key(), String.valueOf(targetIOPerCompactionInMB));
      return this;
    }

    public Builder withMaxNumDeltaCommitsBeforeCompaction(int maxNumDeltaCommitsBeforeCompaction) {
      props.setProperty(INLINE_COMPACT_NUM_DELTA_COMMITS_PROP.key(), String.valueOf(maxNumDeltaCommitsBeforeCompaction));
      return this;
    }

    public Builder withMaxDeltaSecondsBeforeCompaction(int maxDeltaSecondsBeforeCompaction) {
      props.setProperty(INLINE_COMPACT_TIME_DELTA_SECONDS_PROP.key(), String.valueOf(maxDeltaSecondsBeforeCompaction));
      return this;
    }

    public Builder withCompactionLazyBlockReadEnabled(Boolean compactionLazyBlockReadEnabled) {
      props.setProperty(COMPACTION_LAZY_BLOCK_READ_ENABLED_PROP.key(), String.valueOf(compactionLazyBlockReadEnabled));
      return this;
    }

    public Builder withCompactionReverseLogReadEnabled(Boolean compactionReverseLogReadEnabled) {
      props.setProperty(COMPACTION_REVERSE_LOG_READ_ENABLED_PROP.key(), String.valueOf(compactionReverseLogReadEnabled));
      return this;
    }

    public Builder withTargetPartitionsPerDayBasedCompaction(int targetPartitionsPerCompaction) {
      props.setProperty(TARGET_PARTITIONS_PER_DAYBASED_COMPACTION_PROP.key(), String.valueOf(targetPartitionsPerCompaction));
      return this;
    }

    public Builder withCommitsArchivalBatchSize(int batchSize) {
      props.setProperty(COMMITS_ARCHIVAL_BATCH_SIZE_PROP.key(), String.valueOf(batchSize));
      return this;
    }

    public Builder withCleanBootstrapBaseFileEnabled(Boolean cleanBootstrapSourceFileEnabled) {
      props.setProperty(CLEANER_BOOTSTRAP_BASE_FILE_ENABLED.key(), String.valueOf(cleanBootstrapSourceFileEnabled));
      return this;
    }

    public Builder withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy failedWritesPolicy) {
      props.setProperty(FAILED_WRITES_CLEANER_POLICY_PROP.key(), failedWritesPolicy.name());
      return this;
    }

    public HoodieCompactionConfig build() {
      HoodieCompactionConfig config = new HoodieCompactionConfig(props);
      setDefaultValue(props, AUTO_CLEAN_PROP);
      setDefaultValue(props, ASYNC_CLEAN_PROP);
      setDefaultValue(props, CLEANER_INCREMENTAL_MODE);
      setDefaultValue(props, INLINE_COMPACT_PROP);
      setDefaultValue(props, INLINE_COMPACT_NUM_DELTA_COMMITS_PROP);
      setDefaultValue(props, INLINE_COMPACT_TIME_DELTA_SECONDS_PROP);
      setDefaultValue(props, INLINE_COMPACT_TRIGGER_STRATEGY_PROP);
      setDefaultValue(props, CLEANER_POLICY_PROP);
      setDefaultValue(props, CLEANER_FILE_VERSIONS_RETAINED_PROP);
      setDefaultValue(props, CLEANER_COMMITS_RETAINED_PROP);
      setDefaultValue(props, MAX_COMMITS_TO_KEEP_PROP);
      setDefaultValue(props, MIN_COMMITS_TO_KEEP_PROP);
      setDefaultValue(props, PARQUET_SMALL_FILE_LIMIT_BYTES);
      setDefaultValue(props, RECORD_SIZE_ESTIMATION_THRESHOLD_PROP);
      setDefaultValue(props, COPY_ON_WRITE_TABLE_INSERT_SPLIT_SIZE);
      setDefaultValue(props, COPY_ON_WRITE_TABLE_AUTO_SPLIT_INSERTS);
      setDefaultValue(props, COPY_ON_WRITE_TABLE_RECORD_SIZE_ESTIMATE);
      setDefaultValue(props, CLEANER_PARALLELISM);
      setDefaultValue(props, COMPACTION_STRATEGY_PROP);
      setDefaultValue(props, PAYLOAD_CLASS_PROP);
      setDefaultValue(props, TARGET_IO_PER_COMPACTION_IN_MB_PROP);
      setDefaultValue(props, COMPACTION_LAZY_BLOCK_READ_ENABLED_PROP);
      setDefaultValue(props, COMPACTION_REVERSE_LOG_READ_ENABLED_PROP);
      setDefaultValue(props, TARGET_PARTITIONS_PER_DAYBASED_COMPACTION_PROP);
      setDefaultValue(props, COMMITS_ARCHIVAL_BATCH_SIZE_PROP);
      setDefaultValue(props, CLEANER_BOOTSTRAP_BASE_FILE_ENABLED);
      setDefaultValue(props, FAILED_WRITES_CLEANER_POLICY_PROP);
      HoodieCleaningPolicy.valueOf(props.getProperty(CLEANER_POLICY_PROP.key()));

      // Ensure minInstantsToKeep > cleanerCommitsRetained, otherwise we will archive some
      // commit instant on timeline, that still has not been cleaned. Could miss some data via incr pull
      int minInstantsToKeep = Integer.parseInt(getString(props, HoodieCompactionConfig.MIN_COMMITS_TO_KEEP_PROP));
      int maxInstantsToKeep = Integer.parseInt(getString(props, HoodieCompactionConfig.MAX_COMMITS_TO_KEEP_PROP));
      int cleanerCommitsRetained =
          Integer.parseInt(getString(props, HoodieCompactionConfig.CLEANER_COMMITS_RETAINED_PROP));
      ValidationUtils.checkArgument(maxInstantsToKeep > minInstantsToKeep,
          String.format(
              "Increase %s=%d to be greater than %s=%d.",
              HoodieCompactionConfig.MAX_COMMITS_TO_KEEP_PROP.key(), maxInstantsToKeep,
              HoodieCompactionConfig.MIN_COMMITS_TO_KEEP_PROP.key(), minInstantsToKeep));
      ValidationUtils.checkArgument(minInstantsToKeep > cleanerCommitsRetained,
          String.format(
              "Increase %s=%d to be greater than %s=%d. Otherwise, there is risk of incremental pull "
                  + "missing data from few instants.",
              HoodieCompactionConfig.MIN_COMMITS_TO_KEEP_PROP.key(), minInstantsToKeep,
              HoodieCompactionConfig.CLEANER_COMMITS_RETAINED_PROP.key(), cleanerCommitsRetained));
      return config;
    }
  }
}
