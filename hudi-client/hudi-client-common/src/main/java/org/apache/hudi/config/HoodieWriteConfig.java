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

import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.bootstrap.BootstrapMode;
import org.apache.hudi.client.transaction.ConflictResolutionStrategy;
import org.apache.hudi.common.config.ConfigOption;
import org.apache.hudi.common.config.DefaultHoodieConfig;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.fs.ConsistencyGuardConfig;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.execution.bulkinsert.BulkInsertSortMode;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.keygen.SimpleAvroKeyGenerator;
import org.apache.hudi.metrics.MetricsReporterType;
import org.apache.hudi.metrics.datadog.DatadogHttpClient.ApiSite;
import org.apache.hudi.table.action.compact.CompactionTriggerStrategy;
import org.apache.hudi.table.action.compact.strategy.CompactionStrategy;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import javax.annotation.concurrent.Immutable;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.hudi.common.config.LockConfiguration.HIVE_DATABASE_NAME_PROP;
import static org.apache.hudi.common.config.LockConfiguration.HIVE_TABLE_NAME_PROP;

/**
 * Class storing configs for the HoodieWriteClient.
 */
@Immutable
public class HoodieWriteConfig extends DefaultHoodieConfig {

  private static final long serialVersionUID = 0L;

  public static final ConfigOption<String> TABLE_NAME = ConfigOption
      .key("hoodie.table.name")
      .noDefaultValue()
      .withDescription("");

  public static final ConfigOption<String> PRECOMBINE_FIELD_PROP = ConfigOption
      .key("hoodie.datasource.write.precombine.field")
      .defaultValue("ts")
      .withDescription("");

  public static final ConfigOption<String> WRITE_PAYLOAD_CLASS = ConfigOption
      .key("hoodie.datasource.write.payload.class")
      .defaultValue(OverwriteWithLatestAvroPayload.class.getName())
      .withDescription("");

  public static final ConfigOption<String> KEYGENERATOR_CLASS_PROP = ConfigOption
      .key("hoodie.datasource.write.keygenerator.class")
      .defaultValue(SimpleAvroKeyGenerator.class.getName())
      .withDescription("");

  public static final ConfigOption<String> ROLLBACK_USING_MARKERS = ConfigOption
      .key("hoodie.rollback.using.markers")
      .defaultValue("false")
      .withDescription("");

  public static final ConfigOption<String> TIMELINE_LAYOUT_VERSION = ConfigOption
      .key("hoodie.timeline.layout.version")
      .noDefaultValue()
      .withDescription("");

  public static final ConfigOption<String> BASE_PATH_PROP = ConfigOption
      .key("hoodie.base.path")
      .noDefaultValue()
      .withDescription("");

  public static final ConfigOption<String> AVRO_SCHEMA = ConfigOption
      .key("hoodie.avro.schema")
      .noDefaultValue()
      .withDescription("");

  public static final ConfigOption<String> AVRO_SCHEMA_VALIDATE = ConfigOption
      .key("hoodie.avro.schema.validate")
      .defaultValue("false")
      .withDescription("");

  public static final ConfigOption<String> INSERT_PARALLELISM = ConfigOption
      .key("hoodie.insert.shuffle.parallelism")
      .defaultValue("1500")
      .withDescription("");

  public static final ConfigOption<String> BULKINSERT_PARALLELISM = ConfigOption
      .key("hoodie.bulkinsert.shuffle.parallelism")
      .defaultValue("1500")
      .withDescription("");

  public static final ConfigOption<String> BULKINSERT_USER_DEFINED_PARTITIONER_CLASS = ConfigOption
      .key("hoodie.bulkinsert.user.defined.partitioner.class")
      .noDefaultValue()
      .withDescription("");

  public static final ConfigOption<String> BULKINSERT_INPUT_DATA_SCHEMA_DDL = ConfigOption
      .key("hoodie.bulkinsert.schema.ddl")
      .noDefaultValue()
      .withDescription("");

  public static final ConfigOption<String> UPSERT_PARALLELISM = ConfigOption
      .key("hoodie.upsert.shuffle.parallelism")
      .defaultValue("1500")
      .withDescription("");

  public static final ConfigOption<String> DELETE_PARALLELISM = ConfigOption
      .key("hoodie.delete.shuffle.parallelism")
      .defaultValue("1500")
      .withDescription("");

  public static final ConfigOption<String> ROLLBACK_PARALLELISM = ConfigOption
      .key("hoodie.rollback.parallelism")
      .defaultValue("100")
      .withDescription("");

  public static final ConfigOption<String> WRITE_BUFFER_LIMIT_BYTES = ConfigOption
      .key("hoodie.write.buffer.limit.bytes")
      .defaultValue(String.valueOf(4 * 1024 * 1024))
      .withDescription("");

  public static final ConfigOption<String> COMBINE_BEFORE_INSERT_PROP = ConfigOption
      .key("hoodie.combine.before.insert")
      .defaultValue("false")
      .withDescription("");

  public static final ConfigOption<String> COMBINE_BEFORE_UPSERT_PROP = ConfigOption
      .key("hoodie.combine.before.upsert")
      .defaultValue("true")
      .withDescription("");

  public static final ConfigOption<String> COMBINE_BEFORE_DELETE_PROP = ConfigOption
      .key("hoodie.combine.before.delete")
      .defaultValue("true")
      .withDescription("");

  public static final ConfigOption<String> WRITE_STATUS_STORAGE_LEVEL = ConfigOption
      .key("hoodie.write.status.storage.level")
      .defaultValue("MEMORY_AND_DISK_SER")
      .withDescription("");

  public static final ConfigOption<String> HOODIE_AUTO_COMMIT_PROP = ConfigOption
      .key("hoodie.auto.commit")
      .defaultValue("true")
      .withDescription("");

  public static final ConfigOption<String> HOODIE_WRITE_STATUS_CLASS_PROP = ConfigOption
      .key("hoodie.writestatus.class")
      .defaultValue(WriteStatus.class.getName())
      .withDescription("");

  public static final ConfigOption<String> FINALIZE_WRITE_PARALLELISM = ConfigOption
      .key("hoodie.finalize.write.parallelism")
      .defaultValue("1500")
      .withDescription("");

  public static final ConfigOption<String> MARKERS_DELETE_PARALLELISM = ConfigOption
      .key("hoodie.markers.delete.parallelism")
      .defaultValue("100")
      .withDescription("");

  public static final ConfigOption<String> BULKINSERT_SORT_MODE = ConfigOption
      .key("hoodie.bulkinsert.sort.mode")
      .defaultValue(BulkInsertSortMode.GLOBAL_SORT.toString())
      .withDescription("");

  public static final ConfigOption<String> EMBEDDED_TIMELINE_SERVER_ENABLED = ConfigOption
      .key("hoodie.embed.timeline.server")
      .defaultValue("true")
      .withDescription("");

  public static final ConfigOption<String> EMBEDDED_TIMELINE_SERVER_PORT = ConfigOption
      .key("hoodie.embed.timeline.server.port")
      .defaultValue("0")
      .withDescription("");

  public static final ConfigOption<String> EMBEDDED_TIMELINE_SERVER_THREADS = ConfigOption
      .key("hoodie.embed.timeline.server.threads")
      .defaultValue("-1")
      .withDescription("");

  public static final ConfigOption<String> EMBEDDED_TIMELINE_SERVER_COMPRESS_OUTPUT = ConfigOption
      .key("hoodie.embed.timeline.server.gzip")
      .defaultValue("true")
      .withDescription("");

  public static final ConfigOption<String> EMBEDDED_TIMELINE_SERVER_USE_ASYNC = ConfigOption
      .key("hoodie.embed.timeline.server.async")
      .defaultValue("false")
      .withDescription("");

  public static final ConfigOption<String> FAIL_ON_TIMELINE_ARCHIVING_ENABLED_PROP = ConfigOption
      .key("hoodie.fail.on.timeline.archiving")
      .defaultValue("true")
      .withDescription("");

  // time between successive attempts to ensure written data's metadata is consistent on storage
  public static final ConfigOption<Long> INITIAL_CONSISTENCY_CHECK_INTERVAL_MS_PROP = ConfigOption
      .key("hoodie.consistency.check.initial_interval_ms")
      .defaultValue(2000L)
      .withDescription("");

  // max interval time
  public static final ConfigOption<Long> MAX_CONSISTENCY_CHECK_INTERVAL_MS_PROP = ConfigOption
      .key("hoodie.consistency.check.max_interval_ms")
      .defaultValue(300000L)
      .withDescription("");

  // maximum number of checks, for consistency of written data. Will wait upto 256 Secs
  public static final ConfigOption<Integer> MAX_CONSISTENCY_CHECKS_PROP = ConfigOption
      .key("hoodie.consistency.check.max_checks")
      .defaultValue(7)
      .withDescription("");

  // Data validation check performed during merges before actual commits
  public static final ConfigOption<String> MERGE_DATA_VALIDATION_CHECK_ENABLED = ConfigOption
      .key("hoodie.merge.data.validation.enabled")
      .defaultValue("false")
      .withDescription("");

  // Allow duplicates with inserts while merging with existing records
  public static final ConfigOption<String> MERGE_ALLOW_DUPLICATE_ON_INSERTS = ConfigOption
      .key("hoodie.merge.allow.duplicate.on.inserts")
      .defaultValue("false")
      .withDescription("");

  public static final ConfigOption<Integer> CLIENT_HEARTBEAT_INTERVAL_IN_MS_PROP = ConfigOption
      .key("hoodie.client.heartbeat.interval_in_ms")
      .defaultValue(60 * 1000)
      .withDescription("");

  public static final ConfigOption<Integer> CLIENT_HEARTBEAT_NUM_TOLERABLE_MISSES_PROP = ConfigOption
      .key("hoodie.client.heartbeat.tolerable.misses")
      .defaultValue(2)
      .withDescription("");

  // Enable different concurrency support
  public static final ConfigOption<String> WRITE_CONCURRENCY_MODE_PROP = ConfigOption
      .key("hoodie.write.concurrency.mode")
      .defaultValue(WriteConcurrencyMode.SINGLE_WRITER.name())
      .withDescription("");

  // Comma separated metadata key prefixes to override from latest commit during overlapping commits via multi writing
  public static final ConfigOption<String> WRITE_META_KEY_PREFIXES_PROP = ConfigOption
      .key("hoodie.write.meta.key.prefixes")
      .defaultValue("")
      .withDescription("");

  /**
   * HUDI-858 : There are users who had been directly using RDD APIs and have relied on a behavior in 0.4.x to allow
   * multiple write operations (upsert/buk-insert/...) to be executed within a single commit.
   * <p>
   * Given Hudi commit protocol, these are generally unsafe operations and user need to handle failure scenarios. It
   * only works with COW table. Hudi 0.5.x had stopped this behavior.
   * <p>
   * Given the importance of supporting such cases for the user's migration to 0.5.x, we are proposing a safety flag
   * (disabled by default) which will allow this old behavior.
   */
  public static final ConfigOption<String> ALLOW_MULTI_WRITE_ON_SAME_INSTANT = ConfigOption
      .key("_.hoodie.allow.multi.write.on.same.instant")
      .defaultValue("false")
      .withDescription("");

  public static final ConfigOption<String> EXTERNAL_RECORD_AND_SCHEMA_TRANSFORMATION = ConfigOption
      .key(AVRO_SCHEMA + ".externalTransformation")
      .defaultValue("false")
      .withDescription("");

  private ConsistencyGuardConfig consistencyGuardConfig;

  // Hoodie Write Client transparently rewrites File System View config when embedded mode is enabled
  // We keep track of original config and rewritten config
  private final FileSystemViewStorageConfig clientSpecifiedViewStorageConfig;
  private FileSystemViewStorageConfig viewStorageConfig;
  private HoodiePayloadConfig hoodiePayloadConfig;
  private HoodieMetadataConfig metadataConfig;

  private EngineType engineType;

  /**
   * Use Spark engine by default.
   */
  protected HoodieWriteConfig(Properties props) {
    this(EngineType.SPARK, props);
  }

  protected HoodieWriteConfig(EngineType engineType, Properties props) {
    super(props);
    Properties newProps = new Properties();
    newProps.putAll(props);
    this.engineType = engineType;
    this.consistencyGuardConfig = ConsistencyGuardConfig.newBuilder().fromProperties(newProps).build();
    this.clientSpecifiedViewStorageConfig = FileSystemViewStorageConfig.newBuilder().fromProperties(newProps).build();
    this.viewStorageConfig = clientSpecifiedViewStorageConfig;
    this.hoodiePayloadConfig = HoodiePayloadConfig.newBuilder().fromProperties(newProps).build();
    this.metadataConfig = HoodieMetadataConfig.newBuilder().fromProperties(props).build();
  }

  public static HoodieWriteConfig.Builder newBuilder() {
    return new Builder();
  }

  /**
   * base properties.
   */
  public String getBasePath() {
    return props.getProperty(BASE_PATH_PROP.key());
  }

  public String getSchema() {
    return props.getProperty(AVRO_SCHEMA.key());
  }

  public void setSchema(String schemaStr) {
    props.setProperty(AVRO_SCHEMA.key(), schemaStr);
  }

  public boolean getAvroSchemaValidate() {
    return Boolean.parseBoolean(props.getProperty(AVRO_SCHEMA_VALIDATE.key()));
  }

  public String getTableName() {
    return props.getProperty(TABLE_NAME.key());
  }

  public String getPreCombineField() {
    return props.getProperty(PRECOMBINE_FIELD_PROP.key());
  }

  public String getWritePayloadClass() {
    return props.getProperty(WRITE_PAYLOAD_CLASS.key());
  }

  public String getKeyGeneratorClass() {
    return props.getProperty(KEYGENERATOR_CLASS_PROP.key());
  }

  public Boolean shouldAutoCommit() {
    return Boolean.parseBoolean(props.getProperty(HOODIE_AUTO_COMMIT_PROP.key()));
  }

  public Boolean shouldAssumeDatePartitioning() {
    return metadataConfig.shouldAssumeDatePartitioning();
  }

  public boolean shouldUseExternalSchemaTransformation() {
    return Boolean.parseBoolean(props.getProperty(EXTERNAL_RECORD_AND_SCHEMA_TRANSFORMATION.key()));
  }

  public Integer getTimelineLayoutVersion() {
    return Integer.parseInt(props.getProperty(TIMELINE_LAYOUT_VERSION.key()));
  }

  public int getBulkInsertShuffleParallelism() {
    return Integer.parseInt(props.getProperty(BULKINSERT_PARALLELISM.key()));
  }

  public String getUserDefinedBulkInsertPartitionerClass() {
    return props.getProperty(BULKINSERT_USER_DEFINED_PARTITIONER_CLASS.key());
  }

  public int getInsertShuffleParallelism() {
    return Integer.parseInt(props.getProperty(INSERT_PARALLELISM.key()));
  }

  public int getUpsertShuffleParallelism() {
    return Integer.parseInt(props.getProperty(UPSERT_PARALLELISM.key()));
  }

  public int getDeleteShuffleParallelism() {
    return Math.max(Integer.parseInt(props.getProperty(DELETE_PARALLELISM.key())), 1);
  }

  public int getRollbackParallelism() {
    return Integer.parseInt(props.getProperty(ROLLBACK_PARALLELISM.key()));
  }

  public int getFileListingParallelism() {
    return metadataConfig.getFileListingParallelism();
  }

  public boolean shouldRollbackUsingMarkers() {
    return Boolean.parseBoolean(props.getProperty(ROLLBACK_USING_MARKERS.key()));
  }

  public int getWriteBufferLimitBytes() {
    return Integer.parseInt(props.getProperty(WRITE_BUFFER_LIMIT_BYTES.key(), WRITE_BUFFER_LIMIT_BYTES.defaultValue()));
  }

  public boolean shouldCombineBeforeInsert() {
    return Boolean.parseBoolean(props.getProperty(COMBINE_BEFORE_INSERT_PROP.key()));
  }

  public boolean shouldCombineBeforeUpsert() {
    return Boolean.parseBoolean(props.getProperty(COMBINE_BEFORE_UPSERT_PROP.key()));
  }

  public boolean shouldCombineBeforeDelete() {
    return Boolean.parseBoolean(props.getProperty(COMBINE_BEFORE_DELETE_PROP.key()));
  }

  public boolean shouldAllowMultiWriteOnSameInstant() {
    return Boolean.parseBoolean(props.getProperty(ALLOW_MULTI_WRITE_ON_SAME_INSTANT.key()));
  }

  public String getWriteStatusClassName() {
    return props.getProperty(HOODIE_WRITE_STATUS_CLASS_PROP.key());
  }

  public int getFinalizeWriteParallelism() {
    return Integer.parseInt(props.getProperty(FINALIZE_WRITE_PARALLELISM.key()));
  }

  public int getMarkersDeleteParallelism() {
    return Integer.parseInt(props.getProperty(MARKERS_DELETE_PARALLELISM.key()));
  }

  public boolean isEmbeddedTimelineServerEnabled() {
    return Boolean.parseBoolean(props.getProperty(EMBEDDED_TIMELINE_SERVER_ENABLED.key()));
  }

  public int getEmbeddedTimelineServerPort() {
    return Integer.parseInt(props.getProperty(EMBEDDED_TIMELINE_SERVER_PORT.key(), EMBEDDED_TIMELINE_SERVER_PORT.defaultValue()));
  }

  public int getEmbeddedTimelineServerThreads() {
    return Integer.parseInt(props.getProperty(EMBEDDED_TIMELINE_SERVER_THREADS.key(), EMBEDDED_TIMELINE_SERVER_THREADS.defaultValue()));
  }

  public boolean getEmbeddedTimelineServerCompressOutput() {
    return Boolean.parseBoolean(props.getProperty(EMBEDDED_TIMELINE_SERVER_COMPRESS_OUTPUT.key(), EMBEDDED_TIMELINE_SERVER_COMPRESS_OUTPUT.defaultValue()));
  }

  public boolean getEmbeddedTimelineServerUseAsync() {
    return Boolean.parseBoolean(props.getProperty(EMBEDDED_TIMELINE_SERVER_USE_ASYNC.key(), EMBEDDED_TIMELINE_SERVER_USE_ASYNC.defaultValue()));
  }

  public boolean isFailOnTimelineArchivingEnabled() {
    return Boolean.parseBoolean(props.getProperty(FAIL_ON_TIMELINE_ARCHIVING_ENABLED_PROP.key()));
  }

  public int getMaxConsistencyChecks() {
    return Integer.parseInt(props.getProperty(MAX_CONSISTENCY_CHECKS_PROP.key()));
  }

  public int getInitialConsistencyCheckIntervalMs() {
    return Integer.parseInt(props.getProperty(INITIAL_CONSISTENCY_CHECK_INTERVAL_MS_PROP.key()));
  }

  public int getMaxConsistencyCheckIntervalMs() {
    return Integer.parseInt(props.getProperty(MAX_CONSISTENCY_CHECK_INTERVAL_MS_PROP.key()));
  }

  public BulkInsertSortMode getBulkInsertSortMode() {
    String sortMode = props.getProperty(BULKINSERT_SORT_MODE.key());
    return BulkInsertSortMode.valueOf(sortMode.toUpperCase());
  }

  public boolean isMergeDataValidationCheckEnabled() {
    return Boolean.parseBoolean(props.getProperty(MERGE_DATA_VALIDATION_CHECK_ENABLED.key()));
  }

  public boolean allowDuplicateInserts() {
    return Boolean.parseBoolean(props.getProperty(MERGE_ALLOW_DUPLICATE_ON_INSERTS.key()));
  }

  public EngineType getEngineType() {
    return engineType;
  }

  /**
   * compaction properties.
   */
  public HoodieCleaningPolicy getCleanerPolicy() {
    return HoodieCleaningPolicy.valueOf(props.getProperty(HoodieCompactionConfig.CLEANER_POLICY_PROP.key()));
  }

  public int getCleanerFileVersionsRetained() {
    return Integer.parseInt(props.getProperty(HoodieCompactionConfig.CLEANER_FILE_VERSIONS_RETAINED_PROP.key()));
  }

  public int getCleanerCommitsRetained() {
    return Integer.parseInt(props.getProperty(HoodieCompactionConfig.CLEANER_COMMITS_RETAINED_PROP.key()));
  }

  public int getMaxCommitsToKeep() {
    return Integer.parseInt(props.getProperty(HoodieCompactionConfig.MAX_COMMITS_TO_KEEP_PROP.key()));
  }

  public int getMinCommitsToKeep() {
    return Integer.parseInt(props.getProperty(HoodieCompactionConfig.MIN_COMMITS_TO_KEEP_PROP.key()));
  }

  public int getParquetSmallFileLimit() {
    return Integer.parseInt(props.getProperty(HoodieCompactionConfig.PARQUET_SMALL_FILE_LIMIT_BYTES.key()));
  }

  public double getRecordSizeEstimationThreshold() {
    return Double.parseDouble(props.getProperty(HoodieCompactionConfig.RECORD_SIZE_ESTIMATION_THRESHOLD_PROP.key()));
  }

  public int getCopyOnWriteInsertSplitSize() {
    return Integer.parseInt(props.getProperty(HoodieCompactionConfig.COPY_ON_WRITE_TABLE_INSERT_SPLIT_SIZE.key()));
  }

  public int getCopyOnWriteRecordSizeEstimate() {
    return Integer.parseInt(props.getProperty(HoodieCompactionConfig.COPY_ON_WRITE_TABLE_RECORD_SIZE_ESTIMATE.key()));
  }

  public boolean shouldAutoTuneInsertSplits() {
    return Boolean.parseBoolean(props.getProperty(HoodieCompactionConfig.COPY_ON_WRITE_TABLE_AUTO_SPLIT_INSERTS.key()));
  }

  public int getCleanerParallelism() {
    return Integer.parseInt(props.getProperty(HoodieCompactionConfig.CLEANER_PARALLELISM.key()));
  }

  public boolean isAutoClean() {
    return Boolean.parseBoolean(props.getProperty(HoodieCompactionConfig.AUTO_CLEAN_PROP.key()));
  }

  public boolean isAsyncClean() {
    return Boolean.parseBoolean(props.getProperty(HoodieCompactionConfig.ASYNC_CLEAN_PROP.key()));
  }

  public boolean incrementalCleanerModeEnabled() {
    return Boolean.parseBoolean(props.getProperty(HoodieCompactionConfig.CLEANER_INCREMENTAL_MODE.key()));
  }

  public boolean inlineCompactionEnabled() {
    return Boolean.parseBoolean(props.getProperty(HoodieCompactionConfig.INLINE_COMPACT_PROP.key()));
  }

  public CompactionTriggerStrategy getInlineCompactTriggerStrategy() {
    return CompactionTriggerStrategy.valueOf(props.getProperty(HoodieCompactionConfig.INLINE_COMPACT_TRIGGER_STRATEGY_PROP.key()));
  }

  public int getInlineCompactDeltaCommitMax() {
    return Integer.parseInt(props.getProperty(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS_PROP.key()));
  }

  public int getInlineCompactDeltaSecondsMax() {
    return Integer.parseInt(props.getProperty(HoodieCompactionConfig.INLINE_COMPACT_TIME_DELTA_SECONDS_PROP.key()));
  }

  public CompactionStrategy getCompactionStrategy() {
    return ReflectionUtils.loadClass(props.getProperty(HoodieCompactionConfig.COMPACTION_STRATEGY_PROP.key()));
  }

  public Long getTargetIOPerCompactionInMB() {
    return Long.parseLong(props.getProperty(HoodieCompactionConfig.TARGET_IO_PER_COMPACTION_IN_MB_PROP.key()));
  }

  public Boolean getCompactionLazyBlockReadEnabled() {
    return Boolean.valueOf(props.getProperty(HoodieCompactionConfig.COMPACTION_LAZY_BLOCK_READ_ENABLED_PROP.key()));
  }

  public Boolean getCompactionReverseLogReadEnabled() {
    return Boolean.valueOf(props.getProperty(HoodieCompactionConfig.COMPACTION_REVERSE_LOG_READ_ENABLED_PROP.key()));
  }

  public boolean inlineClusteringEnabled() {
    return Boolean.parseBoolean(props.getProperty(HoodieClusteringConfig.INLINE_CLUSTERING_PROP.key()));
  }

  public boolean isAsyncClusteringEnabled() {
    return Boolean.parseBoolean(props.getProperty(HoodieClusteringConfig.ASYNC_CLUSTERING_ENABLE_OPT_KEY.key()));
  }

  public boolean isClusteringEnabled() {
    // TODO: future support async clustering
    return inlineClusteringEnabled() || isAsyncClusteringEnabled();
  }

  public int getInlineClusterMaxCommits() {
    return Integer.parseInt(props.getProperty(HoodieClusteringConfig.INLINE_CLUSTERING_MAX_COMMIT_PROP.key()));
  }

  public String getPayloadClass() {
    return props.getProperty(HoodieCompactionConfig.PAYLOAD_CLASS_PROP.key());
  }

  public int getTargetPartitionsPerDayBasedCompaction() {
    return Integer.parseInt(props.getProperty(HoodieCompactionConfig.TARGET_PARTITIONS_PER_DAYBASED_COMPACTION_PROP.key()));
  }

  public int getCommitArchivalBatchSize() {
    return Integer.parseInt(props.getProperty(HoodieCompactionConfig.COMMITS_ARCHIVAL_BATCH_SIZE_PROP.key()));
  }

  public Boolean shouldCleanBootstrapBaseFile() {
    return Boolean.valueOf(props.getProperty(HoodieCompactionConfig.CLEANER_BOOTSTRAP_BASE_FILE_ENABLED.key()));
  }

  public String getClusteringUpdatesStrategyClass() {
    return props.getProperty(HoodieClusteringConfig.CLUSTERING_UPDATES_STRATEGY_PROP.key());
  }

  public HoodieFailedWritesCleaningPolicy getFailedWritesCleanPolicy() {
    return HoodieFailedWritesCleaningPolicy
        .valueOf(props.getProperty(HoodieCompactionConfig.FAILED_WRITES_CLEANER_POLICY_PROP.key()));
  }

  /**
   * Clustering properties.
   */
  public String getClusteringPlanStrategyClass() {
    return props.getProperty(HoodieClusteringConfig.CLUSTERING_PLAN_STRATEGY_CLASS.key());
  }

  public String getClusteringExecutionStrategyClass() {
    return props.getProperty(HoodieClusteringConfig.CLUSTERING_EXECUTION_STRATEGY_CLASS.key());
  }

  public long getClusteringMaxBytesInGroup() {
    return Long.parseLong(props.getProperty(HoodieClusteringConfig.CLUSTERING_MAX_BYTES_PER_GROUP.key()));
  }

  public long getClusteringSmallFileLimit() {
    return Long.parseLong(props.getProperty(HoodieClusteringConfig.CLUSTERING_PLAN_SMALL_FILE_LIMIT.key()));
  }

  public int getClusteringMaxNumGroups() {
    return Integer.parseInt(props.getProperty(HoodieClusteringConfig.CLUSTERING_MAX_NUM_GROUPS.key()));
  }

  public long getClusteringTargetFileMaxBytes() {
    return Long.parseLong(props.getProperty(HoodieClusteringConfig.CLUSTERING_TARGET_FILE_MAX_BYTES.key()));
  }

  public int getTargetPartitionsForClustering() {
    return Integer.parseInt(props.getProperty(HoodieClusteringConfig.CLUSTERING_TARGET_PARTITIONS.key()));
  }

  public String getClusteringSortColumns() {
    return props.getProperty(HoodieClusteringConfig.CLUSTERING_SORT_COLUMNS_PROPERTY.key());
  }

  /**
   * index properties.
   */
  public HoodieIndex.IndexType getIndexType() {
    return HoodieIndex.IndexType.valueOf(props.getProperty(HoodieIndexConfig.INDEX_TYPE_PROP.key()));
  }

  public String getIndexClass() {
    return props.getProperty(HoodieIndexConfig.INDEX_CLASS_PROP.key());
  }

  public int getBloomFilterNumEntries() {
    return Integer.parseInt(props.getProperty(HoodieIndexConfig.BLOOM_FILTER_NUM_ENTRIES.key()));
  }

  public double getBloomFilterFPP() {
    return Double.parseDouble(props.getProperty(HoodieIndexConfig.BLOOM_FILTER_FPP.key()));
  }

  public String getHbaseZkQuorum() {
    return props.getProperty(HoodieHBaseIndexConfig.HBASE_ZKQUORUM_PROP.key());
  }

  public int getHbaseZkPort() {
    return Integer.parseInt(props.getProperty(HoodieHBaseIndexConfig.HBASE_ZKPORT_PROP.key()));
  }

  public String getHBaseZkZnodeParent() {
    return props.getProperty(HoodieHBaseIndexConfig.HBASE_ZK_ZNODEPARENT.key());
  }

  public String getHbaseTableName() {
    return props.getProperty(HoodieHBaseIndexConfig.HBASE_TABLENAME_PROP.key());
  }

  public int getHbaseIndexGetBatchSize() {
    return Integer.parseInt(props.getProperty(HoodieHBaseIndexConfig.HBASE_GET_BATCH_SIZE_PROP.key()));
  }

  public Boolean getHBaseIndexRollbackSync() {
    return Boolean.parseBoolean(props.getProperty(HoodieHBaseIndexConfig.HBASE_INDEX_ROLLBACK_SYNC.key()));
  }

  public int getHbaseIndexPutBatchSize() {
    return Integer.parseInt(props.getProperty(HoodieHBaseIndexConfig.HBASE_PUT_BATCH_SIZE_PROP.key()));
  }

  public Boolean getHbaseIndexPutBatchSizeAutoCompute() {
    return Boolean.valueOf(props.getProperty(HoodieHBaseIndexConfig.HBASE_PUT_BATCH_SIZE_AUTO_COMPUTE_PROP.key()));
  }

  public String getHBaseQPSResourceAllocatorClass() {
    return props.getProperty(HoodieHBaseIndexConfig.HBASE_INDEX_QPS_ALLOCATOR_CLASS.key());
  }

  public String getHBaseQPSZKnodePath() {
    return props.getProperty(HoodieHBaseIndexConfig.HBASE_ZK_PATH_QPS_ROOT.key());
  }

  public String getHBaseZkZnodeSessionTimeout() {
    return props.getProperty(HoodieHBaseIndexConfig.HOODIE_INDEX_HBASE_ZK_SESSION_TIMEOUT_MS.key());
  }

  public String getHBaseZkZnodeConnectionTimeout() {
    return props.getProperty(HoodieHBaseIndexConfig.HOODIE_INDEX_HBASE_ZK_CONNECTION_TIMEOUT_MS.key());
  }

  public boolean getHBaseIndexShouldComputeQPSDynamically() {
    return Boolean.parseBoolean(props.getProperty(HoodieHBaseIndexConfig.HOODIE_INDEX_COMPUTE_QPS_DYNAMICALLY.key()));
  }

  public int getHBaseIndexDesiredPutsTime() {
    return Integer.parseInt(props.getProperty(HoodieHBaseIndexConfig.HOODIE_INDEX_DESIRED_PUTS_TIME_IN_SECS.key()));
  }

  public String getBloomFilterType() {
    return props.getProperty(HoodieIndexConfig.BLOOM_INDEX_FILTER_TYPE.key());
  }

  public int getDynamicBloomFilterMaxNumEntries() {
    return Integer.parseInt(props.getProperty(HoodieIndexConfig.HOODIE_BLOOM_INDEX_FILTER_DYNAMIC_MAX_ENTRIES.key()));
  }

  /**
   * Fraction of the global share of QPS that should be allocated to this job. Let's say there are 3 jobs which have
   * input size in terms of number of rows required for HbaseIndexing as x, 2x, 3x respectively. Then this fraction for
   * the jobs would be (0.17) 1/6, 0.33 (2/6) and 0.5 (3/6) respectively.
   */
  public float getHbaseIndexQPSFraction() {
    return Float.parseFloat(props.getProperty(HoodieHBaseIndexConfig.HBASE_QPS_FRACTION_PROP.key()));
  }

  public float getHBaseIndexMinQPSFraction() {
    return Float.parseFloat(props.getProperty(HoodieHBaseIndexConfig.HBASE_MIN_QPS_FRACTION_PROP.key()));
  }

  public float getHBaseIndexMaxQPSFraction() {
    return Float.parseFloat(props.getProperty(HoodieHBaseIndexConfig.HBASE_MAX_QPS_FRACTION_PROP.key()));
  }

  /**
   * This should be same across various jobs. This is intended to limit the aggregate QPS generated across various
   * Hoodie jobs to an Hbase Region Server
   */
  public int getHbaseIndexMaxQPSPerRegionServer() {
    return Integer.parseInt(props.getProperty(HoodieHBaseIndexConfig.HBASE_MAX_QPS_PER_REGION_SERVER_PROP.key()));
  }

  public boolean getHbaseIndexUpdatePartitionPath() {
    return Boolean.parseBoolean(props.getProperty(HoodieHBaseIndexConfig.HBASE_INDEX_UPDATE_PARTITION_PATH.key()));
  }

  public int getBloomIndexParallelism() {
    return Integer.parseInt(props.getProperty(HoodieIndexConfig.BLOOM_INDEX_PARALLELISM_PROP.key()));
  }

  public boolean getBloomIndexPruneByRanges() {
    return Boolean.parseBoolean(props.getProperty(HoodieIndexConfig.BLOOM_INDEX_PRUNE_BY_RANGES_PROP.key()));
  }

  public boolean getBloomIndexUseCaching() {
    return Boolean.parseBoolean(props.getProperty(HoodieIndexConfig.BLOOM_INDEX_USE_CACHING_PROP.key()));
  }

  public boolean useBloomIndexTreebasedFilter() {
    return Boolean.parseBoolean(props.getProperty(HoodieIndexConfig.BLOOM_INDEX_TREE_BASED_FILTER_PROP.key()));
  }

  public boolean useBloomIndexBucketizedChecking() {
    return Boolean.parseBoolean(props.getProperty(HoodieIndexConfig.BLOOM_INDEX_BUCKETIZED_CHECKING_PROP.key()));
  }

  public int getBloomIndexKeysPerBucket() {
    return Integer.parseInt(props.getProperty(HoodieIndexConfig.BLOOM_INDEX_KEYS_PER_BUCKET_PROP.key()));
  }

  public boolean getBloomIndexUpdatePartitionPath() {
    return Boolean.parseBoolean(props.getProperty(HoodieIndexConfig.BLOOM_INDEX_UPDATE_PARTITION_PATH.key()));
  }

  public int getSimpleIndexParallelism() {
    return Integer.parseInt(props.getProperty(HoodieIndexConfig.SIMPLE_INDEX_PARALLELISM_PROP.key()));
  }

  public boolean getSimpleIndexUseCaching() {
    return Boolean.parseBoolean(props.getProperty(HoodieIndexConfig.SIMPLE_INDEX_USE_CACHING_PROP.key()));
  }

  public int getGlobalSimpleIndexParallelism() {
    return Integer.parseInt(props.getProperty(HoodieIndexConfig.GLOBAL_SIMPLE_INDEX_PARALLELISM_PROP.key()));
  }

  public boolean getGlobalSimpleIndexUpdatePartitionPath() {
    return Boolean.parseBoolean(props.getProperty(HoodieIndexConfig.SIMPLE_INDEX_UPDATE_PARTITION_PATH.key()));
  }

  /**
   * storage properties.
   */
  public long getParquetMaxFileSize() {
    return Long.parseLong(props.getProperty(HoodieStorageConfig.PARQUET_FILE_MAX_BYTES.key()));
  }

  public int getParquetBlockSize() {
    return Integer.parseInt(props.getProperty(HoodieStorageConfig.PARQUET_BLOCK_SIZE_BYTES.key()));
  }

  public int getParquetPageSize() {
    return Integer.parseInt(props.getProperty(HoodieStorageConfig.PARQUET_PAGE_SIZE_BYTES.key()));
  }

  public int getLogFileDataBlockMaxSize() {
    return Integer.parseInt(props.getProperty(HoodieStorageConfig.LOGFILE_DATA_BLOCK_SIZE_MAX_BYTES.key()));
  }

  public int getLogFileMaxSize() {
    return Integer.parseInt(props.getProperty(HoodieStorageConfig.LOGFILE_SIZE_MAX_BYTES.key()));
  }

  public double getParquetCompressionRatio() {
    return Double.parseDouble(props.getProperty(HoodieStorageConfig.PARQUET_COMPRESSION_RATIO.key()));
  }

  public CompressionCodecName getParquetCompressionCodec() {
    return CompressionCodecName.fromConf(props.getProperty(HoodieStorageConfig.PARQUET_COMPRESSION_CODEC.key()));
  }

  public double getLogFileToParquetCompressionRatio() {
    return Double.parseDouble(props.getProperty(HoodieStorageConfig.LOGFILE_TO_PARQUET_COMPRESSION_RATIO.key()));
  }

  public long getHFileMaxFileSize() {
    return Long.parseLong(props.getProperty(HoodieStorageConfig.HFILE_FILE_MAX_BYTES.key()));
  }

  public int getHFileBlockSize() {
    return Integer.parseInt(props.getProperty(HoodieStorageConfig.HFILE_BLOCK_SIZE_BYTES.key()));
  }

  public Compression.Algorithm getHFileCompressionAlgorithm() {
    return Compression.Algorithm.valueOf(props.getProperty(HoodieStorageConfig.HFILE_COMPRESSION_ALGORITHM.key()));
  }

  /**
   * metrics properties.
   */
  public boolean isMetricsOn() {
    return Boolean.parseBoolean(props.getProperty(HoodieMetricsConfig.METRICS_ON.key()));
  }

  public boolean isExecutorMetricsEnabled() {
    return Boolean.parseBoolean(props.getProperty(HoodieMetricsConfig.ENABLE_EXECUTOR_METRICS.key(), "false"));
  }

  public MetricsReporterType getMetricsReporterType() {
    return MetricsReporterType.valueOf(props.getProperty(HoodieMetricsConfig.METRICS_REPORTER_TYPE.key()));
  }

  public String getGraphiteServerHost() {
    return props.getProperty(HoodieMetricsConfig.GRAPHITE_SERVER_HOST.key());
  }

  public int getGraphiteServerPort() {
    return Integer.parseInt(props.getProperty(HoodieMetricsConfig.GRAPHITE_SERVER_PORT.key()));
  }

  public String getGraphiteMetricPrefix() {
    return props.getProperty(HoodieMetricsConfig.GRAPHITE_METRIC_PREFIX.key());
  }

  public String getJmxHost() {
    return props.getProperty(HoodieMetricsConfig.JMX_HOST.key());
  }

  public String getJmxPort() {
    return props.getProperty(HoodieMetricsConfig.JMX_PORT.key());
  }

  public int getDatadogReportPeriodSeconds() {
    return Integer.parseInt(props.getProperty(HoodieMetricsDatadogConfig.DATADOG_REPORT_PERIOD_SECONDS.key()));
  }

  public ApiSite getDatadogApiSite() {
    return ApiSite.valueOf(props.getProperty(HoodieMetricsDatadogConfig.DATADOG_API_SITE.key()));
  }

  public String getDatadogApiKey() {
    if (props.containsKey(HoodieMetricsDatadogConfig.DATADOG_API_KEY.key())) {
      return props.getProperty(HoodieMetricsDatadogConfig.DATADOG_API_KEY.key());
    } else {
      Supplier<String> apiKeySupplier = ReflectionUtils.loadClass(
          props.getProperty(HoodieMetricsDatadogConfig.DATADOG_API_KEY_SUPPLIER.key()));
      return apiKeySupplier.get();
    }
  }

  public boolean getDatadogApiKeySkipValidation() {
    return Boolean.parseBoolean(props.getProperty(HoodieMetricsDatadogConfig.DATADOG_API_KEY_SKIP_VALIDATION.key()));
  }

  public int getDatadogApiTimeoutSeconds() {
    return Integer.parseInt(props.getProperty(HoodieMetricsDatadogConfig.DATADOG_API_TIMEOUT_SECONDS.key()));
  }

  public String getDatadogMetricPrefix() {
    return props.getProperty(HoodieMetricsDatadogConfig.DATADOG_METRIC_PREFIX.key());
  }

  public String getDatadogMetricHost() {
    return props.getProperty(HoodieMetricsDatadogConfig.DATADOG_METRIC_HOST.key());
  }

  public List<String> getDatadogMetricTags() {
    return Arrays.stream(props.getProperty(
        HoodieMetricsDatadogConfig.DATADOG_METRIC_TAGS.key(), ",").split("\\s*,\\s*")).collect(Collectors.toList());
  }

  public String getMetricReporterClassName() {
    return props.getProperty(HoodieMetricsConfig.METRICS_REPORTER_CLASS.key());
  }

  public int getPrometheusPort() {
    return Integer.parseInt(props.getProperty(HoodieMetricsPrometheusConfig.PROMETHEUS_PORT.key()));
  }

  public String getPushGatewayHost() {
    return props.getProperty(HoodieMetricsPrometheusConfig.PUSHGATEWAY_HOST.key());
  }

  public int getPushGatewayPort() {
    return Integer.parseInt(props.getProperty(HoodieMetricsPrometheusConfig.PUSHGATEWAY_PORT.key()));
  }

  public int getPushGatewayReportPeriodSeconds() {
    return Integer.parseInt(props.getProperty(HoodieMetricsPrometheusConfig.PUSHGATEWAY_REPORT_PERIOD_SECONDS.key()));
  }

  public boolean getPushGatewayDeleteOnShutdown() {
    return Boolean.parseBoolean(props.getProperty(HoodieMetricsPrometheusConfig.PUSHGATEWAY_DELETE_ON_SHUTDOWN.key()));
  }

  public String getPushGatewayJobName() {
    return props.getProperty(HoodieMetricsPrometheusConfig.PUSHGATEWAY_JOB_NAME.key());
  }

  public boolean getPushGatewayRandomJobNameSuffix() {
    return Boolean.parseBoolean(props.getProperty(HoodieMetricsPrometheusConfig.PUSHGATEWAY_RANDOM_JOB_NAME_SUFFIX.key()));
  }

  /**
   * memory configs.
   */
  public int getMaxDFSStreamBufferSize() {
    return Integer.parseInt(props.getProperty(HoodieMemoryConfig.MAX_DFS_STREAM_BUFFER_SIZE_PROP.key()));
  }

  public String getSpillableMapBasePath() {
    return props.getProperty(HoodieMemoryConfig.SPILLABLE_MAP_BASE_PATH_PROP.key());
  }

  public double getWriteStatusFailureFraction() {
    return Double.parseDouble(props.getProperty(HoodieMemoryConfig.WRITESTATUS_FAILURE_FRACTION_PROP.key()));
  }

  public ConsistencyGuardConfig getConsistencyGuardConfig() {
    return consistencyGuardConfig;
  }

  public void setConsistencyGuardConfig(ConsistencyGuardConfig consistencyGuardConfig) {
    this.consistencyGuardConfig = consistencyGuardConfig;
  }

  public FileSystemViewStorageConfig getViewStorageConfig() {
    return viewStorageConfig;
  }

  public void setViewStorageConfig(FileSystemViewStorageConfig viewStorageConfig) {
    this.viewStorageConfig = viewStorageConfig;
  }

  public void resetViewStorageConfig() {
    this.setViewStorageConfig(getClientSpecifiedViewStorageConfig());
  }

  public FileSystemViewStorageConfig getClientSpecifiedViewStorageConfig() {
    return clientSpecifiedViewStorageConfig;
  }

  public HoodiePayloadConfig getPayloadConfig() {
    return hoodiePayloadConfig;
  }

  public HoodieMetadataConfig getMetadataConfig() {
    return metadataConfig;
  }

  /**
   * Commit call back configs.
   */
  public boolean writeCommitCallbackOn() {
    return Boolean.parseBoolean(props.getProperty(HoodieWriteCommitCallbackConfig.CALLBACK_ON.key()));
  }

  public String getCallbackClass() {
    return props.getProperty(HoodieWriteCommitCallbackConfig.CALLBACK_CLASS_PROP.key());
  }

  public String getBootstrapSourceBasePath() {
    return props.getProperty(HoodieBootstrapConfig.BOOTSTRAP_BASE_PATH_PROP.key());
  }

  public String getBootstrapModeSelectorClass() {
    return props.getProperty(HoodieBootstrapConfig.BOOTSTRAP_MODE_SELECTOR.key());
  }

  public String getFullBootstrapInputProvider() {
    return props.getProperty(HoodieBootstrapConfig.FULL_BOOTSTRAP_INPUT_PROVIDER.key());
  }

  public String getBootstrapKeyGeneratorClass() {
    return props.getProperty(HoodieBootstrapConfig.BOOTSTRAP_KEYGEN_CLASS.key());
  }

  public String getBootstrapModeSelectorRegex() {
    return props.getProperty(HoodieBootstrapConfig.BOOTSTRAP_MODE_SELECTOR_REGEX.key());
  }

  public BootstrapMode getBootstrapModeForRegexMatch() {
    return BootstrapMode.valueOf(props.getProperty(HoodieBootstrapConfig.BOOTSTRAP_MODE_SELECTOR_REGEX_MODE.key()));
  }

  public String getBootstrapPartitionPathTranslatorClass() {
    return props.getProperty(HoodieBootstrapConfig.BOOTSTRAP_PARTITION_PATH_TRANSLATOR_CLASS.key());
  }

  public int getBootstrapParallelism() {
    return Integer.parseInt(props.getProperty(HoodieBootstrapConfig.BOOTSTRAP_PARALLELISM.key()));
  }

  public Long getMaxMemoryPerPartitionMerge() {
    return Long.valueOf(props.getProperty(HoodieMemoryConfig.MAX_MEMORY_FOR_MERGE_PROP.key()));
  }

  public Long getHoodieClientHeartbeatIntervalInMs() {
    return Long.valueOf(props.getProperty(CLIENT_HEARTBEAT_INTERVAL_IN_MS_PROP.key()));
  }

  public Integer getHoodieClientHeartbeatTolerableMisses() {
    return Integer.valueOf(props.getProperty(CLIENT_HEARTBEAT_NUM_TOLERABLE_MISSES_PROP.key()));
  }

  /**
   * File listing metadata configs.
   */
  public boolean useFileListingMetadata() {
    return metadataConfig.useFileListingMetadata();
  }

  public boolean getFileListingMetadataVerify() {
    return metadataConfig.validateFileListingMetadata();
  }

  public int getMetadataInsertParallelism() {
    return Integer.parseInt(props.getProperty(HoodieMetadataConfig.METADATA_INSERT_PARALLELISM_PROP.key()));
  }

  public int getMetadataCompactDeltaCommitMax() {
    return Integer.parseInt(props.getProperty(HoodieMetadataConfig.METADATA_COMPACT_NUM_DELTA_COMMITS_PROP.key()));
  }

  public boolean isMetadataAsyncClean() {
    return Boolean.parseBoolean(props.getProperty(HoodieMetadataConfig.METADATA_ASYNC_CLEAN_PROP.key()));
  }

  public int getMetadataMaxCommitsToKeep() {
    return Integer.parseInt(props.getProperty(HoodieMetadataConfig.MAX_COMMITS_TO_KEEP_PROP.key()));
  }

  public int getMetadataMinCommitsToKeep() {
    return Integer.parseInt(props.getProperty(HoodieMetadataConfig.MIN_COMMITS_TO_KEEP_PROP.key()));
  }

  public int getMetadataCleanerCommitsRetained() {
    return Integer.parseInt(props.getProperty(HoodieMetadataConfig.CLEANER_COMMITS_RETAINED_PROP.key()));
  }

  /**
   * Hoodie Client Lock Configs.
   * @return
   */

  public String getLockProviderClass() {
    return props.getProperty(HoodieLockConfig.LOCK_PROVIDER_CLASS_PROP.key());
  }

  public String getLockHiveDatabaseName() {
    return props.getProperty(HIVE_DATABASE_NAME_PROP.key());
  }

  public String getLockHiveTableName() {
    return props.getProperty(HIVE_TABLE_NAME_PROP.key());
  }

  public ConflictResolutionStrategy getWriteConflictResolutionStrategy() {
    return ReflectionUtils.loadClass(props.getProperty(HoodieLockConfig.WRITE_CONFLICT_RESOLUTION_STRATEGY_CLASS_PROP.key()));
  }

  public Long getLockAcquireWaitTimeoutInMs() {
    return Long.valueOf(props.getProperty(LockConfiguration.LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP.key()));
  }

  public WriteConcurrencyMode getWriteConcurrencyMode() {
    return WriteConcurrencyMode.fromValue(props.getProperty(WRITE_CONCURRENCY_MODE_PROP.key()));
  }

  public Boolean inlineTableServices() {
    return inlineClusteringEnabled() || inlineCompactionEnabled() || isAutoClean();
  }

  public String getWriteMetaKeyPrefixes() {
    return props.getProperty(WRITE_META_KEY_PREFIXES_PROP.key());
  }

  public static class Builder {

    protected final Properties props = new Properties();
    protected EngineType engineType = EngineType.SPARK;
    private boolean isIndexConfigSet = false;
    private boolean isStorageConfigSet = false;
    private boolean isCompactionConfigSet = false;
    private boolean isClusteringConfigSet = false;
    private boolean isMetricsConfigSet = false;
    private boolean isBootstrapConfigSet = false;
    private boolean isMemoryConfigSet = false;
    private boolean isViewConfigSet = false;
    private boolean isConsistencyGuardSet = false;
    private boolean isCallbackConfigSet = false;
    private boolean isPayloadConfigSet = false;
    private boolean isMetadataConfigSet = false;
    private boolean isLockConfigSet = false;

    public Builder withEngineType(EngineType engineType) {
      this.engineType = engineType;
      return this;
    }

    public Builder fromFile(File propertiesFile) throws IOException {
      try (FileReader reader = new FileReader(propertiesFile)) {
        this.props.load(reader);
        return this;
      }
    }

    public Builder fromInputStream(InputStream inputStream) throws IOException {
      try {
        this.props.load(inputStream);
        return this;
      } finally {
        inputStream.close();
      }
    }

    public Builder withProps(Map kvprops) {
      props.putAll(kvprops);
      return this;
    }

    public Builder withPath(String basePath) {
      props.setProperty(BASE_PATH_PROP.key(), basePath);
      return this;
    }

    public Builder withSchema(String schemaStr) {
      props.setProperty(AVRO_SCHEMA.key(), schemaStr);
      return this;
    }

    public Builder withAvroSchemaValidate(boolean enable) {
      props.setProperty(AVRO_SCHEMA_VALIDATE.key(), String.valueOf(enable));
      return this;
    }

    public Builder forTable(String tableName) {
      props.setProperty(TABLE_NAME.key(), tableName);
      return this;
    }

    public Builder withPreCombineField(String preCombineField) {
      props.setProperty(PRECOMBINE_FIELD_PROP.key(), preCombineField);
      return this;
    }

    public Builder withWritePayLoad(String payload) {
      props.setProperty(WRITE_PAYLOAD_CLASS.key(), payload);
      return this;
    }

    public Builder withKeyGenerator(String keyGeneratorClass) {
      props.setProperty(KEYGENERATOR_CLASS_PROP.key(), keyGeneratorClass);
      return this;
    }

    public Builder withTimelineLayoutVersion(int version) {
      props.setProperty(TIMELINE_LAYOUT_VERSION.key(), String.valueOf(version));
      return this;
    }

    public Builder withBulkInsertParallelism(int bulkInsertParallelism) {
      props.setProperty(BULKINSERT_PARALLELISM.key(), String.valueOf(bulkInsertParallelism));
      return this;
    }

    public Builder withUserDefinedBulkInsertPartitionerClass(String className) {
      props.setProperty(BULKINSERT_USER_DEFINED_PARTITIONER_CLASS.key(), className);
      return this;
    }

    public Builder withDeleteParallelism(int parallelism) {
      props.setProperty(DELETE_PARALLELISM.key(), String.valueOf(parallelism));
      return this;
    }

    public Builder withParallelism(int insertShuffleParallelism, int upsertShuffleParallelism) {
      props.setProperty(INSERT_PARALLELISM.key(), String.valueOf(insertShuffleParallelism));
      props.setProperty(UPSERT_PARALLELISM.key(), String.valueOf(upsertShuffleParallelism));
      return this;
    }

    public Builder withRollbackParallelism(int rollbackParallelism) {
      props.setProperty(ROLLBACK_PARALLELISM.key(), String.valueOf(rollbackParallelism));
      return this;
    }

    public Builder withRollbackUsingMarkers(boolean rollbackUsingMarkers) {
      props.setProperty(ROLLBACK_USING_MARKERS.key(), String.valueOf(rollbackUsingMarkers));
      return this;
    }

    public Builder withWriteBufferLimitBytes(int writeBufferLimit) {
      props.setProperty(WRITE_BUFFER_LIMIT_BYTES.key(), String.valueOf(writeBufferLimit));
      return this;
    }

    public Builder combineInput(boolean onInsert, boolean onUpsert) {
      props.setProperty(COMBINE_BEFORE_INSERT_PROP.key(), String.valueOf(onInsert));
      props.setProperty(COMBINE_BEFORE_UPSERT_PROP.key(), String.valueOf(onUpsert));
      return this;
    }

    public Builder combineDeleteInput(boolean onDelete) {
      props.setProperty(COMBINE_BEFORE_DELETE_PROP.key(), String.valueOf(onDelete));
      return this;
    }

    public Builder withWriteStatusStorageLevel(String level) {
      props.setProperty(WRITE_STATUS_STORAGE_LEVEL.key(), level);
      return this;
    }

    public Builder withIndexConfig(HoodieIndexConfig indexConfig) {
      props.putAll(indexConfig.getProps());
      isIndexConfigSet = true;
      return this;
    }

    public Builder withStorageConfig(HoodieStorageConfig storageConfig) {
      props.putAll(storageConfig.getProps());
      isStorageConfigSet = true;
      return this;
    }

    public Builder withCompactionConfig(HoodieCompactionConfig compactionConfig) {
      props.putAll(compactionConfig.getProps());
      isCompactionConfigSet = true;
      return this;
    }

    public Builder withClusteringConfig(HoodieClusteringConfig clusteringConfig) {
      props.putAll(clusteringConfig.getProps());
      isClusteringConfigSet = true;
      return this;
    }

    public Builder withLockConfig(HoodieLockConfig lockConfig) {
      props.putAll(lockConfig.getProps());
      isLockConfigSet = true;
      return this;
    }

    public Builder withMetricsConfig(HoodieMetricsConfig metricsConfig) {
      props.putAll(metricsConfig.getProps());
      isMetricsConfigSet = true;
      return this;
    }

    public Builder withMemoryConfig(HoodieMemoryConfig memoryConfig) {
      props.putAll(memoryConfig.getProps());
      isMemoryConfigSet = true;
      return this;
    }

    public Builder withBootstrapConfig(HoodieBootstrapConfig bootstrapConfig) {
      props.putAll(bootstrapConfig.getProps());
      isBootstrapConfigSet = true;
      return this;
    }

    public Builder withPayloadConfig(HoodiePayloadConfig payloadConfig) {
      props.putAll(payloadConfig.getProps());
      isPayloadConfigSet = true;
      return this;
    }

    public Builder withMetadataConfig(HoodieMetadataConfig metadataConfig) {
      props.putAll(metadataConfig.getProps());
      isMetadataConfigSet = true;
      return this;
    }

    public Builder withAutoCommit(boolean autoCommit) {
      props.setProperty(HOODIE_AUTO_COMMIT_PROP.key(), String.valueOf(autoCommit));
      return this;
    }

    public Builder withWriteStatusClass(Class<? extends WriteStatus> writeStatusClass) {
      props.setProperty(HOODIE_WRITE_STATUS_CLASS_PROP.key(), writeStatusClass.getName());
      return this;
    }

    public Builder withFileSystemViewConfig(FileSystemViewStorageConfig viewStorageConfig) {
      props.putAll(viewStorageConfig.getProps());
      isViewConfigSet = true;
      return this;
    }

    public Builder withConsistencyGuardConfig(ConsistencyGuardConfig consistencyGuardConfig) {
      props.putAll(consistencyGuardConfig.getProps());
      isConsistencyGuardSet = true;
      return this;
    }

    public Builder withCallbackConfig(HoodieWriteCommitCallbackConfig callbackConfig) {
      props.putAll(callbackConfig.getProps());
      isCallbackConfigSet = true;
      return this;
    }

    public Builder withFinalizeWriteParallelism(int parallelism) {
      props.setProperty(FINALIZE_WRITE_PARALLELISM.key(), String.valueOf(parallelism));
      return this;
    }

    public Builder withMarkersDeleteParallelism(int parallelism) {
      props.setProperty(MARKERS_DELETE_PARALLELISM.key(), String.valueOf(parallelism));
      return this;
    }

    public Builder withEmbeddedTimelineServerEnabled(boolean enabled) {
      props.setProperty(EMBEDDED_TIMELINE_SERVER_ENABLED.key(), String.valueOf(enabled));
      return this;
    }

    public Builder withEmbeddedTimelineServerPort(int port) {
      props.setProperty(EMBEDDED_TIMELINE_SERVER_PORT.key(), String.valueOf(port));
      return this;
    }

    public Builder withBulkInsertSortMode(String mode) {
      props.setProperty(BULKINSERT_SORT_MODE.key(), mode);
      return this;
    }

    public Builder withAllowMultiWriteOnSameInstant(boolean allow) {
      props.setProperty(ALLOW_MULTI_WRITE_ON_SAME_INSTANT.key(), String.valueOf(allow));
      return this;
    }

    public Builder withExternalSchemaTrasformation(boolean enabled) {
      props.setProperty(EXTERNAL_RECORD_AND_SCHEMA_TRANSFORMATION.key(), String.valueOf(enabled));
      return this;
    }

    public Builder withMergeDataValidationCheckEnabled(boolean enabled) {
      props.setProperty(MERGE_DATA_VALIDATION_CHECK_ENABLED.key(), String.valueOf(enabled));
      return this;
    }

    public Builder withMergeAllowDuplicateOnInserts(boolean routeInsertsToNewFiles) {
      props.setProperty(MERGE_ALLOW_DUPLICATE_ON_INSERTS.key(), String.valueOf(routeInsertsToNewFiles));
      return this;
    }

    public Builder withHeartbeatIntervalInMs(Integer heartbeatIntervalInMs) {
      props.setProperty(CLIENT_HEARTBEAT_INTERVAL_IN_MS_PROP.key(), String.valueOf(heartbeatIntervalInMs));
      return this;
    }

    public Builder withHeartbeatTolerableMisses(Integer heartbeatTolerableMisses) {
      props.setProperty(CLIENT_HEARTBEAT_NUM_TOLERABLE_MISSES_PROP.key(), String.valueOf(heartbeatTolerableMisses));
      return this;
    }

    public Builder withWriteConcurrencyMode(WriteConcurrencyMode concurrencyMode) {
      props.setProperty(WRITE_CONCURRENCY_MODE_PROP.key(), concurrencyMode.value());
      return this;
    }

    public Builder withWriteMetaKeyPrefixes(String writeMetaKeyPrefixes) {
      props.setProperty(WRITE_META_KEY_PREFIXES_PROP.key(), writeMetaKeyPrefixes);
      return this;
    }

    public Builder withProperties(Properties properties) {
      this.props.putAll(properties);
      return this;
    }

    protected void setDefaults() {
      // Check for mandatory properties
      setDefaultValue(props, INSERT_PARALLELISM);
      setDefaultValue(props, BULKINSERT_PARALLELISM);
      setDefaultValue(props, UPSERT_PARALLELISM);
      setDefaultValue(props, DELETE_PARALLELISM);

      setDefaultValue(props, ROLLBACK_PARALLELISM);
      setDefaultValue(props, KEYGENERATOR_CLASS_PROP);
      setDefaultValue(props, WRITE_PAYLOAD_CLASS);
      setDefaultValue(props, ROLLBACK_USING_MARKERS);
      setDefaultValue(props, COMBINE_BEFORE_INSERT_PROP);
      setDefaultValue(props, COMBINE_BEFORE_UPSERT_PROP);
      setDefaultValue(props, COMBINE_BEFORE_DELETE_PROP);
      setDefaultValue(props, ALLOW_MULTI_WRITE_ON_SAME_INSTANT);
      setDefaultValue(props, WRITE_STATUS_STORAGE_LEVEL);
      setDefaultValue(props, HOODIE_AUTO_COMMIT_PROP);
      setDefaultValue(props, HOODIE_WRITE_STATUS_CLASS_PROP);
      setDefaultValue(props, FINALIZE_WRITE_PARALLELISM);
      setDefaultValue(props, MARKERS_DELETE_PARALLELISM);
      setDefaultValue(props, EMBEDDED_TIMELINE_SERVER_ENABLED);
      setDefaultValue(props, INITIAL_CONSISTENCY_CHECK_INTERVAL_MS_PROP);
      setDefaultValue(props, MAX_CONSISTENCY_CHECK_INTERVAL_MS_PROP);
      setDefaultValue(props, MAX_CONSISTENCY_CHECKS_PROP);
      setDefaultValue(props, FAIL_ON_TIMELINE_ARCHIVING_ENABLED_PROP);
      setDefaultValue(props, AVRO_SCHEMA_VALIDATE);
      setDefaultValue(props, BULKINSERT_SORT_MODE);
      setDefaultValue(props, MERGE_DATA_VALIDATION_CHECK_ENABLED);
      setDefaultValue(props, MERGE_ALLOW_DUPLICATE_ON_INSERTS);
      setDefaultValue(props, CLIENT_HEARTBEAT_INTERVAL_IN_MS_PROP);
      setDefaultValue(props, CLIENT_HEARTBEAT_NUM_TOLERABLE_MISSES_PROP);
      setDefaultValue(props, WRITE_CONCURRENCY_MODE_PROP);
      setDefaultValue(props, WRITE_META_KEY_PREFIXES_PROP);
      // Make sure the props is propagated
      setDefaultOnCondition(props, !isIndexConfigSet, HoodieIndexConfig.newBuilder().withEngineType(engineType).fromProperties(props).build());
      setDefaultOnCondition(props, !isStorageConfigSet, HoodieStorageConfig.newBuilder().fromProperties(props).build());
      setDefaultOnCondition(props, !isCompactionConfigSet,
          HoodieCompactionConfig.newBuilder().fromProperties(props).build());
      setDefaultOnCondition(props, !isClusteringConfigSet,
          HoodieClusteringConfig.newBuilder().fromProperties(props).build());
      setDefaultOnCondition(props, !isMetricsConfigSet, HoodieMetricsConfig.newBuilder().fromProperties(props).build());
      setDefaultOnCondition(props, !isBootstrapConfigSet,
          HoodieBootstrapConfig.newBuilder().fromProperties(props).build());
      setDefaultOnCondition(props, !isMemoryConfigSet, HoodieMemoryConfig.newBuilder().fromProperties(props).build());
      setDefaultOnCondition(props, !isViewConfigSet,
          FileSystemViewStorageConfig.newBuilder().fromProperties(props).build());
      setDefaultOnCondition(props, !isConsistencyGuardSet,
          ConsistencyGuardConfig.newBuilder().fromProperties(props).build());
      setDefaultOnCondition(props, !isCallbackConfigSet,
          HoodieWriteCommitCallbackConfig.newBuilder().fromProperties(props).build());
      setDefaultOnCondition(props, !isPayloadConfigSet,
          HoodiePayloadConfig.newBuilder().fromProperties(props).build());
      setDefaultOnCondition(props, !isMetadataConfigSet,
          HoodieMetadataConfig.newBuilder().fromProperties(props).build());
      setDefaultOnCondition(props, !isLockConfigSet,
          HoodieLockConfig.newBuilder().fromProperties(props).build());

      setDefaultValue(props, EXTERNAL_RECORD_AND_SCHEMA_TRANSFORMATION);
      setDefaultOnCondition(props, !props.containsKey(TIMELINE_LAYOUT_VERSION.key()), TIMELINE_LAYOUT_VERSION.key(),
          String.valueOf(TimelineLayoutVersion.CURR_VERSION));

    }

    private void validate() {
      String layoutVersion = props.getProperty(TIMELINE_LAYOUT_VERSION.key());
      // Ensure Layout Version is good
      new TimelineLayoutVersion(Integer.parseInt(layoutVersion));
      Objects.requireNonNull(props.getProperty(BASE_PATH_PROP.key()));
      if (props.getProperty(WRITE_CONCURRENCY_MODE_PROP.key())
          .equalsIgnoreCase(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL.name())) {
        ValidationUtils.checkArgument(props.getProperty(HoodieCompactionConfig.FAILED_WRITES_CLEANER_POLICY_PROP.key())
            != HoodieFailedWritesCleaningPolicy.EAGER.name(), "To enable optimistic concurrency control, set hoodie.cleaner.policy.failed.writes=LAZY");
      }
    }

    public HoodieWriteConfig build() {
      setDefaults();
      validate();
      // Build WriteConfig at the end
      HoodieWriteConfig config = new HoodieWriteConfig(engineType, props);
      return config;
    }
  }
}
