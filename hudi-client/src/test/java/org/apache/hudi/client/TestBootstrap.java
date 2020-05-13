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

import org.apache.hudi.avro.model.HoodieFileStatus;
import org.apache.hudi.client.bootstrap.BootstrapMode;
import org.apache.hudi.client.bootstrap.FullBootstrapInputProvider;
import org.apache.hudi.client.bootstrap.selector.BootstrapModeSelector;
import org.apache.hudi.client.bootstrap.selector.FullBootstrapModeSelector;
import org.apache.hudi.client.bootstrap.selector.MetadataOnlyBootstrapModeSelector;
import org.apache.hudi.client.utils.ParquetReaderIterator;
import org.apache.hudi.common.HoodieMergeOnReadTestUtils;
import org.apache.hudi.common.HoodieTestDataGenerator;
import org.apache.hudi.common.TestRawTripPayload;
import org.apache.hudi.common.bootstrap.FileStatusUtils;
import org.apache.hudi.common.bootstrap.index.BootstrapIndex;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieTestUtils;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hadoop.HoodieParquetInputFormat;
import org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import org.apache.spark.sql.SaveMode;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Map;
import java.util.Random;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;

public class TestBootstrap extends TestHoodieClientBase {

  protected TemporaryFolder tmpFolder = null;
  protected String srcPath = null;

  private HoodieParquetInputFormat roInputFormat;
  private JobConf roJobConf;

  private HoodieParquetRealtimeInputFormat rtInputFormat;
  private JobConf rtJobConf;

  @BeforeEach
  public void setUp() throws Exception {
    initResources();
    tmpFolder = new TemporaryFolder();
    tmpFolder.create();
    srcPath = tmpFolder.getRoot().getAbsolutePath() + "/data";

    // initialize parquet input format
    reloadInputFormats();
  }

  private void reloadInputFormats() {
    // initialize parquet input format
    roInputFormat = new HoodieParquetInputFormat();
    roJobConf = new JobConf(jsc.hadoopConfiguration());
    roInputFormat.setConf(roJobConf);

    rtInputFormat = new HoodieParquetRealtimeInputFormat();
    rtJobConf = new JobConf(jsc.hadoopConfiguration());
    rtInputFormat.setConf(rtJobConf);
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (tmpFolder != null) {
      tmpFolder.delete();
      tmpFolder = null;
    }
    cleanupResources();
  }

  public Schema generateNewDataSetAndReturnSchema(double timestamp, int numRecords, List<String> partitionPaths,
      String srcPath) throws Exception {
    boolean isPartitioned = partitionPaths != null && !partitionPaths.isEmpty();
    Dataset<Row> df = HoodieTestDataGenerator.generateTestRawTripDataset(timestamp,
        numRecords, partitionPaths, jsc, sqlContext);
    df.printSchema();
    if (isPartitioned) {
      df.write().partitionBy("datestr").format("parquet").mode(SaveMode.Overwrite).save(srcPath);
    } else {
      df.write().format("parquet").mode(SaveMode.Overwrite).save(srcPath);
    }
    String filePath = FileStatusUtils.toPath(FSUtils.getAllLeafFoldersWithFiles(metaClient.getFs(), srcPath,
        (status) -> true).stream().findAny().map(p -> p.getValue().stream().findAny()).orElse(null)
        .get().getPath()).toString();
    ParquetFileReader reader = ParquetFileReader.open(metaClient.getHadoopConf(), new Path(filePath));
    MessageType schema = reader.getFooter().getFileMetaData().getSchema();
    return new AvroSchemaConverter().convert(schema);
  }

  @Test
  public void testMetadataBootstrapUnpartitionedCOW() throws Exception {
    /**
     * Perform metadata bootstrap of source and check the content
     */
    int totalRecords = 100;
    List<String> partitions = Arrays.asList();
    double timestamp = new Double(Instant.now().toEpochMilli()).longValue();
    Schema schema = generateNewDataSetAndReturnSchema(timestamp, totalRecords, partitions, srcPath);
    HoodieWriteConfig config = getConfigBuilder(schema.toString())
        .withBootstrapSourceBasePath(srcPath)
        .withBootstrapRecordKeyColumns("_row_key")
        .withBootstrapParallelism(3)
        .withAutoCommit(true)
        .withSchema(schema.toString())
        .withBootstrapModeSelector(MetadataOnlyBootstrapModeSelector.class.getName()).build();
    HoodieWriteClient client = new HoodieWriteClient(jsc, config);
    client.bootstrap(Option.empty());
    checkBootstrapResults(totalRecords, schema, HoodieTimeline.METADATA_BOOTSTRAP_INSTANT_TS, true, 1, timestamp,
        timestamp, false);

    // Rollback Bootstrap
    FSUtils.deleteInstantFile(metaClient.getFs(), metaClient.getMetaPath(), new HoodieInstant(State.COMPLETED,
        HoodieTimeline.COMMIT_ACTION, HoodieTimeline.METADATA_BOOTSTRAP_INSTANT_TS));
    client.rollBackPendingBootstrap();
    metaClient.reloadActiveTimeline();
    Assert.assertEquals(0, metaClient.getCommitsTimeline().countInstants());
    Assert.assertEquals(0L, FSUtils.getAllLeafFoldersWithFiles(metaClient.getFs(), basePath,
        (status) -> status.getName().endsWith(".parquet")).stream().flatMap(f -> f.getValue().stream()).count());

    BootstrapIndex index = BootstrapIndex.getBootstrapIndex(metaClient);
    Assert.assertFalse(index.isIndexAvailable());

    // Run bootstrap again
    client = new HoodieWriteClient(jsc, config);
    client.bootstrap(Option.empty());

    metaClient.reloadActiveTimeline();
    index = BootstrapIndex.getBootstrapIndex(metaClient);
    Assert.assertTrue(index.isIndexAvailable());

    checkBootstrapResults(totalRecords, schema, HoodieTimeline.METADATA_BOOTSTRAP_INSTANT_TS, true, 1, timestamp,
        timestamp, false);

    // Upsert case
    double updateTimestamp = new Double(Instant.now().toEpochMilli()).longValue();
    String updateSPath = tmpFolder.getRoot().getAbsolutePath() + "/data2";
    generateNewDataSetAndReturnSchema(updateTimestamp, totalRecords, partitions, updateSPath);
    JavaRDD<HoodieRecord> updateBatch =
        generateInputBatch(jsc, FSUtils.getAllLeafFoldersWithFiles(metaClient.getFs(), updateSPath,
            (status) -> status.getName().endsWith("parquet")), schema);
    String newInstantTs = client.startCommit();
    client.upsert(updateBatch, newInstantTs);
    checkBootstrapResults(totalRecords, schema, newInstantTs, true, 2, updateTimestamp,
        updateTimestamp, false);
  }

  @Test
  public void testMetadataBootstrapWithUpdatesCOW() throws Exception {
    /**
     * Perform metadata bootstrap of source and check the content
     */
    int totalRecords = 100;
    List<String> partitions = Arrays.asList("2020/04/01", "2020/04/02", "2020/04/03");
    double timestamp = new Double(Instant.now().toEpochMilli()).longValue();
    Schema schema = generateNewDataSetAndReturnSchema(timestamp, totalRecords, partitions, srcPath);
    HoodieWriteConfig config = getConfigBuilder(schema.toString())
        .withBootstrapSourceBasePath(srcPath)
        .withBootstrapRecordKeyColumns("_row_key")
        .withBootstrapParallelism(3)
        .withAutoCommit(true)
        .withSchema(schema.toString())
        .withBootstrapModeSelector(MetadataOnlyBootstrapModeSelector.class.getName()).build();
    HoodieWriteClient client = new HoodieWriteClient(jsc, config);
    client.bootstrap(Option.empty());
    checkBootstrapResults(totalRecords, schema, HoodieTimeline.METADATA_BOOTSTRAP_INSTANT_TS, true, 1, timestamp,
        timestamp, false);

    // Rollback Bootstrap
    FSUtils.deleteInstantFile(metaClient.getFs(), metaClient.getMetaPath(), new HoodieInstant(State.COMPLETED,
        HoodieTimeline.COMMIT_ACTION, HoodieTimeline.METADATA_BOOTSTRAP_INSTANT_TS));
    client.rollBackPendingBootstrap();
    metaClient.reloadActiveTimeline();
    Assert.assertEquals(0, metaClient.getCommitsTimeline().countInstants());
    Assert.assertEquals(0L, FSUtils.getAllLeafFoldersWithFiles(metaClient.getFs(), basePath,
        (status) -> status.getName().endsWith(".parquet")).stream().flatMap(f -> f.getValue().stream()).count());

    BootstrapIndex index = BootstrapIndex.getBootstrapIndex(metaClient);
    Assert.assertFalse(index.isIndexAvailable());

    // Run bootstrap again
    client = new HoodieWriteClient(jsc, config);
    client.bootstrap(Option.empty());

    metaClient.reloadActiveTimeline();
    index = BootstrapIndex.getBootstrapIndex(metaClient);
    Assert.assertTrue(index.isIndexAvailable());

    checkBootstrapResults(totalRecords, schema, HoodieTimeline.METADATA_BOOTSTRAP_INSTANT_TS, true, 1, timestamp,
        timestamp, false);

    // Upsert case
    double updateTimestamp = new Double(Instant.now().toEpochMilli()).longValue();
    String updateSPath = tmpFolder.getRoot().getAbsolutePath() + "/data2";
    generateNewDataSetAndReturnSchema(updateTimestamp, totalRecords, partitions, updateSPath);
    JavaRDD<HoodieRecord> updateBatch =
        generateInputBatch(jsc, FSUtils.getAllLeafFoldersWithFiles(metaClient.getFs(), updateSPath,
            (status) -> status.getName().endsWith("parquet")), schema);
    String newInstantTs = client.startCommit();
    client.upsert(updateBatch, newInstantTs);
    checkBootstrapResults(totalRecords, schema, newInstantTs, true, 2, updateTimestamp,
        updateTimestamp, false);
  }

  @Test
  public void testMetadataBootstrapWithUpdatesMOR() throws Exception {
    /**
     * Perform metadata bootstrap of source and check the content
     */
    metaClient = HoodieTestUtils.init(basePath, HoodieTableType.MERGE_ON_READ);
    int totalRecords = 100;
    List<String> partitions = Arrays.asList("2020/04/01", "2020/04/02", "2020/04/03");
    double timestamp = new Double(Instant.now().toEpochMilli()).longValue();
    Schema schema = generateNewDataSetAndReturnSchema(timestamp, totalRecords, partitions, srcPath);
    HoodieWriteConfig config = getConfigBuilder(schema.toString())
        .withBootstrapSourceBasePath(srcPath)
        .withBootstrapRecordKeyColumns("_row_key")
        .withBootstrapParallelism(3)
        .withSchema(schema.toString())
        .withAutoCommit(true)
        .withBootstrapModeSelector(MetadataOnlyBootstrapModeSelector.class.getName()).build();
    HoodieWriteClient client = new HoodieWriteClient(jsc, config);
    client.bootstrap(Option.empty());
    checkBootstrapResults(totalRecords, schema, HoodieTimeline.METADATA_BOOTSTRAP_INSTANT_TS, true, 1,
        timestamp, timestamp, false);
    // Rollback Bootstrap
    FSUtils.deleteInstantFile(metaClient.getFs(), metaClient.getMetaPath(), new HoodieInstant(State.COMPLETED,
        HoodieTimeline.DELTA_COMMIT_ACTION, HoodieTimeline.METADATA_BOOTSTRAP_INSTANT_TS));
    client.rollBackPendingBootstrap();
    metaClient.reloadActiveTimeline();
    Assert.assertEquals(0, metaClient.getCommitsTimeline().countInstants());
    Assert.assertEquals(0L, FSUtils.getAllLeafFoldersWithFiles(metaClient.getFs(), basePath,
        (status) -> status.getName().endsWith(".parquet")).stream().flatMap(f -> f.getValue().stream()).count());

    BootstrapIndex index = BootstrapIndex.getBootstrapIndex(metaClient);
    Assert.assertFalse(index.isIndexAvailable());

    // Run bootstrap again
    client = new HoodieWriteClient(jsc, config);
    client.bootstrap(Option.empty());

    metaClient.reloadActiveTimeline();
    index = BootstrapIndex.getBootstrapIndex(metaClient);
    Assert.assertTrue(index.isIndexAvailable());

    // Upsert delta-commit case
    double updateTimestamp = new Double(Instant.now().toEpochMilli()).longValue();
    String updateSPath = tmpFolder.getRoot().getAbsolutePath() + "/data2";
    generateNewDataSetAndReturnSchema(updateTimestamp, totalRecords, partitions, updateSPath);
    JavaRDD<HoodieRecord> updateBatch =
        generateInputBatch(jsc, FSUtils.getAllLeafFoldersWithFiles(metaClient.getFs(), updateSPath,
            (status) -> status.getName().endsWith("parquet")), schema);
    String newInstantTs = client.startCommit();
    client.upsert(updateBatch, newInstantTs);
    checkBootstrapResults(totalRecords, schema, newInstantTs, false, 2, updateTimestamp,
        timestamp, true);

    Option<String> compactionInstant = client.scheduleCompaction(Option.empty());
    Assert.assertTrue(compactionInstant.isPresent());
    client.compact(compactionInstant.get());
    checkBootstrapResults(totalRecords, schema, compactionInstant.get(), true, 3, 2,
        updateTimestamp, updateTimestamp, false, Arrays.asList(compactionInstant.get()));
  }

  @Test
  public void testFullBoostrapOnlyCOW() throws Exception {
    /**
     * Perform full bootstrap of source and check the content
     */
    int totalRecords = 100;
    double timestamp = new Double(Instant.now().toEpochMilli()).longValue();
    List<String> partitions = Arrays.asList("2020/04/01", "2020/04/02", "2020/04/03");
    Schema schema = generateNewDataSetAndReturnSchema(timestamp, totalRecords, partitions, srcPath);
    HoodieWriteConfig config = getConfigBuilder(schema.toString())
        .withBootstrapSourceBasePath(srcPath)
        .withBootstrapRecordKeyColumns("_row_key")
        .withSchema(schema.toString())
        .withBootstrapParallelism(3)
        .withFullBootstrapInputProvider(FullTestBootstrapInputProvider.class.getName())
        .withBootstrapModeSelector(FullBootstrapModeSelector.class.getName()).build();
    HoodieWriteClient client = new HoodieWriteClient(jsc, config);
    client.bootstrap(Option.empty());
    checkBootstrapResults(totalRecords, schema, HoodieTimeline.FULL_BOOTSTRAP_INSTANT_TS, false, 1, timestamp,
        timestamp, false);
    // Rollback Bootstrap
    FSUtils.deleteInstantFile(metaClient.getFs(), metaClient.getMetaPath(), new HoodieInstant(State.COMPLETED,
        HoodieTimeline.COMMIT_ACTION, HoodieTimeline.FULL_BOOTSTRAP_INSTANT_TS));
    client.rollBackPendingBootstrap();
    metaClient.reloadActiveTimeline();
    Assert.assertEquals(0, metaClient.getCommitsTimeline().countInstants());
    Assert.assertEquals(0L, FSUtils.getAllLeafFoldersWithFiles(metaClient.getFs(), basePath,
        (status) -> status.getName().endsWith(".parquet")).stream().flatMap(f -> f.getValue().stream()).count());

    BootstrapIndex index = BootstrapIndex.getBootstrapIndex(metaClient);
    Assert.assertFalse(index.isIndexAvailable());

    // Run bootstrap again
    client = new HoodieWriteClient(jsc, config);
    client.bootstrap(Option.empty());

    metaClient.reloadActiveTimeline();
    index = BootstrapIndex.getBootstrapIndex(metaClient);
    Assert.assertFalse(index.isIndexAvailable());

    // Upsert case
    double updateTimestamp = new Double(Instant.now().toEpochMilli()).longValue();
    String updateSPath = tmpFolder.getRoot().getAbsolutePath() + "/data2";
    generateNewDataSetAndReturnSchema(updateTimestamp, totalRecords, partitions, updateSPath);
    JavaRDD<HoodieRecord> updateBatch =
        generateInputBatch(jsc, FSUtils.getAllLeafFoldersWithFiles(metaClient.getFs(), updateSPath,
            (status) -> status.getName().endsWith("parquet")), schema);
    String newInstantTs = client.startCommit();
    client.upsert(updateBatch, newInstantTs);
    checkBootstrapResults(totalRecords, schema, newInstantTs, false, 2, updateTimestamp,
        updateTimestamp, false);
  }

  @Test
  public void testFullBootstrapWithUpdatesMOR() throws Exception {
    /**
     * Perform metadata bootstrap of source and check the content
     */
    metaClient = HoodieTestUtils.init(basePath, HoodieTableType.MERGE_ON_READ);
    int totalRecords = 100;
    List<String> partitions = Arrays.asList("2020/04/01", "2020/04/02", "2020/04/03");
    double timestamp = new Double(Instant.now().toEpochMilli()).longValue();
    Schema schema = generateNewDataSetAndReturnSchema(timestamp, totalRecords, partitions, srcPath);
    HoodieWriteConfig config = getConfigBuilder(schema.toString())
        .withBootstrapSourceBasePath(srcPath)
        .withBootstrapRecordKeyColumns("_row_key")
        .withSchema(schema.toString())
        .withBootstrapParallelism(3)
        .withFullBootstrapInputProvider(FullTestBootstrapInputProvider.class.getName())
        .withBootstrapModeSelector(FullBootstrapModeSelector.class.getName()).build();
    HoodieWriteClient client = new HoodieWriteClient(jsc, config);
    client.bootstrap(Option.empty());
    checkBootstrapResults(totalRecords, schema, HoodieTimeline.FULL_BOOTSTRAP_INSTANT_TS, false, 1, timestamp,
        timestamp, false);
    // Rollback Bootstrap
    FSUtils.deleteInstantFile(metaClient.getFs(), metaClient.getMetaPath(), new HoodieInstant(State.COMPLETED,
        HoodieTimeline.DELTA_COMMIT_ACTION, HoodieTimeline.FULL_BOOTSTRAP_INSTANT_TS));
    client.rollBackPendingBootstrap();
    metaClient.reloadActiveTimeline();
    Assert.assertEquals(0, metaClient.getCommitsTimeline().countInstants());
    Assert.assertEquals(0L, FSUtils.getAllLeafFoldersWithFiles(metaClient.getFs(), basePath,
        (status) -> status.getName().endsWith(".parquet")).stream().flatMap(f -> f.getValue().stream()).count());

    BootstrapIndex index = BootstrapIndex.getBootstrapIndex(metaClient);
    Assert.assertFalse(index.isIndexAvailable());

    // Run bootstrap again
    client = new HoodieWriteClient(jsc, config);
    client.bootstrap(Option.empty());

    metaClient.reloadActiveTimeline();
    index = BootstrapIndex.getBootstrapIndex(metaClient);
    Assert.assertFalse(index.isIndexAvailable());

    // Upsert delta-commit case
    double updateTimestamp = new Double(Instant.now().toEpochMilli()).longValue();
    String updateSPath = tmpFolder.getRoot().getAbsolutePath() + "/data2";
    generateNewDataSetAndReturnSchema(updateTimestamp, totalRecords, partitions, updateSPath);
    JavaRDD<HoodieRecord> updateBatch =
        generateInputBatch(jsc, FSUtils.getAllLeafFoldersWithFiles(metaClient.getFs(), updateSPath,
            (status) -> status.getName().endsWith("parquet")), schema);
    String newInstantTs = client.startCommit();
    client.upsert(updateBatch, newInstantTs);
    checkBootstrapResults(totalRecords, schema, newInstantTs, false, 2, updateTimestamp,
        timestamp, true);

    Option<String> compactionInstant = client.scheduleCompaction(Option.empty());
    Assert.assertTrue(compactionInstant.isPresent());
    client.compact(compactionInstant.get());
    checkBootstrapResults(totalRecords, schema, compactionInstant.get(), false, 3, 2,
        updateTimestamp, updateTimestamp, false, Arrays.asList(compactionInstant.get()));
  }

  @Test
  public void testMetaAndFullBoostrapCOW() throws Exception {
    /**
     * Perform full bootstrap of source and check the content
     */
    int totalRecords = 100;
    double timestamp = new Double(Instant.now().toEpochMilli()).longValue();
    List<String> partitions = Arrays.asList("2020/04/01", "2020/04/02", "2020/04/03");
    Schema schema = generateNewDataSetAndReturnSchema(timestamp, totalRecords, partitions, srcPath);
    HoodieWriteConfig config = getConfigBuilder(schema.toString())
        .withBootstrapSourceBasePath(srcPath)
        .withBootstrapRecordKeyColumns("_row_key")
        .withBootstrapParallelism(3)
        .withSchema(schema.toString())
        .withFullBootstrapInputProvider(FullTestBootstrapInputProvider.class.getName())
        .withBootstrapModeSelector(TestRandomBootstapModeSelector.class.getName()).build();
    HoodieWriteClient client = new HoodieWriteClient(jsc, config);
    client.bootstrap(Option.empty());
    checkBootstrapResults(totalRecords, schema, HoodieTimeline.FULL_BOOTSTRAP_INSTANT_TS, false, 2, 2,
        timestamp, timestamp, false,
        Arrays.asList(HoodieTimeline.METADATA_BOOTSTRAP_INSTANT_TS, HoodieTimeline.FULL_BOOTSTRAP_INSTANT_TS));
    // Rollback Bootstrap
    FSUtils.deleteInstantFile(metaClient.getFs(), metaClient.getMetaPath(), new HoodieInstant(State.COMPLETED,
        HoodieTimeline.COMMIT_ACTION, HoodieTimeline.FULL_BOOTSTRAP_INSTANT_TS));
    client.rollBackPendingBootstrap();
    metaClient.reloadActiveTimeline();
    Assert.assertEquals(0, metaClient.getCommitsTimeline().countInstants());
    Assert.assertEquals(0L, FSUtils.getAllLeafFoldersWithFiles(metaClient.getFs(), basePath,
        (status) -> status.getName().endsWith(".parquet")).stream().flatMap(f -> f.getValue().stream()).count());

    BootstrapIndex index = BootstrapIndex.getBootstrapIndex(metaClient);
    Assert.assertFalse(index.isIndexAvailable());

    // Run bootstrap again
    client = new HoodieWriteClient(jsc, config);
    client.bootstrap(Option.empty());

    metaClient.reloadActiveTimeline();
    index = BootstrapIndex.getBootstrapIndex(metaClient);
    Assert.assertTrue(index.isIndexAvailable());

    // Upsert case
    double updateTimestamp = new Double(Instant.now().toEpochMilli()).longValue();
    String updateSPath = tmpFolder.getRoot().getAbsolutePath() + "/data2";
    generateNewDataSetAndReturnSchema(updateTimestamp, totalRecords, partitions, updateSPath);
    JavaRDD<HoodieRecord> updateBatch =
        generateInputBatch(jsc, FSUtils.getAllLeafFoldersWithFiles(metaClient.getFs(), updateSPath,
            (status) -> status.getName().endsWith("parquet")), schema);
    String newInstantTs = client.startCommit();
    client.upsert(updateBatch, newInstantTs);
    checkBootstrapResults(totalRecords, schema, newInstantTs, false, 3, updateTimestamp,
        updateTimestamp, false);
  }

  @Test
  public void testMetadataAndFullBootstrapWithUpdatesMOR() throws Exception {
    /**
     * Perform metadata bootstrap of source and check the content
     */
    metaClient = HoodieTestUtils.init(basePath, HoodieTableType.MERGE_ON_READ);
    int totalRecords = 100;
    List<String> partitions = Arrays.asList("2020/04/01", "2020/04/02", "2020/04/03");
    double timestamp = new Double(Instant.now().toEpochMilli()).longValue();
    Schema schema = generateNewDataSetAndReturnSchema(timestamp, totalRecords, partitions, srcPath);
    HoodieWriteConfig config = getConfigBuilder(schema.toString())
        .withBootstrapSourceBasePath(srcPath)
        .withBootstrapRecordKeyColumns("_row_key")
        .withBootstrapParallelism(3)
        .withSchema(schema.toString())
        .withFullBootstrapInputProvider(FullTestBootstrapInputProvider.class.getName())
        .withBootstrapModeSelector(TestRandomBootstapModeSelector.class.getName()).build();
    HoodieWriteClient client = new HoodieWriteClient(jsc, config);
    client.bootstrap(Option.empty());
    checkBootstrapResults(totalRecords, schema, HoodieTimeline.FULL_BOOTSTRAP_INSTANT_TS, false, 2, 2,
        timestamp, timestamp, false,
        Arrays.asList(HoodieTimeline.METADATA_BOOTSTRAP_INSTANT_TS, HoodieTimeline.FULL_BOOTSTRAP_INSTANT_TS));
    // Rollback Bootstrap
    FSUtils.deleteInstantFile(metaClient.getFs(), metaClient.getMetaPath(), new HoodieInstant(State.COMPLETED,
        HoodieTimeline.DELTA_COMMIT_ACTION, HoodieTimeline.FULL_BOOTSTRAP_INSTANT_TS));
    client.rollBackPendingBootstrap();
    metaClient.reloadActiveTimeline();
    Assert.assertEquals(0, metaClient.getCommitsTimeline().countInstants());
    Assert.assertEquals(0L, FSUtils.getAllLeafFoldersWithFiles(metaClient.getFs(), basePath,
        (status) -> status.getName().endsWith(".parquet")).stream().flatMap(f -> f.getValue().stream()).count());

    BootstrapIndex index = BootstrapIndex.getBootstrapIndex(metaClient);
    Assert.assertFalse(index.isIndexAvailable());

    // Run bootstrap again
    client = new HoodieWriteClient(jsc, config);
    client.bootstrap(Option.empty());

    metaClient.reloadActiveTimeline();
    index = BootstrapIndex.getBootstrapIndex(metaClient);
    Assert.assertTrue(index.isIndexAvailable());

    // Upsert delta-commit case
    double updateTimestamp = new Double(Instant.now().toEpochMilli()).longValue();
    String updateSPath = tmpFolder.getRoot().getAbsolutePath() + "/data2";
    generateNewDataSetAndReturnSchema(updateTimestamp, totalRecords, partitions, updateSPath);
    JavaRDD<HoodieRecord> updateBatch =
        generateInputBatch(jsc, FSUtils.getAllLeafFoldersWithFiles(metaClient.getFs(), updateSPath,
            (status) -> status.getName().endsWith("parquet")), schema);
    String newInstantTs = client.startCommit();
    client.upsert(updateBatch, newInstantTs);
    checkBootstrapResults(totalRecords, schema, newInstantTs, false, 3, 2, updateTimestamp,
        timestamp, true, Arrays.asList(newInstantTs));

    Option<String> compactionInstant = client.scheduleCompaction(Option.empty());
    Assert.assertTrue(compactionInstant.isPresent());
    client.compact(compactionInstant.get());
    checkBootstrapResults(totalRecords, schema, compactionInstant.get(), false, 4, 2,
        updateTimestamp, updateTimestamp, false, Arrays.asList(compactionInstant.get()));
  }

  private void checkBootstrapResults(int totalRecords, Schema schema, String maxInstant, boolean checkNumRawFiles,
      int expNumInstants, double expTimestamp, double expROTimestamp, boolean isDeltaCommit) throws Exception {
    checkBootstrapResults(totalRecords, schema, maxInstant, checkNumRawFiles, expNumInstants, expNumInstants,
        expTimestamp, expROTimestamp, isDeltaCommit, Arrays.asList(maxInstant));
  }

  private void checkBootstrapResults(int totalRecords, Schema schema, String instant, boolean checkNumRawFiles,
      int expNumInstants, int numVersions, double expTimestamp, double expROTimestamp, boolean isDeltaCommit,
      List<String> instantsWithValidRecords) throws Exception {
    metaClient.reloadActiveTimeline();
    Assert.assertEquals(expNumInstants, metaClient.getCommitsTimeline().filterCompletedInstants().countInstants());
    Assert.assertEquals(instant, metaClient.getActiveTimeline()
        .getCommitsTimeline().filterCompletedInstants().lastInstant().get().getTimestamp());

    Dataset<Row> bootstrapped = sqlContext.read().format("parquet").load(basePath);
    Dataset<Row> original = sqlContext.read().format("parquet").load(srcPath);
    bootstrapped.registerTempTable("bootstrapped");
    original.registerTempTable("original");
    if (checkNumRawFiles) {
      List<HoodieFileStatus> files = FSUtils.getAllLeafFoldersWithFiles(metaClient.getFs(), srcPath,
          (status) -> status.getName().endsWith(".parquet"))
          .stream().flatMap(x -> x.getValue().stream()).collect(Collectors.toList());
      Assert.assertEquals(files.size() * numVersions,
          sqlContext.sql("select distinct _hoodie_file_name from bootstrapped").count());
    }

    if (!isDeltaCommit) {
      String predicate = String.join(", ",
          instantsWithValidRecords.stream().map(p -> "\"" + p + "\"").collect(Collectors.toList()));
      Assert.assertEquals(totalRecords, sqlContext.sql("select * from bootstrapped where _hoodie_commit_time IN "
          + "(" + predicate + ")").count());
      Dataset<Row> missingOriginal = sqlContext.sql("select a._row_key from original a where a._row_key not "
          + "in (select _hoodie_record_key from bootstrapped)");
      Assert.assertEquals(0, missingOriginal.count());
      Dataset<Row> missingBootstrapped = sqlContext.sql("select a._hoodie_record_key from bootstrapped a "
          + "where a._hoodie_record_key not in (select _row_key from original)");
      Assert.assertEquals(0, missingBootstrapped.count());
      //sqlContext.sql("select * from bootstrapped").show(10, false);
    }

    // RO Input Format Read
    reloadInputFormats();
    List<GenericRecord> records = HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(
        FSUtils.getAllPartitionPaths(metaClient.getFs(), basePath, false).stream()
            .map(f -> basePath + "/" + f).collect(Collectors.toList()),
        basePath, roJobConf, roInputFormat, schema, HoodieTestDataGenerator.TRIP_HIVE_COLUMN_TYPES);
    Assert.assertEquals(totalRecords, records.size());
    Set<String> seenKeys = new HashSet<>();
    for (GenericRecord r : records) {
      Assert.assertEquals("Record :" + r, r.get("_row_key").toString(), r.get("_hoodie_record_key").toString());
      Assert.assertEquals("Record :" + r, expROTimestamp, ((DoubleWritable)r.get("timestamp")).get(), 0.1);
      Assert.assertFalse(seenKeys.contains(r.get("_hoodie_record_key").toString()));
      seenKeys.add(r.get("_hoodie_record_key").toString());
    }
    Assert.assertEquals(totalRecords, seenKeys.size());

    //RT Input Format Read
    reloadInputFormats();
    seenKeys = new HashSet<>();
    records = HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(
        FSUtils.getAllPartitionPaths(metaClient.getFs(), basePath, false).stream()
            .map(f -> basePath + "/" + f).collect(Collectors.toList()),
        basePath, rtJobConf, rtInputFormat, schema,  HoodieTestDataGenerator.TRIP_HIVE_COLUMN_TYPES);
    Assert.assertEquals(totalRecords, records.size());
    for (GenericRecord r : records) {
      Assert.assertEquals("Realtime Record :" + r,
          r.get("_row_key").toString(), r.get("_hoodie_record_key").toString());
      Assert.assertEquals("Realtime Record :" + r, expTimestamp, ((DoubleWritable)r.get("timestamp")).get(),0.1);
      Assert.assertFalse(seenKeys.contains(r.get("_hoodie_record_key").toString()));
      seenKeys.add(r.get("_hoodie_record_key").toString());
    }
    Assert.assertEquals(totalRecords, seenKeys.size());

    // RO Input Format Read - Project only Hoodie Columns
    reloadInputFormats();
    records = HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(
        FSUtils.getAllPartitionPaths(metaClient.getFs(), basePath, false).stream()
            .map(f -> basePath + "/" + f).collect(Collectors.toList()),
        basePath, roJobConf, roInputFormat, schema, HoodieTestDataGenerator.TRIP_HIVE_COLUMN_TYPES, true,
        HoodieRecord.HOODIE_META_COLUMNS);
    Assert.assertEquals(totalRecords, records.size());
    seenKeys = new HashSet<>();
    for (GenericRecord r : records) {
      Assert.assertFalse(seenKeys.contains(r.get("_hoodie_record_key").toString()));
      seenKeys.add(r.get("_hoodie_record_key").toString());
    }
    Assert.assertEquals(totalRecords, seenKeys.size());

    //RT Input Format Read - Project only Hoodie Columns
    reloadInputFormats();
    seenKeys = new HashSet<>();
    records = HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(
        FSUtils.getAllPartitionPaths(metaClient.getFs(), basePath, false).stream()
            .map(f -> basePath + "/" + f).collect(Collectors.toList()),
        basePath, rtJobConf, rtInputFormat, schema,  HoodieTestDataGenerator.TRIP_HIVE_COLUMN_TYPES, true,
        HoodieRecord.HOODIE_META_COLUMNS);
    Assert.assertEquals(totalRecords, records.size());
    for (GenericRecord r : records) {
      Assert.assertFalse(seenKeys.contains(r.get("_hoodie_record_key").toString()));
      seenKeys.add(r.get("_hoodie_record_key").toString());
    }
    Assert.assertEquals(totalRecords, seenKeys.size());

    // RO Input Format Read - Project only non-hoodie column
    reloadInputFormats();
    records = HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(
        FSUtils.getAllPartitionPaths(metaClient.getFs(), basePath, false).stream()
            .map(f -> basePath + "/" + f).collect(Collectors.toList()),
        basePath, roJobConf, roInputFormat, schema, HoodieTestDataGenerator.TRIP_HIVE_COLUMN_TYPES, true,
        Arrays.asList("_row_key"));
    Assert.assertEquals(totalRecords, records.size());
    seenKeys = new HashSet<>();
    for (GenericRecord r : records) {
      Assert.assertFalse(seenKeys.contains(r.get("_row_key").toString()));
      seenKeys.add(r.get("_row_key").toString());
    }
    Assert.assertEquals(totalRecords, seenKeys.size());

    //RT Input Format Read - Project only non-hoodie column
    reloadInputFormats();
    seenKeys = new HashSet<>();
    records = HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(
        FSUtils.getAllPartitionPaths(metaClient.getFs(), basePath, false).stream()
            .map(f -> basePath + "/" + f).collect(Collectors.toList()),
        basePath, rtJobConf, rtInputFormat, schema,  HoodieTestDataGenerator.TRIP_HIVE_COLUMN_TYPES, true,
        Arrays.asList("_row_key"));
    Assert.assertEquals(totalRecords, records.size());
    for (GenericRecord r : records) {
      Assert.assertFalse(seenKeys.contains(r.get("_row_key").toString()));
      seenKeys.add(r.get("_row_key").toString());
    }
    Assert.assertEquals(totalRecords, seenKeys.size());
  }

  public static class FullTestBootstrapInputProvider extends FullBootstrapInputProvider {

    public FullTestBootstrapInputProvider(TypedProperties props, JavaSparkContext jsc) {
      super(props, jsc);
    }

    @Override
    public JavaRDD<HoodieRecord> generateInputRecordRDD(String tableName, String sourceBasePath,
        List<Pair<String, List<HoodieFileStatus>>> partitionPaths) {
      String filePath = FileStatusUtils.toPath(partitionPaths.stream().flatMap(p -> p.getValue().stream())
          .findAny().get().getPath()).toString();
      ParquetFileReader reader = null;
      try {
        reader = ParquetFileReader.open(jsc.hadoopConfiguration(), new Path(filePath));
      } catch (IOException e) {
        throw new HoodieIOException(e.getMessage(), e);
      }
      MessageType parquetSchema = reader.getFooter().getFileMetaData().getSchema();
      Schema schema =  new AvroSchemaConverter().convert(parquetSchema);
      return generateInputBatch(jsc, partitionPaths, schema);
    }
  }

  private static JavaRDD<HoodieRecord> generateInputBatch(JavaSparkContext jsc,
      List<Pair<String, List<HoodieFileStatus>>> partitionPaths, Schema writerSchema) {
    List<Pair<String, Path>> fullFilePathsWithPartition = partitionPaths.stream().flatMap(p -> p.getValue().stream()
        .map(x -> Pair.of(p.getKey(), FileStatusUtils.toPath(x.getPath())))).collect(Collectors.toList());
    return jsc.parallelize(fullFilePathsWithPartition.stream().flatMap(p -> {
      try {
        Configuration conf = jsc.hadoopConfiguration();
        AvroReadSupport.setAvroReadSchema(conf, writerSchema);
        Iterator<GenericRecord> recIterator = new ParquetReaderIterator(
            AvroParquetReader.<GenericRecord>builder(p.getValue()).withConf(conf).build());
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(recIterator, 0), false).map(gr -> {
          try {
            String key = gr.get("_row_key").toString();
            String pPath = p.getKey();
            return new HoodieRecord<>(new HoodieKey(key, pPath), new TestRawTripPayload(gr.toString(), key, pPath,
                HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA));
          } catch (IOException e) {
            throw new HoodieIOException(e.getMessage(), e);
          }
        });
      } catch (IOException ioe) {
        throw new HoodieIOException(ioe.getMessage(), ioe);
      }
    }).collect(Collectors.toList()));
  }

  public static class TestRandomBootstapModeSelector extends BootstrapModeSelector {

    private int currIdx = new Random().nextInt(2);

    public TestRandomBootstapModeSelector(HoodieWriteConfig writeConfig) {
      super(writeConfig);
    }

    @Override
    public Map<BootstrapMode, List<String>> select(List<Pair<String, List<HoodieFileStatus>>> partitions) {
      List<Pair<BootstrapMode, String>> selections = new ArrayList<>();
      partitions.stream().forEach(p -> {
        final BootstrapMode mode;
        if (currIdx == 0) {
          mode = BootstrapMode.METADATA_ONLY_BOOTSTRAP;
        } else {
          mode = BootstrapMode.FULL_BOOTSTRAP;
        }
        currIdx = (currIdx + 1) % 2;
        selections.add(Pair.of(mode, p.getKey()));
      });
      return selections.stream().collect(Collectors.groupingBy(Pair::getKey, mapping(Pair::getValue, toList())));
    }
  }
}