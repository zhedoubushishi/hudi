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

package org.apache.hudi.internal;

import org.apache.hudi.client.HoodieInternalWriteStatus;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.testutils.HoodieClientTestHarness;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Base class for TestHoodieBulkInsertDataInternalWriter.
 */
public class HoodieBulkInsertDataInternalWriterTestBase extends HoodieClientTestHarness {

  protected static final Random RANDOM = new Random();

  @BeforeEach
  public void setUp() throws Exception {
    initSparkContexts("TestHoodieBulkInsertDataInternalWriter");
    initPath();
    initFileSystem();
    initTestDataGenerator();
    initMetaClient();
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanupResources();
  }

  protected void assertWriteStatuses(List<HoodieInternalWriteStatus> writeStatuses, int batches, int size, List<String> fileAbsPaths, List<String> fileNames) {
    assertEquals(batches, writeStatuses.size());
    int counter = 0;
    for (HoodieInternalWriteStatus writeStatus : writeStatuses) {
      // verify write status
      assertEquals(writeStatus.getTotalRecords(), size);
      assertNull(writeStatus.getGlobalError());
      assertEquals(writeStatus.getFailedRowsSize(), 0);
      assertNotNull(writeStatus.getFileId());
      String fileId = writeStatus.getFileId();
      assertEquals(HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS[counter % 3], writeStatus.getPartitionPath());
      fileAbsPaths.add(basePath + "/" + writeStatus.getStat().getPath());
      fileNames.add(writeStatus.getStat().getPath().substring(writeStatus.getStat().getPath().lastIndexOf('/') + 1));
      HoodieWriteStat writeStat = writeStatus.getStat();
      assertEquals(size, writeStat.getNumInserts());
      assertEquals(size, writeStat.getNumWrites());
      assertEquals(fileId, writeStat.getFileId());
      assertEquals(HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS[counter++ % 3], writeStat.getPartitionPath());
      assertEquals(0, writeStat.getNumDeletes());
      assertEquals(0, writeStat.getNumUpdateWrites());
      assertEquals(0, writeStat.getTotalWriteErrors());
    }
  }

  protected void assertOutput(Dataset<Row> expectedRows, Dataset<Row> actualRows, String instantTime, List<String> fileNames) {
    // verify 3 meta fields that are filled in within create handle
    actualRows.collectAsList().forEach(entry -> {
      assertEquals(entry.get(HoodieRecord.HOODIE_META_COLUMNS_NAME_TO_POS.get(HoodieRecord.COMMIT_TIME_METADATA_FIELD)).toString(), instantTime);
      assertFalse(entry.isNullAt(HoodieRecord.HOODIE_META_COLUMNS_NAME_TO_POS.get(HoodieRecord.FILENAME_METADATA_FIELD)));
      assertTrue(fileNames.contains(entry.get(HoodieRecord.HOODIE_META_COLUMNS_NAME_TO_POS.get(HoodieRecord.FILENAME_METADATA_FIELD))));
      assertFalse(entry.isNullAt(HoodieRecord.HOODIE_META_COLUMNS_NAME_TO_POS.get(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD)));
    });

    // after trimming 2 of the meta fields, rest of the fields should match
    Dataset<Row> trimmedExpected = expectedRows.drop(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD, HoodieRecord.COMMIT_TIME_METADATA_FIELD, HoodieRecord.FILENAME_METADATA_FIELD);
    Dataset<Row> trimmedActual = actualRows.drop(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD, HoodieRecord.COMMIT_TIME_METADATA_FIELD, HoodieRecord.FILENAME_METADATA_FIELD);
    assertEquals(0, trimmedActual.except(trimmedExpected).count());
  }
}
