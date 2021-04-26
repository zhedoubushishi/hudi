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

import javax.annotation.concurrent.Immutable;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * Storage related config.
 */
@Immutable
public class HoodieStorageConfig extends DefaultHoodieConfig {

  public static final ConfigOption<String> PARQUET_FILE_MAX_BYTES = ConfigOption
      .key("hoodie.parquet.max.file.size")
      .defaultValue(String.valueOf(120 * 1024 * 1024))
      .withDescription("");

  public static final ConfigOption<String> PARQUET_BLOCK_SIZE_BYTES = ConfigOption
      .key("hoodie.parquet.block.size")
      .defaultValue(String.valueOf(120 * 1024 * 1024))
      .withDescription("");

  public static final ConfigOption<String> PARQUET_PAGE_SIZE_BYTES = ConfigOption
      .key("hoodie.parquet.page.size")
      .defaultValue(String.valueOf(1 * 1024 * 1024))
      .withDescription("");

  public static final ConfigOption<String> HFILE_FILE_MAX_BYTES = ConfigOption
      .key("hoodie.hfile.max.file.size")
      .defaultValue(String.valueOf(120 * 1024 * 1024))
      .withDescription("");

  public static final ConfigOption<String> HFILE_BLOCK_SIZE_BYTES = ConfigOption
      .key("hoodie.hfile.block.size")
      .defaultValue(String.valueOf(1 * 1024 * 1024))
      .withDescription("");

  // used to size log files
  public static final ConfigOption<String> LOGFILE_SIZE_MAX_BYTES = ConfigOption
      .key("hoodie.logfile.max.size")
      .defaultValue(String.valueOf(1024 * 1024 * 1024)) // 1 GB
      .withDescription("");

  // used to size data blocks in log file
  public static final ConfigOption<String> LOGFILE_DATA_BLOCK_SIZE_MAX_BYTES = ConfigOption
      .key("hoodie.logfile.data.block.max.size")
      .defaultValue(String.valueOf(256 * 1024 * 1024))
      .withDescription("");

  // Default compression ratio for parquet
  public static final ConfigOption<String> PARQUET_COMPRESSION_RATIO = ConfigOption
      .key("hoodie.parquet.compression.ratio")
      .defaultValue(String.valueOf(0.1))
      .withDescription("");

  // Default compression codec for parquet
  public static final ConfigOption<String> PARQUET_COMPRESSION_CODEC = ConfigOption
      .key("hoodie.parquet.compression.codec")
      .defaultValue("gzip")
      .withDescription("");

  public static final ConfigOption<String> HFILE_COMPRESSION_ALGORITHM = ConfigOption
      .key("hoodie.hfile.compression.algorithm")
      .defaultValue("GZ")
      .withDescription("");

  // Default compression ratio for log file to parquet, general 3x
  public static final ConfigOption<String> LOGFILE_TO_PARQUET_COMPRESSION_RATIO = ConfigOption
      .key("hoodie.logfile.to.parquet.compression.ratio")
      .defaultValue(String.valueOf(0.35))
      .withDescription("");

  private HoodieStorageConfig(Properties props) {
    super(props);
  }

  public static HoodieStorageConfig.Builder newBuilder() {
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

    public Builder parquetMaxFileSize(long maxFileSize) {
      props.setProperty(PARQUET_FILE_MAX_BYTES.key(), String.valueOf(maxFileSize));
      return this;
    }

    public Builder parquetBlockSize(int blockSize) {
      props.setProperty(PARQUET_BLOCK_SIZE_BYTES.key(), String.valueOf(blockSize));
      return this;
    }

    public Builder parquetPageSize(int pageSize) {
      props.setProperty(PARQUET_PAGE_SIZE_BYTES.key(), String.valueOf(pageSize));
      return this;
    }

    public Builder hfileMaxFileSize(long maxFileSize) {
      props.setProperty(HFILE_FILE_MAX_BYTES.key(), String.valueOf(maxFileSize));
      return this;
    }

    public Builder hfileBlockSize(int blockSize) {
      props.setProperty(HFILE_BLOCK_SIZE_BYTES.key(), String.valueOf(blockSize));
      return this;
    }

    public Builder logFileDataBlockMaxSize(int dataBlockSize) {
      props.setProperty(LOGFILE_DATA_BLOCK_SIZE_MAX_BYTES.key(), String.valueOf(dataBlockSize));
      return this;
    }

    public Builder logFileMaxSize(int logFileSize) {
      props.setProperty(LOGFILE_SIZE_MAX_BYTES.key(), String.valueOf(logFileSize));
      return this;
    }

    public Builder parquetCompressionRatio(double parquetCompressionRatio) {
      props.setProperty(PARQUET_COMPRESSION_RATIO.key(), String.valueOf(parquetCompressionRatio));
      return this;
    }

    public Builder parquetCompressionCodec(String parquetCompressionCodec) {
      props.setProperty(PARQUET_COMPRESSION_CODEC.key(), parquetCompressionCodec);
      return this;
    }

    public Builder hfileCompressionAlgorithm(String hfileCompressionAlgorithm) {
      props.setProperty(HFILE_COMPRESSION_ALGORITHM.key(), hfileCompressionAlgorithm);
      return this;
    }

    public Builder logFileToParquetCompressionRatio(double logFileToParquetCompressionRatio) {
      props.setProperty(LOGFILE_TO_PARQUET_COMPRESSION_RATIO.key(), String.valueOf(logFileToParquetCompressionRatio));
      return this;
    }

    public HoodieStorageConfig build() {
      HoodieStorageConfig config = new HoodieStorageConfig(props);
      setDefaultValue(props, PARQUET_FILE_MAX_BYTES);
      setDefaultValue(props, PARQUET_BLOCK_SIZE_BYTES);
      setDefaultValue(props, PARQUET_PAGE_SIZE_BYTES);
      setDefaultValue(props, LOGFILE_DATA_BLOCK_SIZE_MAX_BYTES);
      setDefaultValue(props, LOGFILE_SIZE_MAX_BYTES);
      setDefaultValue(props, PARQUET_COMPRESSION_RATIO);
      setDefaultValue(props, PARQUET_COMPRESSION_CODEC);
      setDefaultValue(props, LOGFILE_TO_PARQUET_COMPRESSION_RATIO);
      setDefaultValue(props, HFILE_BLOCK_SIZE_BYTES);
      setDefaultValue(props, HFILE_COMPRESSION_ALGORITHM);
      setDefaultValue(props, HFILE_FILE_MAX_BYTES);

      return config;
    }
  }

}
