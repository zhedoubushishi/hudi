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

package org.apache.hudi.common.config;

import javax.annotation.concurrent.Immutable;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * Configurations used by the HUDI Metadata Table.
 */
@Immutable
public final class HoodieMetadataConfig extends DefaultHoodieConfig {

  public static final String METADATA_PREFIX = "hoodie.metadata";

  // Enable the internal Metadata Table which saves file listings
  public static final ConfigOption<Boolean> METADATA_ENABLE_PROP = ConfigOption
      .key(METADATA_PREFIX + ".enable")
      .defaultValue(false)
      .withDescription("");

  // Validate contents of Metadata Table on each access against the actual filesystem
  public static final ConfigOption<Boolean> METADATA_VALIDATE_PROP = ConfigOption
      .key(METADATA_PREFIX + ".validate")
      .defaultValue(false)
      .withDescription("");

  public static final boolean DEFAULT_METADATA_ENABLE_FOR_READERS = false;

  // Enable metrics for internal Metadata Table
  public static final ConfigOption<Boolean> METADATA_METRICS_ENABLE_PROP = ConfigOption
      .key(METADATA_PREFIX + ".metrics.enable")
      .defaultValue(false)
      .withDescription("");

  // Parallelism for inserts
  public static final ConfigOption<Integer> METADATA_INSERT_PARALLELISM_PROP = ConfigOption
      .key(METADATA_PREFIX + ".insert.parallelism")
      .defaultValue(1)
      .withDescription("");

  // Async clean
  public static final ConfigOption<Boolean> METADATA_ASYNC_CLEAN_PROP = ConfigOption
      .key(METADATA_PREFIX + ".clean.async")
      .defaultValue(false)
      .withDescription("");

  // Maximum delta commits before compaction occurs
  public static final ConfigOption<Integer> METADATA_COMPACT_NUM_DELTA_COMMITS_PROP = ConfigOption
      .key(METADATA_PREFIX + ".compact.max.delta.commits")
      .defaultValue(24)
      .withDescription("");

  // Archival settings
  public static final ConfigOption<Integer> MIN_COMMITS_TO_KEEP_PROP = ConfigOption
      .key(METADATA_PREFIX + ".keep.min.commits")
      .defaultValue(20)
      .withDescription("");

  public static final ConfigOption<Integer> MAX_COMMITS_TO_KEEP_PROP = ConfigOption
      .key(METADATA_PREFIX + ".keep.max.commits")
      .defaultValue(30)
      .withDescription("");

  // Cleaner commits retained
  public static final ConfigOption<Integer> CLEANER_COMMITS_RETAINED_PROP = ConfigOption
      .key(METADATA_PREFIX + ".cleaner.commits.retained")
      .defaultValue(3)
      .withDescription("");

  // Controls whether or not, upon failure to fetch from metadata table, should fallback to listing.
  public static final ConfigOption<String> ENABLE_FALLBACK_PROP = ConfigOption
      .key(METADATA_PREFIX + ".fallback.enable")
      .defaultValue("true")
      .withDescription("");

  // Regex to filter out matching directories during bootstrap
  public static final ConfigOption<String> DIRECTORY_FILTER_REGEX = ConfigOption
      .key(METADATA_PREFIX + ".dir.filter.regex")
      .defaultValue("")
      .withDescription("");

  public static final ConfigOption<String> HOODIE_ASSUME_DATE_PARTITIONING_PROP = ConfigOption
      .key("hoodie.assume.date.partitioning")
      .defaultValue("false")
      .withDescription("");

  public static final ConfigOption<Integer> FILE_LISTING_PARALLELISM_PROP = ConfigOption
      .key("hoodie.file.listing.parallelism")
      .defaultValue(1500)
      .withDescription("");

  private HoodieMetadataConfig(Properties props) {
    super(props);
  }

  public static HoodieMetadataConfig.Builder newBuilder() {
    return new Builder();
  }

  public int getFileListingParallelism() {
    return Math.max(Integer.parseInt(props.getProperty(HoodieMetadataConfig.FILE_LISTING_PARALLELISM_PROP.key())), 1);
  }

  public Boolean shouldAssumeDatePartitioning() {
    return Boolean.parseBoolean(props.getProperty(HoodieMetadataConfig.HOODIE_ASSUME_DATE_PARTITIONING_PROP.key()));
  }

  public boolean useFileListingMetadata() {
    return Boolean.parseBoolean(props.getProperty(METADATA_ENABLE_PROP.key()));
  }

  public boolean enableFallback() {
    return Boolean.parseBoolean(props.getProperty(ENABLE_FALLBACK_PROP.key()));
  }

  public boolean validateFileListingMetadata() {
    return Boolean.parseBoolean(props.getProperty(METADATA_VALIDATE_PROP.key()));
  }

  public boolean enableMetrics() {
    return Boolean.parseBoolean(props.getProperty(METADATA_METRICS_ENABLE_PROP.key()));
  }

  public String getDirectoryFilterRegex() {
    return props.getProperty(DIRECTORY_FILTER_REGEX.key());
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

    public Builder enable(boolean enable) {
      props.setProperty(METADATA_ENABLE_PROP.key(), String.valueOf(enable));
      return this;
    }

    public Builder enableMetrics(boolean enableMetrics) {
      props.setProperty(METADATA_METRICS_ENABLE_PROP.key(), String.valueOf(enableMetrics));
      return this;
    }

    public Builder enableFallback(boolean fallback) {
      props.setProperty(ENABLE_FALLBACK_PROP.key(), String.valueOf(fallback));
      return this;
    }

    public Builder validate(boolean validate) {
      props.setProperty(METADATA_VALIDATE_PROP.key(), String.valueOf(validate));
      return this;
    }

    public Builder withInsertParallelism(int parallelism) {
      props.setProperty(METADATA_INSERT_PARALLELISM_PROP.key(), String.valueOf(parallelism));
      return this;
    }

    public Builder withAsyncClean(boolean asyncClean) {
      props.setProperty(METADATA_ASYNC_CLEAN_PROP.key(), String.valueOf(asyncClean));
      return this;
    }

    public Builder withMaxNumDeltaCommitsBeforeCompaction(int maxNumDeltaCommitsBeforeCompaction) {
      props.setProperty(METADATA_COMPACT_NUM_DELTA_COMMITS_PROP.key(), String.valueOf(maxNumDeltaCommitsBeforeCompaction));
      return this;
    }

    public Builder archiveCommitsWith(int minToKeep, int maxToKeep) {
      props.setProperty(MIN_COMMITS_TO_KEEP_PROP.key(), String.valueOf(minToKeep));
      props.setProperty(MAX_COMMITS_TO_KEEP_PROP.key(), String.valueOf(maxToKeep));
      return this;
    }

    public Builder retainCommits(int commitsRetained) {
      props.setProperty(CLEANER_COMMITS_RETAINED_PROP.key(), String.valueOf(commitsRetained));
      return this;
    }

    public Builder withFileListingParallelism(int parallelism) {
      props.setProperty(FILE_LISTING_PARALLELISM_PROP.key(), String.valueOf(parallelism));
      return this;
    }

    public Builder withAssumeDatePartitioning(boolean assumeDatePartitioning) {
      props.setProperty(HOODIE_ASSUME_DATE_PARTITIONING_PROP.key(), String.valueOf(assumeDatePartitioning));
      return this;
    }

    public Builder withDirectoryFilterRegex(String regex) {
      props.setProperty(DIRECTORY_FILTER_REGEX.key(), regex);
      return this;
    }

    public HoodieMetadataConfig build() {
      HoodieMetadataConfig config = new HoodieMetadataConfig(props);
      setDefaultValue(props, METADATA_ENABLE_PROP);
      setDefaultValue(props, METADATA_METRICS_ENABLE_PROP);
      setDefaultValue(props, METADATA_VALIDATE_PROP);
      setDefaultValue(props, METADATA_INSERT_PARALLELISM_PROP);
      setDefaultValue(props, METADATA_ASYNC_CLEAN_PROP);
      setDefaultValue(props, METADATA_COMPACT_NUM_DELTA_COMMITS_PROP);
      setDefaultValue(props, CLEANER_COMMITS_RETAINED_PROP);
      setDefaultValue(props, MAX_COMMITS_TO_KEEP_PROP);
      setDefaultValue(props, MIN_COMMITS_TO_KEEP_PROP);
      setDefaultValue(props, FILE_LISTING_PARALLELISM_PROP);
      setDefaultValue(props, HOODIE_ASSUME_DATE_PARTITIONING_PROP);
      setDefaultValue(props, ENABLE_FALLBACK_PROP);
      setDefaultValue(props, DIRECTORY_FILTER_REGEX);
      return config;
    }
  }
}
