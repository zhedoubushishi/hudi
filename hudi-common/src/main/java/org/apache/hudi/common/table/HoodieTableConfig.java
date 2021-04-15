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

package org.apache.hudi.common.table;

import org.apache.hudi.common.bootstrap.index.HFileBootstrapIndex;
import org.apache.hudi.common.bootstrap.index.NoOpBootstrapIndex;
import org.apache.hudi.common.config.ConfigOption;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Configurations on the Hoodie Table like type of ingestion, storage formats, hive table name etc Configurations are loaded from hoodie.properties, these properties are usually set during
 * initializing a path as hoodie base path and never changes during the lifetime of a hoodie table.
 *
 * @see HoodieTableMetaClient
 * @since 0.3.0
 */
public class HoodieTableConfig extends HoodieConfig implements Serializable {

  private static final Logger LOG = LogManager.getLogger(HoodieTableConfig.class);

  public static final String HOODIE_PROPERTIES_FILE = "hoodie.properties";

  public static final ConfigOption<String> HOODIE_TABLE_NAME_PROP_NAME = ConfigOption
      .key("hoodie.table.name")
      .noDefaultValue()
      .withDescription("Table name that will be used for registering with Hive. Needs to be same across runs.");

  public static final ConfigOption<HoodieTableType> HOODIE_TABLE_TYPE_PROP_NAME = ConfigOption
      .key("hoodie.table.type")
      .defaultValue(HoodieTableType.COPY_ON_WRITE)
      .withDescription("The table type for the underlying data, for this write. This can’t change between writes.");

  public static final ConfigOption<HoodieTableVersion> HOODIE_TABLE_VERSION_PROP_NAME = ConfigOption
      .key("hoodie.table.version")
      .defaultValue(HoodieTableVersion.ZERO)
      .withDescription("");

  public static final ConfigOption<String> HOODIE_TABLE_PRECOMBINE_FIELD = ConfigOption
      .key("hoodie.table.precombine.field")
      .noDefaultValue()
      .withDescription("Field used in preCombining before actual write. When two records have the same key value, "
          + "we will pick the one with the largest value for the precombine field, determined by Object.compareTo(..)");

  public static final ConfigOption<String> HOODIE_TABLE_PARTITION_COLUMNS = ConfigOption
      .key("hoodie.table.partition.columns")
      .noDefaultValue()
      .withDescription("Partition path field. Value to be used at the partitionPath component of HoodieKey. "
          + "Actual value ontained by invoking .toString()");

  public static final ConfigOption<HoodieFileFormat> HOODIE_BASE_FILE_FORMAT_PROP_NAME = ConfigOption
      .key("hoodie.table.base.file.format")
      .defaultValue(HoodieFileFormat.PARQUET)
      .withAlternatives("hoodie.table.ro.file.format")
      .withDescription("");

  public static final ConfigOption<HoodieFileFormat> HOODIE_LOG_FILE_FORMAT_PROP_NAME = ConfigOption
      .key("hoodie.table.log.file.format")
      .defaultValue(HoodieFileFormat.HOODIE_LOG)
      .withAlternatives("hoodie.table.rt.file.format")
      .withDescription("");

  public static final ConfigOption<String> HOODIE_TIMELINE_LAYOUT_VERSION = ConfigOption
      .key("hoodie.timeline.layout.version")
      .noDefaultValue()
      .withDescription("");

  public static final ConfigOption<String> HOODIE_PAYLOAD_CLASS_PROP_NAME = ConfigOption
      .key("hoodie.compaction.payload.class")
      .defaultValue(OverwriteWithLatestAvroPayload.class.getName())
      .withDescription("");

  public static final ConfigOption<String> HOODIE_ARCHIVELOG_FOLDER_PROP_NAME = ConfigOption
      .key("hoodie.archivelog.folder")
      .defaultValue("archived")
      .withDescription("");

  public static final ConfigOption<String> HOODIE_BOOTSTRAP_INDEX_ENABLE = ConfigOption
      .key("hoodie.bootstrap.index.enable")
      .noDefaultValue()
      .withDescription("");

  public static final ConfigOption<String> HOODIE_BOOTSTRAP_INDEX_CLASS_PROP_NAME = ConfigOption
      .key("hoodie.bootstrap.index.class")
      .defaultValue(HFileBootstrapIndex.class.getName())
      .withDescription("");

  public static final ConfigOption<String> HOODIE_BOOTSTRAP_BASE_PATH = ConfigOption
      .key("hoodie.bootstrap.base.path")
      .noDefaultValue()
      .withDescription("Base path of the dataset that needs to be bootstrapped as a Hudi table");

  public static final String NO_OP_BOOTSTRAP_INDEX_CLASS = NoOpBootstrapIndex.class.getName();

  public HoodieTableConfig(FileSystem fs, String metaPath, String payloadClassName) {
    super();
    Properties props = new Properties();
    Path propertyPath = new Path(metaPath, HOODIE_PROPERTIES_FILE);
    LOG.info("Loading table properties from " + propertyPath);
    try {
      try (FSDataInputStream inputStream = fs.open(propertyPath)) {
        props.load(inputStream);
      }
      if (contains(props, HOODIE_PAYLOAD_CLASS_PROP_NAME) && payloadClassName != null
          && !getString(props, HOODIE_PAYLOAD_CLASS_PROP_NAME).equals(payloadClassName)) {
        set(props, HOODIE_PAYLOAD_CLASS_PROP_NAME, payloadClassName);
        try (FSDataOutputStream outputStream = fs.create(propertyPath)) {
          props.store(outputStream, "Properties saved on " + new Date(System.currentTimeMillis()));
        }
      }
    } catch (IOException e) {
      throw new HoodieIOException("Could not load Hoodie properties from " + propertyPath, e);
    }
    this.props = props;
    ValidationUtils.checkArgument(props.containsKey(HOODIE_TABLE_TYPE_PROP_NAME.key()) && props.containsKey(HOODIE_TABLE_NAME_PROP_NAME.key()),
        "hoodie.properties file seems invalid. Please check for left over `.updated` files if any, manually copy it to hoodie.properties and retry");
  }

  public HoodieTableConfig(Properties props) {
    super(props);
  }

  /**
   * For serializing and de-serializing.
   *
   * @deprecated
   */
  public HoodieTableConfig() {
  }

  /**
   * Initialize the hoodie meta directory and any necessary files inside the meta (including the hoodie.properties).
   */
  public static void createHoodieProperties(FileSystem fs, Path metadataFolder, Properties properties)
      throws IOException {
    if (!fs.exists(metadataFolder)) {
      fs.mkdirs(metadataFolder);
    }
    Path propertyPath = new Path(metadataFolder, HOODIE_PROPERTIES_FILE);
    try (FSDataOutputStream outputStream = fs.create(propertyPath)) {
      if (!contains(properties, HOODIE_TABLE_NAME_PROP_NAME)) {
        throw new IllegalArgumentException(HOODIE_TABLE_NAME_PROP_NAME.key() + " property needs to be specified");
      }
      setDefaultValue(properties, HOODIE_TABLE_TYPE_PROP_NAME);
      if (getString(properties, HOODIE_TABLE_TYPE_PROP_NAME).equals(HoodieTableType.MERGE_ON_READ.name())) {
        setDefaultValue(properties, HOODIE_PAYLOAD_CLASS_PROP_NAME);
      }
      setDefaultValue(properties, HOODIE_ARCHIVELOG_FOLDER_PROP_NAME);
      if (!contains(properties, HOODIE_TIMELINE_LAYOUT_VERSION)) {
        // Use latest Version as default unless forced by client
        set(properties, HOODIE_TIMELINE_LAYOUT_VERSION, TimelineLayoutVersion.CURR_VERSION.toString());
      }
      if (contains(properties, HOODIE_BOOTSTRAP_BASE_PATH)) {
        // Use the default bootstrap index class.
        properties.setProperty(HOODIE_BOOTSTRAP_INDEX_CLASS_PROP_NAME.key(), getDefaultBootstrapIndexClass(properties));
      }
      properties.store(outputStream, "Properties saved on " + new Date(System.currentTimeMillis()));
    }
  }

  /**
   * Read the table type from the table properties and if not found, return the default.
   */
  public HoodieTableType getTableType() {
    return HoodieTableType.valueOf(getStringOrDefault(props, HOODIE_TABLE_TYPE_PROP_NAME));
  }

  public Option<TimelineLayoutVersion> getTimelineLayoutVersion() {
    return contains(props, HOODIE_TIMELINE_LAYOUT_VERSION)
        ? Option.of(new TimelineLayoutVersion(getInt(props, HOODIE_TIMELINE_LAYOUT_VERSION)))
        : Option.empty();
  }

  /**
   * @return the hoodie.table.version from hoodie.properties file.
   */
  public HoodieTableVersion getTableVersion() {
    return contains(props, HOODIE_TABLE_VERSION_PROP_NAME)
        ? HoodieTableVersion.versionFromCode(getInt(props, HOODIE_TABLE_VERSION_PROP_NAME))
        : HOODIE_TABLE_VERSION_PROP_NAME.defaultValue();
  }

  public void setTableVersion(HoodieTableVersion tableVersion) {
    set(props, HOODIE_TABLE_VERSION_PROP_NAME, Integer.toString(tableVersion.versionCode()));
  }

  /**
   * Read the payload class for HoodieRecords from the table properties.
   */
  public String getPayloadClass() {
    // There could be tables written with payload class from com.uber.hoodie. Need to transparently
    // change to org.apache.hudi
    return getStringOrDefault(props, HOODIE_PAYLOAD_CLASS_PROP_NAME).replace("com.uber.hoodie",
        "org.apache.hudi");
  }

  public String getPreCombineField() {
    return getString(props, HOODIE_TABLE_PRECOMBINE_FIELD);
  }

  public Option<String[]> getPartitionColumns() {
    if (contains(props, HOODIE_TABLE_PARTITION_COLUMNS)) {
      return Option.of(Arrays.stream(getString(props, HOODIE_TABLE_PARTITION_COLUMNS).split(","))
        .filter(p -> p.length() > 0).collect(Collectors.toList()).toArray(new String[]{}));
    }
    return Option.empty();
  }

  /**
   * Read the payload class for HoodieRecords from the table properties.
   */
  public String getBootstrapIndexClass() {
    // There could be tables written with payload class from com.uber.hoodie. Need to transparently
    // change to org.apache.hudi
    return getStringOrDefault(props, HOODIE_BOOTSTRAP_INDEX_CLASS_PROP_NAME, getDefaultBootstrapIndexClass(props));
  }

  public static String getDefaultBootstrapIndexClass(Properties props) {
    String defaultClass = HOODIE_BOOTSTRAP_INDEX_CLASS_PROP_NAME.defaultValue();
    if ("false".equalsIgnoreCase(props.getProperty(HOODIE_BOOTSTRAP_INDEX_ENABLE.key()))) {
      defaultClass = NO_OP_BOOTSTRAP_INDEX_CLASS;
    }
    return defaultClass;
  }

  public Option<String> getBootstrapBasePath() {
    return Option.ofNullable(getString(props, HOODIE_BOOTSTRAP_BASE_PATH));
  }

  /**
   * Read the table name.
   */
  public String getTableName() {
    return getString(props, HOODIE_TABLE_NAME_PROP_NAME);
  }

  /**
   * Get the base file storage format.
   *
   * @return HoodieFileFormat for the base file Storage format
   */
  public HoodieFileFormat getBaseFileFormat() {
    return HoodieFileFormat.valueOf(getStringOrDefault(props, HOODIE_BASE_FILE_FORMAT_PROP_NAME));
  }

  /**
   * Get the log Storage Format.
   *
   * @return HoodieFileFormat for the log Storage format
   */
  public HoodieFileFormat getLogFileFormat() {
    return HoodieFileFormat.valueOf(getStringOrDefault(props, HOODIE_LOG_FILE_FORMAT_PROP_NAME));
  }

  /**
   * Get the relative path of archive log folder under metafolder, for this table.
   */
  public String getArchivelogFolder() {
    return getStringOrDefault(props, HOODIE_ARCHIVELOG_FOLDER_PROP_NAME);
  }

  public Map<String, String> getMapProps() {
    return props.entrySet().stream()
        .collect(Collectors.toMap(e -> String.valueOf(e.getKey()), e -> String.valueOf(e.getValue())));
  }

  public Properties getProperties() {
    return props;
  }
}
