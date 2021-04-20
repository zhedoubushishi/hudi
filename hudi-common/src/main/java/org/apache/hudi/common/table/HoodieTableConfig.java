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
import org.apache.hudi.common.config.DefaultHoodieConfig;
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
public class HoodieTableConfig implements Serializable {

  private static final Logger LOG = LogManager.getLogger(HoodieTableConfig.class);

  public static final String HOODIE_PROPERTIES_FILE = "hoodie.properties";

  public static final ConfigOption<String> HOODIE_TABLE_NAME_PROP_NAME = ConfigOption
      .key("hoodie.table.name")
      .noDefaultValue()
      .withDescription("");

  public static final ConfigOption<HoodieTableType> HOODIE_TABLE_TYPE_PROP_NAME = ConfigOption
      .key("hoodie.table.type")
      .defaultValue(HoodieTableType.COPY_ON_WRITE)
      .withDescription("");

  public static final ConfigOption<HoodieTableVersion> HOODIE_TABLE_VERSION_PROP_NAME = ConfigOption
      .key("hoodie.table.version")
      .defaultValue(HoodieTableVersion.ZERO)
      .withDescription("");

  public static final ConfigOption<String> HOODIE_TABLE_PRECOMBINE_FIELD = ConfigOption
      .key("hoodie.table.precombine.field")
      .noDefaultValue()
      .withDescription("");

  public static final ConfigOption<String> HOODIE_TABLE_PARTITION_COLUMNS = ConfigOption
      .key("hoodie.table.partition.columns")
      .noDefaultValue()
      .withDescription("");

  public static final ConfigOption<HoodieFileFormat> HOODIE_BASE_FILE_FORMAT_PROP_NAME = ConfigOption
      .key("hoodie.table.base.file.format")
      .defaultValue(HoodieFileFormat.PARQUET)
      .withDeprecatedNames("hoodie.table.ro.file.format")
      .withDescription("");

  public static final ConfigOption<HoodieFileFormat> HOODIE_LOG_FILE_FORMAT_PROP_NAME = ConfigOption
      .key("hoodie.table.log.file.format")
      .defaultValue(HoodieFileFormat.HOODIE_LOG)
      .withDeprecatedNames("hoodie.table.rt.file.format")
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
      .defaultValue("")
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
      .withDescription("");

  public static final String NO_OP_BOOTSTRAP_INDEX_CLASS = NoOpBootstrapIndex.class.getName();

  private Properties props;

  public HoodieTableConfig(FileSystem fs, String metaPath, String payloadClassName) {
    Properties props = new Properties();
    Path propertyPath = new Path(metaPath, HOODIE_PROPERTIES_FILE);
    LOG.info("Loading table properties from " + propertyPath);
    try {
      try (FSDataInputStream inputStream = fs.open(propertyPath)) {
        props.load(inputStream);
      }
      if (props.containsKey(HOODIE_PAYLOAD_CLASS_PROP_NAME.key()) && payloadClassName != null
          && !props.getProperty(HOODIE_PAYLOAD_CLASS_PROP_NAME.key()).equals(payloadClassName)) {
        props.setProperty(HOODIE_PAYLOAD_CLASS_PROP_NAME.key(), payloadClassName);
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
    this.props = props;
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
      if (!DefaultHoodieConfig.contains(properties, HOODIE_TABLE_NAME_PROP_NAME)) {
        throw new IllegalArgumentException(HOODIE_TABLE_NAME_PROP_NAME.key() + " property needs to be specified");
      }
      if (!DefaultHoodieConfig.contains(properties, HOODIE_TABLE_TYPE_PROP_NAME)) {
        properties.setProperty(HOODIE_TABLE_TYPE_PROP_NAME.key(), HOODIE_TABLE_TYPE_PROP_NAME.defaultValue().name());
      }
      if (properties.getProperty(HOODIE_TABLE_TYPE_PROP_NAME.key()).equals(HoodieTableType.MERGE_ON_READ.name())
          && !DefaultHoodieConfig.contains(properties, HOODIE_PAYLOAD_CLASS_PROP_NAME)) {
        properties.setProperty(HOODIE_PAYLOAD_CLASS_PROP_NAME.key(), HOODIE_PAYLOAD_CLASS_PROP_NAME.defaultValue());
      }
      if (!DefaultHoodieConfig.contains(properties, HOODIE_ARCHIVELOG_FOLDER_PROP_NAME)) {
        properties.setProperty(HOODIE_ARCHIVELOG_FOLDER_PROP_NAME.key(), HOODIE_ARCHIVELOG_FOLDER_PROP_NAME.defaultValue());
      }
      if (!DefaultHoodieConfig.contains(properties, HOODIE_TIMELINE_LAYOUT_VERSION)) {
        // Use latest Version as default unless forced by client
        properties.setProperty(HOODIE_TIMELINE_LAYOUT_VERSION.key(), TimelineLayoutVersion.CURR_VERSION.toString());
      }
      if (DefaultHoodieConfig.contains(properties, HOODIE_BOOTSTRAP_BASE_PATH) && !DefaultHoodieConfig.contains(properties, HOODIE_BOOTSTRAP_INDEX_CLASS_PROP_NAME)) {
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
    if (props.containsKey(HOODIE_TABLE_TYPE_PROP_NAME.key())) {
      return HoodieTableType.valueOf(props.getProperty(HOODIE_TABLE_TYPE_PROP_NAME.key()));
    }
    return HOODIE_TABLE_TYPE_PROP_NAME.defaultValue();
  }

  public Option<TimelineLayoutVersion> getTimelineLayoutVersion() {
    return props.containsKey(HOODIE_TIMELINE_LAYOUT_VERSION.key())
        ? Option.of(new TimelineLayoutVersion(Integer.valueOf(props.getProperty(HOODIE_TIMELINE_LAYOUT_VERSION.key()))))
        : Option.empty();
  }

  /**
   * @return the hoodie.table.version from hoodie.properties file.
   */
  public HoodieTableVersion getTableVersion() {
    return props.containsKey(HOODIE_TABLE_VERSION_PROP_NAME.key())
        ? HoodieTableVersion.versionFromCode(Integer.parseInt(props.getProperty(HOODIE_TABLE_VERSION_PROP_NAME.key())))
        : HOODIE_TABLE_VERSION_PROP_NAME.defaultValue();
  }

  public void setTableVersion(HoodieTableVersion tableVersion) {
    props.put(HOODIE_TABLE_VERSION_PROP_NAME.key(), Integer.toString(tableVersion.versionCode()));
  }

  /**
   * Read the payload class for HoodieRecords from the table properties.
   */
  public String getPayloadClass() {
    // There could be tables written with payload class from com.uber.hoodie. Need to transparently
    // change to org.apache.hudi
    return props.getProperty(HOODIE_PAYLOAD_CLASS_PROP_NAME.key(), HOODIE_PAYLOAD_CLASS_PROP_NAME.defaultValue()).replace("com.uber.hoodie",
        "org.apache.hudi");
  }

  public String getPreCombineField() {
    return props.getProperty(HOODIE_TABLE_PRECOMBINE_FIELD.key());
  }

  public Option<String[]> getPartitionColumns() {
    if (props.containsKey(HOODIE_TABLE_PARTITION_COLUMNS.key())) {
      return Option.of(Arrays.stream(props.getProperty(HOODIE_TABLE_PARTITION_COLUMNS.key()).split(","))
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
    return props.getProperty(HOODIE_BOOTSTRAP_INDEX_CLASS_PROP_NAME.key(), getDefaultBootstrapIndexClass(props));
  }

  public static String getDefaultBootstrapIndexClass(Properties props) {
    String defaultClass = HOODIE_BOOTSTRAP_INDEX_CLASS_PROP_NAME.defaultValue();
    if ("false".equalsIgnoreCase(props.getProperty(HOODIE_BOOTSTRAP_INDEX_ENABLE.key()))) {
      defaultClass = NO_OP_BOOTSTRAP_INDEX_CLASS;
    }
    return defaultClass;
  }

  public Option<String> getBootstrapBasePath() {
    return Option.ofNullable(props.getProperty(HOODIE_BOOTSTRAP_BASE_PATH.key()));
  }

  /**
   * Read the table name.
   */
  public String getTableName() {
    return props.getProperty(HOODIE_TABLE_NAME_PROP_NAME.key());
  }

  /**
   * Get the base file storage format.
   *
   * @return HoodieFileFormat for the base file Storage format
   */
  public HoodieFileFormat getBaseFileFormat() {
    return HoodieFileFormat.valueOf(DefaultHoodieConfig.getString(props, HOODIE_BASE_FILE_FORMAT_PROP_NAME));
  }

  /**
   * Get the log Storage Format.
   *
   * @return HoodieFileFormat for the log Storage format
   */
  public HoodieFileFormat getLogFileFormat() {
    return HoodieFileFormat.valueOf(DefaultHoodieConfig.getString(props, HOODIE_LOG_FILE_FORMAT_PROP_NAME));
  }

  /**
   * Get the relative path of archive log folder under metafolder, for this table.
   */
  public String getArchivelogFolder() {
    return DefaultHoodieConfig.getString(props, HOODIE_ARCHIVELOG_FOLDER_PROP_NAME);
  }

  public Map<String, String> getProps() {
    return props.entrySet().stream()
        .collect(Collectors.toMap(e -> String.valueOf(e.getKey()), e -> String.valueOf(e.getValue())));
  }

  public Properties getProperties() {
    return props;
  }
}
