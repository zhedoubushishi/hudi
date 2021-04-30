/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi

import org.apache.hudi.common.config.ConfigOption
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.model.WriteOperationType
import org.apache.hudi.common.util.Option
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.hive.{HiveStylePartitionValueExtractor, HiveSyncTool, MultiPartKeysValueExtractor,
  NonPartitionedExtractor, SlashEncodedDayPartitionValueExtractor}
import org.apache.hudi.keygen.{ComplexKeyGenerator, CustomKeyGenerator, NonpartitionedKeyGenerator, SimpleKeyGenerator}
import org.apache.hudi.keygen.constant.KeyGeneratorOptions
import org.apache.log4j.LogManager
import org.apache.spark.sql.execution.datasources.{DataSourceUtils => SparkDataSourceUtils}

import java.util.function.{Function => JavaFunction}

/**
  * List of options that can be passed to the Hoodie datasource,
  * in addition to the hoodie client configs
  */

/**
  * Options supported for reading hoodie tables.
  */
object DataSourceReadOptions {

  private val log = LogManager.getLogger(DataSourceReadOptions.getClass)

  /**
   * Whether data needs to be read, in
   *
   * 1) Snapshot mode (obtain latest view, based on row & columnar data)
   * 2) incremental mode (new data since an instantTime)
   * 3) Read Optimized mode (obtain latest view, based on columnar data)
   *
   * Default: snapshot
   */
  val QUERY_TYPE_SNAPSHOT_OPT_VAL = "snapshot"
  val QUERY_TYPE_READ_OPTIMIZED_OPT_VAL = "read_optimized"
  val QUERY_TYPE_INCREMENTAL_OPT_VAL = "incremental"
  val QUERY_TYPE_OPT_KEY: ConfigOption[String] = ConfigOption
    .key("hoodie.datasource.query.type")
    .defaultValue(QUERY_TYPE_SNAPSHOT_OPT_VAL)
    .withDeprecatedNames("hoodie.datasource.view.type")
    .withDescription("Whether data needs to be read, in incremental mode (new data since an instantTime) " +
      "(or) Read Optimized mode (obtain latest view, based on columnar data) (or) Snapshot mode " +
      "(obtain latest view, based on row & columnar data)")

  /**
   * For Snapshot query on merge on read table. Use this key to define the payload class.
   */
  val REALTIME_SKIP_MERGE_OPT_VAL = "skip_merge"
  val REALTIME_PAYLOAD_COMBINE_OPT_VAL = "payload_combine"
  val REALTIME_MERGE_OPT_KEY: ConfigOption[String] = ConfigOption
    .key("hoodie.datasource.merge.type")
    .defaultValue(REALTIME_PAYLOAD_COMBINE_OPT_VAL)
    .withDescription("")

  val READ_PATHS_OPT_KEY: ConfigOption[String] = ConfigOption
    .key("hoodie.datasource.read.paths")
    .noDefaultValue()
    .withDescription("")

  // TODO modify this when refactor HoodieWriteConfig
  val READ_PRE_COMBINE_FIELD = HoodieWriteConfig.PRECOMBINE_FIELD_PROP

  val ENABLE_HOODIE_FILE_INDEX: ConfigOption[Boolean] = ConfigOption
    .key("hoodie.file.index.enable")
    .defaultValue(true)
    .withDescription("")

  @Deprecated
  val VIEW_TYPE_OPT_KEY = "hoodie.datasource.view.type"
  @Deprecated
  val VIEW_TYPE_READ_OPTIMIZED_OPT_VAL = "read_optimized"
  @Deprecated
  val VIEW_TYPE_INCREMENTAL_OPT_VAL = "incremental"
  @Deprecated
  val VIEW_TYPE_REALTIME_OPT_VAL = "realtime"
  @Deprecated
  val DEFAULT_VIEW_TYPE_OPT_VAL = VIEW_TYPE_READ_OPTIMIZED_OPT_VAL

  /**
    * This eases migration from old configs to new configs.
    */
  def translateViewTypesToQueryTypes(optParams: Map[String, String]) : Map[String, String] = {
    val translation = Map(VIEW_TYPE_READ_OPTIMIZED_OPT_VAL -> QUERY_TYPE_SNAPSHOT_OPT_VAL,
                          VIEW_TYPE_INCREMENTAL_OPT_VAL -> QUERY_TYPE_INCREMENTAL_OPT_VAL,
                          VIEW_TYPE_REALTIME_OPT_VAL -> QUERY_TYPE_SNAPSHOT_OPT_VAL)
    if (!optParams.contains(QUERY_TYPE_OPT_KEY.key)) {
      if (optParams.contains(VIEW_TYPE_OPT_KEY)) {
        log.warn(VIEW_TYPE_OPT_KEY + " is deprecated and will be removed in a later release. Please use " + QUERY_TYPE_OPT_KEY)
        optParams ++ Map(QUERY_TYPE_OPT_KEY.key -> translation(optParams(VIEW_TYPE_OPT_KEY)))
      } else {
        optParams ++ Map(QUERY_TYPE_OPT_KEY.key -> QUERY_TYPE_OPT_KEY.defaultValue)
      }
    } else {
      optParams
    }
  }

  /**
    * Instant time to start incrementally pulling data from. The instanttime here need not
    * necessarily correspond to an instant on the timeline. New data written with an
    * `instant_time > BEGIN_INSTANTTIME` are fetched out. For e.g: '20170901080000' will get
    * all new data written after Sep 1, 2017 08:00AM.
    *
    * Default: None (Mandatory in incremental mode)
    */
  val BEGIN_INSTANTTIME_OPT_KEY: ConfigOption[String] = ConfigOption
    .key("hoodie.datasource.read.begin.instanttime")
    .noDefaultValue()
    .withDescription("Instant time to start incrementally pulling data from. The instanttime here need not necessarily " +
      "correspond to an instant on the timeline. New data written with an instant_time > BEGIN_INSTANTTIME are fetched out. " +
      "For e.g: ‘20170901080000’ will get all new data written after Sep 1, 2017 08:00AM.")

  /**
    * Instant time to limit incrementally fetched data to. New data written with an
    * `instant_time <= END_INSTANTTIME` are fetched out.
    *
    * Default: latest instant (i.e fetches all new data since begin instant time)
    *
    */
  val END_INSTANTTIME_OPT_KEY: ConfigOption[String] = ConfigOption
    .key("hoodie.datasource.read.end.instanttime")
    .noDefaultValue()
    .withDescription("Instant time to limit incrementally fetched data to. " +
      "New data written with an instant_time <= END_INSTANTTIME are fetched out.")

  /**
    * If use the end instant schema when incrementally fetched data to.
    *
    * Default: false (use latest instant schema)
    *
    */
  val INCREMENTAL_READ_SCHEMA_USE_END_INSTANTTIME_OPT_KEY: ConfigOption[String] = ConfigOption
    .key("hoodie.datasource.read.schema.use.end.instanttime")
    .defaultValue("false")
    .withDescription("Uses end instant schema when incrementally fetched data to. Default: users latest instant schema.")

  /**
    * For use-cases like DeltaStreamer which reads from Hoodie Incremental table and applies opaque map functions,
    * filters appearing late in the sequence of transformations cannot be automatically pushed down.
    * This option allows setting filters directly on Hoodie Source
    */
  val PUSH_DOWN_INCR_FILTERS_OPT_KEY: ConfigOption[String] = ConfigOption
    .key("hoodie.datasource.read.incr.filters")
    .defaultValue("")
    .withDescription("")

  /**
   * For the use-cases like users only want to incremental pull from certain partitions instead of the full table.
   * This option allows using glob pattern to directly filter on path.
   */
  val INCR_PATH_GLOB_OPT_KEY: ConfigOption[String] = ConfigOption
    .key("hoodie.datasource.read.incr.path.glob")
    .defaultValue("")
    .withDescription("")
}

/**
  * Options supported for writing hoodie tables.
  */
object DataSourceWriteOptions {

  private val log = LogManager.getLogger(DataSourceWriteOptions.getClass)

  /**
    * The write operation, that this write should do
    *
    * Default: upsert()
    */
  val BULK_INSERT_OPERATION_OPT_VAL = WriteOperationType.BULK_INSERT.value
  val INSERT_OPERATION_OPT_VAL = WriteOperationType.INSERT.value
  val UPSERT_OPERATION_OPT_VAL = WriteOperationType.UPSERT.value
  val DELETE_OPERATION_OPT_VAL = WriteOperationType.DELETE.value
  val BOOTSTRAP_OPERATION_OPT_VAL = WriteOperationType.BOOTSTRAP.value
  val INSERT_OVERWRITE_OPERATION_OPT_VAL = WriteOperationType.INSERT_OVERWRITE.value
  val INSERT_OVERWRITE_TABLE_OPERATION_OPT_VAL = WriteOperationType.INSERT_OVERWRITE_TABLE.value
  val OPERATION_OPT_KEY: ConfigOption[String] = ConfigOption
    .key("hoodie.datasource.write.operation")
    .defaultValue(UPSERT_OPERATION_OPT_VAL)
    .withDescription("Whether to do upsert, insert or bulkinsert for the write operation. " +
      "Use bulkinsert to load new data into a table, and there on use upsert/insert. " +
      "bulk insert uses a disk based write path to scale to load large inputs without need to cache it.")

  /**
    * The table type for the underlying data, for this write.
    * Note that this can't change across writes.
    *
    * Default: COPY_ON_WRITE
    */
  val COW_TABLE_TYPE_OPT_VAL = HoodieTableType.COPY_ON_WRITE.name
  val MOR_TABLE_TYPE_OPT_VAL = HoodieTableType.MERGE_ON_READ.name
  val TABLE_TYPE_OPT_KEY: ConfigOption[String] = ConfigOption
    .key("hoodie.datasource.write.table.type")
    .defaultValue(COW_TABLE_TYPE_OPT_VAL)
    .withDeprecatedNames("hoodie.datasource.write.storage.type")
    .withDescription("The table type for the underlying data, for this write. This can’t change between writes.")

  @Deprecated
  val STORAGE_TYPE_OPT_KEY = "hoodie.datasource.write.storage.type"
  @Deprecated
  val COW_STORAGE_TYPE_OPT_VAL = HoodieTableType.COPY_ON_WRITE.name
  @Deprecated
  val MOR_STORAGE_TYPE_OPT_VAL = HoodieTableType.MERGE_ON_READ.name
  @Deprecated
  val DEFAULT_STORAGE_TYPE_OPT_VAL = COW_STORAGE_TYPE_OPT_VAL

  def translateStorageTypeToTableType(optParams: Map[String, String]) : Map[String, String] = {
    if (optParams.contains(STORAGE_TYPE_OPT_KEY) && !optParams.contains(TABLE_TYPE_OPT_KEY.key)) {
      log.warn(STORAGE_TYPE_OPT_KEY + " is deprecated and will be removed in a later release; Please use " + TABLE_TYPE_OPT_KEY.key)
      optParams ++ Map(TABLE_TYPE_OPT_KEY.key -> optParams(STORAGE_TYPE_OPT_KEY))
    } else {
      optParams
    }
  }

  /**
    * Translate spark parameters to hudi parameters
    *
    * @param optParams Parameters to be translated
    * @return Parameters after translation
    */
  def translateSqlOptions(optParams: Map[String, String]): Map[String, String] = {
    var translatedOptParams = optParams
    // translate the api partitionBy of spark DataFrameWriter to PARTITIONPATH_FIELD_OPT_KEY
    if (optParams.contains(SparkDataSourceUtils.PARTITIONING_COLUMNS_KEY)) {
      val partitionColumns = optParams.get(SparkDataSourceUtils.PARTITIONING_COLUMNS_KEY)
        .map(SparkDataSourceUtils.decodePartitioningColumns)
        .getOrElse(Nil)
      val keyGeneratorClass = optParams.getOrElse(DataSourceWriteOptions.KEYGENERATOR_CLASS_OPT_KEY.key(),
        DataSourceWriteOptions.KEYGENERATOR_CLASS_OPT_KEY.defaultValue)

      val partitionPathField =
        keyGeneratorClass match {
          // Only CustomKeyGenerator needs special treatment, because it needs to be specified in a way
          // such as "field1:PartitionKeyType1,field2:PartitionKeyType2".
          // partitionBy can specify the partition like this: partitionBy("p1", "p2:SIMPLE", "p3:TIMESTAMP")
          case c if c == classOf[CustomKeyGenerator].getName =>
            partitionColumns.map(e => {
              if (e.contains(":")) {
                e
              } else {
                s"$e:SIMPLE"
              }
            }).mkString(",")
          case _ =>
            partitionColumns.mkString(",")
        }
      translatedOptParams = optParams ++ Map(PARTITIONPATH_FIELD_OPT_KEY.key -> partitionPathField)
    }
    translatedOptParams
  }

  /**
    * Hive table name, to register the table into.
    *
    * Default:  None (mandatory)
    */
  val TABLE_NAME_OPT_KEY: ConfigOption[String] = ConfigOption
    .key("hoodie.datasource.write.table.name")
    .noDefaultValue()
    .withDescription("Hive table name, to register the table into.")

  /**
    * Field used in preCombining before actual write. When two records have the same
    * key value, we will pick the one with the largest value for the precombine field,
    * determined by Object.compareTo(..)
    */
  val PRECOMBINE_FIELD_OPT_KEY = HoodieWriteConfig.PRECOMBINE_FIELD_PROP

  /**
    * Payload class used. Override this, if you like to roll your own merge logic, when upserting/inserting.
    * This will render any value set for `PRECOMBINE_FIELD_OPT_VAL` in-effective
    */
  val PAYLOAD_CLASS_OPT_KEY = HoodieWriteConfig.WRITE_PAYLOAD_CLASS

  /**
    * Record key field. Value to be used as the `recordKey` component of `HoodieKey`. Actual value
    * will be obtained by invoking .toString() on the field value. Nested fields can be specified using
    * the dot notation eg: `a.b.c`
    *
    */
  val RECORDKEY_FIELD_OPT_KEY = KeyGeneratorOptions.RECORDKEY_FIELD_OPT_KEY

  /**
    * Partition path field. Value to be used at the `partitionPath` component of `HoodieKey`. Actual
    * value obtained by invoking .toString()
    */
  val PARTITIONPATH_FIELD_OPT_KEY = KeyGeneratorOptions.PARTITIONPATH_FIELD_OPT_KEY

  /**
    * Flag to indicate whether to use Hive style partitioning.
    * If set true, the names of partition folders follow <partition_column_name>=<partition_value> format.
    * By default false (the names of partition folders are only partition values)
    */
  val HIVE_STYLE_PARTITIONING_OPT_KEY = KeyGeneratorOptions.HIVE_STYLE_PARTITIONING_OPT_KEY
  val URL_ENCODE_PARTITIONING_OPT_KEY = KeyGeneratorOptions.URL_ENCODE_PARTITIONING_OPT_KEY

  /**
    * Key generator class, that implements will extract the key out of incoming record
    *
    */
  val keyGeneraterInferFunc = DataSourceOptionsHelper.scalaFunctionToJavaFunction((p: java.util.Map[_, _]) => {
    if (!p.containsKey(PARTITIONPATH_FIELD_OPT_KEY.key)) {
      Option.of(classOf[NonpartitionedKeyGenerator].getName)
    } else {
      val numOfPartFields = p.get(PARTITIONPATH_FIELD_OPT_KEY.key).toString.split(",").length
      if (numOfPartFields == 1) {
        Option.of(classOf[SimpleKeyGenerator].getName)
      } else {
        Option.of(classOf[ComplexKeyGenerator].getName)
      }
    }
  })
  val KEYGENERATOR_CLASS_OPT_KEY: ConfigOption[String] = ConfigOption
    .key("hoodie.datasource.write.keygenerator.class")
    .defaultValue(classOf[SimpleKeyGenerator].getName)
    .withInferFunction(keyGeneraterInferFunc)
    .withDescription("Key generator class, that implements will extract the key out of incoming Row object")

  /**
   * When set to true, will perform write operations directly using the spark native `Row` representation.
   * By default, false (will be enabled as default in a future release)
   */
  val ENABLE_ROW_WRITER_OPT_KEY: ConfigOption[String] = ConfigOption
    .key("hoodie.datasource.write.row.writer.enable")
    .defaultValue("false")
    .withDescription("")

  /**
    * Option keys beginning with this prefix, are automatically added to the commit/deltacommit metadata.
    * This is useful to store checkpointing information, in a consistent way with the hoodie timeline
    */
  val COMMIT_METADATA_KEYPREFIX_OPT_KEY: ConfigOption[String] = ConfigOption
    .key("hoodie.datasource.write.commitmeta.key.prefix")
    .defaultValue("_")
    .withDescription("Option keys beginning with this prefix, are automatically added to the commit/deltacommit metadata. " +
      "This is useful to store checkpointing information, in a consistent way with the hudi timeline")

  /**
    * Flag to indicate whether to drop duplicates upon insert.
    * By default insert will accept duplicates, to gain extra performance.
    */
  val INSERT_DROP_DUPS_OPT_KEY: ConfigOption[String] = ConfigOption
    .key("hoodie.datasource.write.insert.drop.duplicates")
    .defaultValue("false")
    .withDescription("If set to true, filters out all duplicate records from incoming dataframe, during insert operations.")

  /**
    * Flag to indicate how many times streaming job should retry for a failed microbatch
    * By default 3
    */
  val STREAMING_RETRY_CNT_OPT_KEY: ConfigOption[String] = ConfigOption
    .key("hoodie.datasource.write.streaming.retry.count")
    .defaultValue("3")
    .withDescription("")

  /**
    * Flag to indicate how long (by millisecond) before a retry should issued for failed microbatch
    * By default 2000 and it will be doubled by every retry
    */
  val STREAMING_RETRY_INTERVAL_MS_OPT_KEY: ConfigOption[String] = ConfigOption
    .key("hoodie.datasource.write.streaming.retry.interval.ms")
    .defaultValue("2000")
    .withDescription("")

  /**
    * Flag to indicate whether to ignore any non exception error (e.g. writestatus error)
    * within a streaming microbatch
    * By default true (in favor of streaming progressing over data integrity)
    */
  val STREAMING_IGNORE_FAILED_BATCH_OPT_KEY: ConfigOption[String] = ConfigOption
    .key("hoodie.datasource.write.streaming.ignore.failed.batch")
    .defaultValue("true")
    .withDescription("")

  val META_SYNC_CLIENT_TOOL_CLASS: ConfigOption[String] = ConfigOption
    .key("hoodie.meta.sync.client.tool.class")
    .defaultValue(classOf[HiveSyncTool].getName)
    .withDescription("")

  // HIVE SYNC SPECIFIC CONFIGS
  // NOTE: DO NOT USE uppercase for the keys as they are internally lower-cased. Using upper-cases causes
  // unexpected issues with config getting reset

  val HIVE_SYNC_ENABLED_OPT_KEY: ConfigOption[String] = ConfigOption
    .key("hoodie.datasource.hive_sync.enable")
    .defaultValue("false")
    .withDescription("When set to true, register/sync the table to Apache Hive metastore")

  val META_SYNC_ENABLED_OPT_KEY: ConfigOption[String] = ConfigOption
    .key("hoodie.datasource.meta.sync.enable")
    .defaultValue("false")
    .withDescription("")

  val HIVE_DATABASE_OPT_KEY: ConfigOption[String] = ConfigOption
    .key("hoodie.datasource.hive_sync.database")
    .defaultValue("default")
    .withDescription("database to sync to")

  val hiveTableOptKeyInferFunc = DataSourceOptionsHelper.scalaFunctionToJavaFunction((p: java.util.Map[_, _]) => {
    if (p.containsKey(TABLE_NAME_OPT_KEY.key)) {
      Option.of(p.get(TABLE_NAME_OPT_KEY.key).toString)
    } else if (p.containsKey(HoodieWriteConfig.TABLE_NAME.key)) {
      Option.of(p.get(HoodieWriteConfig.TABLE_NAME.key).toString)
    } else {
      Option.empty[String]()
    }
  })
  val HIVE_TABLE_OPT_KEY: ConfigOption[String] = ConfigOption
    .key("hoodie.datasource.hive_sync.table")
    .defaultValue("unknown")
    .withInferFunction(hiveTableOptKeyInferFunc)
    .withDescription("table to sync to")

  val HIVE_BASE_FILE_FORMAT_OPT_KEY: ConfigOption[String] = ConfigOption
    .key("hoodie.datasource.hive_sync.base_file_format")
    .defaultValue("PARQUET")
    .withDescription("")

  val HIVE_USER_OPT_KEY: ConfigOption[String] = ConfigOption
    .key("hoodie.datasource.hive_sync.username")
    .defaultValue("hive")
    .withDescription("hive user name to use")

  val HIVE_PASS_OPT_KEY: ConfigOption[String] = ConfigOption
    .key("hoodie.datasource.hive_sync.password")
    .defaultValue("hive")
    .withDescription("hive password to use")

  val HIVE_URL_OPT_KEY: ConfigOption[String] = ConfigOption
    .key("hoodie.datasource.hive_sync.jdbcurl")
    .defaultValue("jdbc:hive2://localhost:10000")
    .withDescription("Hive metastore url")

  val hivePartitionFieldsInferFunc = DataSourceOptionsHelper.scalaFunctionToJavaFunction((p: java.util.Map[_, _]) => {
    if (p.containsKey(PARTITIONPATH_FIELD_OPT_KEY.key)) {
      Option.of(p.get(PARTITIONPATH_FIELD_OPT_KEY.key).toString)
    } else {
      Option.empty[String]()
    }
  })
  val HIVE_PARTITION_FIELDS_OPT_KEY: ConfigOption[String] = ConfigOption
    .key("hoodie.datasource.hive_sync.partition_fields")
    .defaultValue("")
    .withInferFunction(hivePartitionFieldsInferFunc)
    .withDescription("field in the table to use for determining hive partition columns.")

  val hivePartitionExtractorInferFunc = DataSourceOptionsHelper.scalaFunctionToJavaFunction((p: java.util.Map[_, _]) => {
    if (!p.containsKey(PARTITIONPATH_FIELD_OPT_KEY.key)) {
      Option.of(classOf[NonPartitionedExtractor].getName)
    } else {
      val numOfPartFields = p.get(PARTITIONPATH_FIELD_OPT_KEY.key).toString.split(",").length
      if (numOfPartFields == 1 && p.containsKey(HIVE_STYLE_PARTITIONING_OPT_KEY.key) && p.get(HIVE_STYLE_PARTITIONING_OPT_KEY.key).toString == "true") {
        Option.of(classOf[HiveStylePartitionValueExtractor].getName)
      } else {
        Option.of(classOf[MultiPartKeysValueExtractor].getName)
      }
    }
  })
  val HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY: ConfigOption[String] = ConfigOption
    .key("hoodie.datasource.hive_sync.partition_extractor_class")
    .defaultValue(classOf[SlashEncodedDayPartitionValueExtractor].getCanonicalName)
    .withInferFunction(hivePartitionExtractorInferFunc)
    .withDescription("")

  val HIVE_ASSUME_DATE_PARTITION_OPT_KEY: ConfigOption[String] = ConfigOption
    .key("hoodie.datasource.hive_sync.assume_date_partitioning")
    .defaultValue("false")
    .withDescription("Assume partitioning is yyyy/mm/dd")

  val HIVE_USE_PRE_APACHE_INPUT_FORMAT_OPT_KEY: ConfigOption[String] = ConfigOption
    .key("hoodie.datasource.hive_sync.use_pre_apache_input_format")
    .defaultValue("false")
    .withDescription("")

  val HIVE_USE_JDBC_OPT_KEY: ConfigOption[String] = ConfigOption
    .key("hoodie.datasource.hive_sync.use_jdbc")
    .defaultValue("true")
    .withDescription("Use JDBC when hive synchronization is enabled")

  val HIVE_AUTO_CREATE_DATABASE_OPT_KEY: ConfigOption[String] = ConfigOption
    .key("hoodie.datasource.hive_sync.auto_create_database")
    .defaultValue("true")
    .withDescription("Auto create hive database if does not exists")

  val HIVE_IGNORE_EXCEPTIONS_OPT_KEY: ConfigOption[String] = ConfigOption
    .key("hoodie.datasource.hive_sync.ignore_exceptions")
    .defaultValue("false")
    .withDescription("")

  val HIVE_SKIP_RO_SUFFIX: ConfigOption[String] = ConfigOption
    .key("hoodie.datasource.hive_sync.skip_ro_suffix")
    .defaultValue("false")
    .withDescription("Skip the _ro suffix for Read optimized table, when registering")

  val HIVE_SUPPORT_TIMESTAMP: ConfigOption[String] = ConfigOption
    .key("hoodie.datasource.hive_sync.support_timestamp")
    .defaultValue("false")
    .withDescription("‘INT64’ with original type TIMESTAMP_MICROS is converted to hive ‘timestamp’ type. " +
      "Disabled by default for backward compatibility.")

  val HIVE_TABLE_PROPERTIES: ConfigOption[String] = ConfigOption
    .key("hoodie.datasource.hive_sync.table_properties")
    .noDefaultValue()
    .withDescription("")

  val HIVE_TABLE_SERDE_PROPERTIES: ConfigOption[String] = ConfigOption
    .key("hoodie.datasource.hive_sync.serde_properties")
    .noDefaultValue()
    .withDescription("")

  val HIVE_SYNC_AS_DATA_SOURCE_TABLE: ConfigOption[String] = ConfigOption
    .key("hoodie.datasource.hive_sync.sync_as_datasource")
    .defaultValue("true")
    .withDescription("")

  // Async Compaction - Enabled by default for MOR
  val ASYNC_COMPACT_ENABLE_OPT_KEY: ConfigOption[String] = ConfigOption
    .key("hoodie.datasource.compaction.async.enable")
    .defaultValue("true")
    .withDescription("")

  // Avro Kafka Source configs
  val KAFKA_AVRO_VALUE_DESERIALIZER: ConfigOption[String] = ConfigOption
    .key("hoodie.deltastreamer.source.kafka.value.deserializer.class")
    .noDefaultValue()
    .withDescription("")

  // Schema provider class to be set to be used in custom kakfa deserializer
  val SCHEMA_PROVIDER_CLASS_PROP: ConfigOption[String] = ConfigOption
    .key("hoodie.deltastreamer.schemaprovider.class")
    .noDefaultValue()
    .withDescription("")

}

object DataSourceOptionsHelper {
  implicit def scalaFunctionToJavaFunction[From, To](function: (From) => To): JavaFunction[From, To] = {
    new JavaFunction[From, To] {
      override def apply (input: From): To = function (input)
    }
  }
}