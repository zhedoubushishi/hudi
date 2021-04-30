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

import scala.collection.JavaConverters._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.common.config.{DefaultHoodieConfig, TypedProperties}

import scala.collection.JavaConversions.mapAsJavaMap
import scala.collection.JavaConverters.mapAsScalaMapConverter
import org.apache.hudi.common.config.HoodieMetadataConfig.METADATA_ENABLE_PROP
import org.apache.hudi.common.config.HoodieMetadataConfig.METADATA_VALIDATE_PROP
import org.apache.hudi.keygen.{BaseKeyGenerator, CustomAvroKeyGenerator, CustomKeyGenerator, KeyGenerator}

/**
 * WriterUtils to assist in write path in Datasource and tests.
 */
object HoodieWriterUtils {

  def javaParametersWithWriteDefaults(parameters: java.util.Map[String, String]): java.util.Map[String, String] = {
    mapAsJavaMap(parametersWithWriteDefaults(parameters.asScala.toMap))
  }

  /**
    * Add default options for unspecified write options keys.
    *
    * @param parameters
    * @return
    */
  def parametersWithWriteDefaults(parameters: Map[String, String]): Map[String, String] = {
    val javaMapParameters: java.util.HashMap[String, String] = new java.util.HashMap(parameters)
    DefaultHoodieConfig.setDefaultValue(javaMapParameters, OPERATION_OPT_KEY)
    DefaultHoodieConfig.setDefaultValue(javaMapParameters, TABLE_TYPE_OPT_KEY)
    DefaultHoodieConfig.setDefaultValue(javaMapParameters, PRECOMBINE_FIELD_OPT_KEY)
    DefaultHoodieConfig.setDefaultValue(javaMapParameters, PAYLOAD_CLASS_OPT_KEY)
    DefaultHoodieConfig.setDefaultValue(javaMapParameters, RECORDKEY_FIELD_OPT_KEY)
    DefaultHoodieConfig.setDefaultValue(javaMapParameters, PARTITIONPATH_FIELD_OPT_KEY)
    DefaultHoodieConfig.setDefaultValue(javaMapParameters, KEYGENERATOR_CLASS_OPT_KEY)
    DefaultHoodieConfig.setDefaultValue(javaMapParameters, METADATA_ENABLE_PROP)
    DefaultHoodieConfig.setDefaultValue(javaMapParameters, METADATA_VALIDATE_PROP)
    DefaultHoodieConfig.setDefaultValue(javaMapParameters, COMMIT_METADATA_KEYPREFIX_OPT_KEY)
    DefaultHoodieConfig.setDefaultValue(javaMapParameters, INSERT_DROP_DUPS_OPT_KEY)
    DefaultHoodieConfig.setDefaultValue(javaMapParameters, STREAMING_RETRY_CNT_OPT_KEY)
    DefaultHoodieConfig.setDefaultValue(javaMapParameters, STREAMING_RETRY_INTERVAL_MS_OPT_KEY)
    DefaultHoodieConfig.setDefaultValue(javaMapParameters, STREAMING_IGNORE_FAILED_BATCH_OPT_KEY)
    DefaultHoodieConfig.setDefaultValue(javaMapParameters, META_SYNC_CLIENT_TOOL_CLASS)
    DefaultHoodieConfig.setDefaultValue(javaMapParameters, HIVE_SYNC_ENABLED_OPT_KEY)
    DefaultHoodieConfig.setDefaultValue(javaMapParameters, META_SYNC_ENABLED_OPT_KEY)
    DefaultHoodieConfig.setDefaultValue(javaMapParameters, HIVE_DATABASE_OPT_KEY)
    DefaultHoodieConfig.setDefaultValue(javaMapParameters, HIVE_TABLE_OPT_KEY)
    DefaultHoodieConfig.setDefaultValue(javaMapParameters, HIVE_BASE_FILE_FORMAT_OPT_KEY)
    DefaultHoodieConfig.setDefaultValue(javaMapParameters, HIVE_USER_OPT_KEY)
    DefaultHoodieConfig.setDefaultValue(javaMapParameters, HIVE_PASS_OPT_KEY)
    DefaultHoodieConfig.setDefaultValue(javaMapParameters, HIVE_URL_OPT_KEY)
    DefaultHoodieConfig.setDefaultValue(javaMapParameters, HIVE_PARTITION_FIELDS_OPT_KEY)
    DefaultHoodieConfig.setDefaultValue(javaMapParameters, HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY)
    DefaultHoodieConfig.setDefaultValue(javaMapParameters, HIVE_STYLE_PARTITIONING_OPT_KEY)
    DefaultHoodieConfig.setDefaultValue(javaMapParameters, HIVE_USE_JDBC_OPT_KEY)
    DefaultHoodieConfig.setDefaultValue(javaMapParameters, ASYNC_COMPACT_ENABLE_OPT_KEY)
    DefaultHoodieConfig.setDefaultValue(javaMapParameters, ENABLE_ROW_WRITER_OPT_KEY)
    Map() ++ javaMapParameters.asScala ++ translateStorageTypeToTableType(parameters)
  }

  def toProperties(params: Map[String, String]): TypedProperties = {
    val props = new TypedProperties()
    params.foreach(kv => props.setProperty(kv._1, kv._2))
    props
  }

  /**
   * Get the partition columns to stored to hoodie.properties.
   * @param parameters
   * @return
   */
  def getPartitionColumns(parameters: Map[String, String]): String = {
    val props = new TypedProperties()
    props.putAll(parameters.asJava)
    val keyGen = DataSourceUtils.createKeyGenerator(props)
    getPartitionColumns(keyGen)
  }

  def getPartitionColumns(keyGen: KeyGenerator): String = {
    keyGen match {
      // For CustomKeyGenerator and CustomAvroKeyGenerator, the partition path filed format
      // is: "field_name: field_type", we extract the field_name from the partition path field.
      case c: BaseKeyGenerator
        if c.isInstanceOf[CustomKeyGenerator] || c.isInstanceOf[CustomAvroKeyGenerator] =>
          c.getPartitionPathFields.asScala.map(pathField =>
            pathField.split(CustomAvroKeyGenerator.SPLIT_REGEX)
                .headOption.getOrElse(s"Illegal partition path field format: '$pathField' for ${c.getClass.getSimpleName}"))
            .mkString(",")

      case b: BaseKeyGenerator => b.getPartitionPathFields.asScala.mkString(",")
      case _=> null
    }
  }
}
