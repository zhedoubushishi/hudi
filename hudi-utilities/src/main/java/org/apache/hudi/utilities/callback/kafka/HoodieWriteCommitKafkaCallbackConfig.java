/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.utilities.callback.kafka;

import org.apache.hudi.common.config.ConfigOption;

import java.util.Properties;

import static org.apache.hudi.common.config.DefaultHoodieConfig.setDefaultValue;
import static org.apache.hudi.config.HoodieWriteCommitCallbackConfig.CALLBACK_PREFIX;

/**
 * Kafka write callback related config.
 */
public class HoodieWriteCommitKafkaCallbackConfig {

  public static final ConfigOption<String> CALLBACK_KAFKA_BOOTSTRAP_SERVERS = ConfigOption
      .key(CALLBACK_PREFIX + "kafka.bootstrap.servers")
      .noDefaultValue()
      .withDescription("Bootstrap servers of kafka callback cluster");

  public static final ConfigOption<String> CALLBACK_KAFKA_TOPIC = ConfigOption
      .key(CALLBACK_PREFIX + "kafka.topic")
      .noDefaultValue()
      .withDescription("Kafka topic to be sent along with callback messages");

  public static final ConfigOption<String> CALLBACK_KAFKA_PARTITION = ConfigOption
      .key(CALLBACK_PREFIX + "kafka.partition")
      .noDefaultValue()
      .withDescription("partition of CALLBACK_KAFKA_TOPIC, 0 by default");

  public static final ConfigOption<String> CALLBACK_KAFKA_ACKS = ConfigOption
      .key(CALLBACK_PREFIX + "kafka.acks")
      .defaultValue("all")
      .withDescription("kafka acks level, all by default");

  public static final ConfigOption<Integer> CALLBACK_KAFKA_RETRIES = ConfigOption
      .key(CALLBACK_PREFIX + "kafka.retries")
      .defaultValue(3)
      .withDescription("Times to retry. 3 by default");

  /**
   * Set default value for {@link HoodieWriteCommitKafkaCallbackConfig} if needed.
   */
  public static void setCallbackKafkaConfigIfNeeded(Properties props) {
    setDefaultValue(props, CALLBACK_KAFKA_ACKS);
    setDefaultValue(props, CALLBACK_KAFKA_RETRIES);
  }

}
