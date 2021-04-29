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

import java.io.Serializable;
import java.util.Properties;

/**
 * Configuration for managing locks. Since this configuration needs to be shared with HiveMetaStore based lock,
 * which is in a different package than other lock providers, we use this as a data transfer object in hoodie-common
 */
public class LockConfiguration implements Serializable {

  public static final String LOCK_PREFIX = "hoodie.write.lock.";

  public static final ConfigOption<String> LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS_PROP = ConfigOption
      .key(LOCK_PREFIX + "wait_time_ms_between_retry")
      .defaultValue(String.valueOf(5000L))
      .withDescription("");

  public static final ConfigOption<String> LOCK_ACQUIRE_RETRY_MAX_WAIT_TIME_IN_MILLIS_PROP = ConfigOption
      .key(LOCK_PREFIX + "max_wait_time_ms_between_retry")
      .defaultValue(String.valueOf(5000L))
      .withDescription("Initial amount of time to wait between retries by lock provider client");

  public static final ConfigOption<String> LOCK_ACQUIRE_CLIENT_RETRY_WAIT_TIME_IN_MILLIS_PROP = ConfigOption
      .key(LOCK_PREFIX + "client.wait_time_ms_between_retry")
      .defaultValue(String.valueOf(10000L))
      .withDescription("Amount of time to wait between retries from the hudi client");

  public static final ConfigOption<String> LOCK_ACQUIRE_NUM_RETRIES_PROP = ConfigOption
      .key(LOCK_PREFIX + "num_retries")
      .defaultValue(String.valueOf(3))
      .withDescription("Maximum number of times to retry by lock provider client");

  public static final ConfigOption<String> LOCK_ACQUIRE_CLIENT_NUM_RETRIES_PROP = ConfigOption
      .key(LOCK_PREFIX + "client.num_retries")
      .defaultValue(String.valueOf(0))
      .withDescription("Maximum number of times to retry to acquire lock additionally from the hudi client");

  public static final ConfigOption<Integer> LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP = ConfigOption
      .key(LOCK_PREFIX + "wait_time_ms")
      .defaultValue(60 * 1000)
      .withDescription("");

  // configs for file system based locks. NOTE: This only works for DFS with atomic create/delete operation
  public static final String FILESYSTEM_BASED_LOCK_PROPERTY_PREFIX = LOCK_PREFIX + "filesystem.";

  public static final ConfigOption<String> FILESYSTEM_LOCK_PATH_PROP = ConfigOption
      .key(FILESYSTEM_BASED_LOCK_PROPERTY_PREFIX + "path")
      .noDefaultValue()
      .withDescription("");

  // configs for metastore based locks
  public static final String HIVE_METASTORE_LOCK_PROPERTY_PREFIX = LOCK_PREFIX + "hivemetastore.";

  public static final ConfigOption<String> HIVE_DATABASE_NAME_PROP = ConfigOption
      .key(HIVE_METASTORE_LOCK_PROPERTY_PREFIX + "database")
      .noDefaultValue()
      .withDescription("The Hive database to acquire lock against");

  public static final ConfigOption<String> HIVE_TABLE_NAME_PROP = ConfigOption
      .key(HIVE_METASTORE_LOCK_PROPERTY_PREFIX + "table")
      .noDefaultValue()
      .withDescription("The Hive table under the hive database to acquire lock against");

  public static final ConfigOption<String> HIVE_METASTORE_URI_PROP = ConfigOption
      .key(HIVE_METASTORE_LOCK_PROPERTY_PREFIX + "uris")
      .noDefaultValue()
      .withDescription("");

  // Zookeeper configs for zk based locks
  public static final String ZOOKEEPER_BASED_LOCK_PROPERTY_PREFIX = LOCK_PREFIX + "zookeeper.";

  public static final ConfigOption<String> ZK_BASE_PATH_PROP = ConfigOption
      .key(ZOOKEEPER_BASED_LOCK_PROPERTY_PREFIX + "base_path")
      .noDefaultValue()
      .withDescription("The base path on Zookeeper under which to create a ZNode to acquire the lock. "
          + "This should be common for all jobs writing to the same table");

  public static final ConfigOption<Integer> ZK_SESSION_TIMEOUT_MS_PROP = ConfigOption
      .key(ZOOKEEPER_BASED_LOCK_PROPERTY_PREFIX + "session_timeout_ms")
      .defaultValue(60 * 1000)
      .withDescription("How long to wait after losing a connection to ZooKeeper before the session is expired");

  public static final ConfigOption<Integer> ZK_CONNECTION_TIMEOUT_MS_PROP = ConfigOption
      .key(ZOOKEEPER_BASED_LOCK_PROPERTY_PREFIX + "connection_timeout_ms")
      .defaultValue(15 * 1000)
      .withDescription("How long to wait when connecting to ZooKeeper before considering the connection a failure");

  public static final ConfigOption<String> ZK_CONNECT_URL_PROP = ConfigOption
      .key(ZOOKEEPER_BASED_LOCK_PROPERTY_PREFIX + "url")
      .noDefaultValue()
      .withDescription("Set the list of comma separated servers to connect to");

  public static final ConfigOption<String> ZK_PORT_PROP = ConfigOption
      .key(ZOOKEEPER_BASED_LOCK_PROPERTY_PREFIX + "port")
      .noDefaultValue()
      .withDescription("The connection port to be used for Zookeeper");

  public static final ConfigOption<String> ZK_LOCK_KEY_PROP = ConfigOption
      .key(ZOOKEEPER_BASED_LOCK_PROPERTY_PREFIX + "lock_key")
      .noDefaultValue()
      .withDescription("Key name under base_path at which to create a ZNode and acquire lock. "
          + "Final path on zk will look like base_path/lock_key. We recommend setting this to the table name");

  private final TypedProperties props;

  public LockConfiguration(Properties props) {
    this.props = new TypedProperties(props);
  }

  public TypedProperties getConfig() {
    return props;
  }

}
