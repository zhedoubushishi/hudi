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

package org.apache.hudi.config;

import org.apache.hudi.client.transaction.SimpleConcurrentFileWritesConflictResolutionStrategy;
import org.apache.hudi.client.transaction.ConflictResolutionStrategy;
import org.apache.hudi.client.transaction.lock.ZookeeperBasedLockProvider;
import org.apache.hudi.common.config.ConfigOption;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.lock.LockProvider;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import static org.apache.hudi.common.config.LockConfiguration.HIVE_DATABASE_NAME_PROP;
import static org.apache.hudi.common.config.LockConfiguration.HIVE_METASTORE_URI_PROP;
import static org.apache.hudi.common.config.LockConfiguration.HIVE_TABLE_NAME_PROP;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_CLIENT_NUM_RETRIES_PROP;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_CLIENT_RETRY_WAIT_TIME_IN_MILLIS_PROP;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_NUM_RETRIES_PROP;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_RETRY_MAX_WAIT_TIME_IN_MILLIS_PROP;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS_PROP;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP;
import static org.apache.hudi.common.config.LockConfiguration.LOCK_PREFIX;
import static org.apache.hudi.common.config.LockConfiguration.ZK_BASE_PATH_PROP;
import static org.apache.hudi.common.config.LockConfiguration.ZK_CONNECTION_TIMEOUT_MS_PROP;
import static org.apache.hudi.common.config.LockConfiguration.ZK_CONNECT_URL_PROP;
import static org.apache.hudi.common.config.LockConfiguration.ZK_LOCK_KEY_PROP;
import static org.apache.hudi.common.config.LockConfiguration.ZK_PORT_PROP;
import static org.apache.hudi.common.config.LockConfiguration.ZK_SESSION_TIMEOUT_MS_PROP;


/**
 * Hoodie Configs for Locks.
 */
public class HoodieLockConfig extends HoodieConfig {

  // Pluggable type of lock provider
  public static final ConfigOption<String> LOCK_PROVIDER_CLASS_PROP = ConfigOption
      .key(LOCK_PREFIX + "provider")
      .defaultValue(ZookeeperBasedLockProvider.class.getName())
      .withVersion("0.8.0")
      .withDescription("Lock provider class name, user can provide their own implementation of LockProvider "
          + "which should be subclass of org.apache.hudi.common.lock.LockProvider");

  // Pluggable strategies to use when resolving conflicts
  public static final ConfigOption<String> WRITE_CONFLICT_RESOLUTION_STRATEGY_CLASS_PROP = ConfigOption
      .key(LOCK_PREFIX + "conflict.resolution.strategy")
      .defaultValue(SimpleConcurrentFileWritesConflictResolutionStrategy.class.getName())
      .withVersion("0.8.0")
      .withDescription("Lock provider class name, this should be subclass of "
          + "org.apache.hudi.client.transaction.ConflictResolutionStrategy");

  private HoodieLockConfig(Properties props) {
    super(props);
  }

  public static HoodieLockConfig.Builder newBuilder() {
    return new HoodieLockConfig.Builder();
  }

  public static class Builder {

    private final Properties props = new Properties();

    public HoodieLockConfig.Builder fromFile(File propertiesFile) throws IOException {
      try (FileReader reader = new FileReader(propertiesFile)) {
        this.props.load(reader);
        return this;
      }
    }

    public HoodieLockConfig.Builder fromProperties(Properties props) {
      this.props.putAll(props);
      return this;
    }

    public HoodieLockConfig.Builder withLockProvider(Class<? extends LockProvider> lockProvider) {
      set(props, LOCK_PROVIDER_CLASS_PROP, lockProvider.getName());
      return this;
    }

    public HoodieLockConfig.Builder withHiveDatabaseName(String databaseName) {
      set(props, HIVE_DATABASE_NAME_PROP, databaseName);
      return this;
    }

    public HoodieLockConfig.Builder withHiveTableName(String tableName) {
      set(props, HIVE_TABLE_NAME_PROP, tableName);
      return this;
    }

    public HoodieLockConfig.Builder withHiveMetastoreURIs(String hiveMetastoreURIs) {
      set(props, HIVE_METASTORE_URI_PROP, hiveMetastoreURIs);
      return this;
    }

    public HoodieLockConfig.Builder withZkQuorum(String zkQuorum) {
      set(props, ZK_CONNECT_URL_PROP, zkQuorum);
      return this;
    }

    public HoodieLockConfig.Builder withZkBasePath(String zkBasePath) {
      set(props, ZK_BASE_PATH_PROP, zkBasePath);
      return this;
    }

    public HoodieLockConfig.Builder withZkPort(String zkPort) {
      set(props, ZK_PORT_PROP, zkPort);
      return this;
    }

    public HoodieLockConfig.Builder withZkLockKey(String zkLockKey) {
      set(props, ZK_LOCK_KEY_PROP, zkLockKey);
      return this;
    }

    public HoodieLockConfig.Builder withZkConnectionTimeoutInMs(Long connectionTimeoutInMs) {
      set(props, ZK_CONNECTION_TIMEOUT_MS_PROP, String.valueOf(connectionTimeoutInMs));
      return this;
    }

    public HoodieLockConfig.Builder withZkSessionTimeoutInMs(Long sessionTimeoutInMs) {
      set(props, ZK_SESSION_TIMEOUT_MS_PROP, String.valueOf(sessionTimeoutInMs));
      return this;
    }

    public HoodieLockConfig.Builder withNumRetries(int numRetries) {
      set(props, LOCK_ACQUIRE_NUM_RETRIES_PROP, String.valueOf(numRetries));
      return this;
    }

    public HoodieLockConfig.Builder withRetryWaitTimeInMillis(Long retryWaitTimeInMillis) {
      set(props, LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS_PROP, String.valueOf(retryWaitTimeInMillis));
      return this;
    }

    public HoodieLockConfig.Builder withRetryMaxWaitTimeInMillis(Long retryMaxWaitTimeInMillis) {
      set(props, LOCK_ACQUIRE_RETRY_MAX_WAIT_TIME_IN_MILLIS_PROP, String.valueOf(retryMaxWaitTimeInMillis));
      return this;
    }

    public HoodieLockConfig.Builder withClientNumRetries(int clientNumRetries) {
      set(props, LOCK_ACQUIRE_CLIENT_NUM_RETRIES_PROP, String.valueOf(clientNumRetries));
      return this;
    }

    public HoodieLockConfig.Builder withClientRetryWaitTimeInMillis(Long clientRetryWaitTimeInMillis) {
      set(props, LOCK_ACQUIRE_CLIENT_RETRY_WAIT_TIME_IN_MILLIS_PROP, String.valueOf(clientRetryWaitTimeInMillis));
      return this;
    }

    public HoodieLockConfig.Builder withLockWaitTimeInMillis(Long waitTimeInMillis) {
      set(props, LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP, String.valueOf(waitTimeInMillis));
      return this;
    }

    public HoodieLockConfig.Builder withConflictResolutionStrategy(ConflictResolutionStrategy conflictResolutionStrategy) {
      set(props, WRITE_CONFLICT_RESOLUTION_STRATEGY_CLASS_PROP, conflictResolutionStrategy.getClass().getName());
      return this;
    }

    public HoodieLockConfig build() {
      HoodieLockConfig config = new HoodieLockConfig(props);
      setDefaultValue(props, LOCK_PROVIDER_CLASS_PROP);
      setDefaultValue(props, WRITE_CONFLICT_RESOLUTION_STRATEGY_CLASS_PROP);
      setDefaultValue(props, LOCK_ACQUIRE_NUM_RETRIES_PROP);
      setDefaultValue(props, LOCK_ACQUIRE_RETRY_WAIT_TIME_IN_MILLIS_PROP);
      setDefaultValue(props, LOCK_ACQUIRE_RETRY_MAX_WAIT_TIME_IN_MILLIS_PROP);
      setDefaultValue(props, LOCK_ACQUIRE_CLIENT_NUM_RETRIES_PROP);
      setDefaultValue(props, LOCK_ACQUIRE_CLIENT_RETRY_WAIT_TIME_IN_MILLIS_PROP);
      setDefaultValue(props, ZK_CONNECTION_TIMEOUT_MS_PROP);
      setDefaultValue(props, ZK_SESSION_TIMEOUT_MS_PROP);
      setDefaultValue(props, LOCK_ACQUIRE_WAIT_TIMEOUT_MS_PROP);
      return config;
    }
  }

}
