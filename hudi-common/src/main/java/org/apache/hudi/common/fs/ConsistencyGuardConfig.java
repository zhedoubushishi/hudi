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

package org.apache.hudi.common.fs;

import org.apache.hudi.common.config.ConfigOption;
import org.apache.hudi.common.config.HoodieConfig;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * The consistency guard relevant config options.
 */
public class ConsistencyGuardConfig extends HoodieConfig {

  // time between successive attempts to ensure written data's metadata is consistent on storage
  @Deprecated
  public static final ConfigOption<String> CONSISTENCY_CHECK_ENABLED_PROP = ConfigOption
      .key("hoodie.consistency.check.enabled")
      .defaultValue("false")
      .withVersion("0.5.0")
      .withDescription("Enabled to handle S3 eventual consistency issue. This property is no longer required "
          + "since S3 is now strongly consistent. Will be removed in the future releases.");

  public static final ConfigOption<Long> INITIAL_CONSISTENCY_CHECK_INTERVAL_MS_PROP = ConfigOption
      .key("hoodie.consistency.check.initial_interval_ms")
      .defaultValue(400L)
      .withVersion("0.5.0")
      .withDescription("");

  // max interval time
  public static final ConfigOption<Long> MAX_CONSISTENCY_CHECK_INTERVAL_MS_PROP = ConfigOption
      .key("hoodie.consistency.check.max_interval_ms")
      .defaultValue(20000L)
      .withVersion("0.5.0")
      .withDescription("");

  // maximum number of checks, for consistency of written data. Will wait upto 140 Secs
  public static final ConfigOption<Integer> MAX_CONSISTENCY_CHECKS_PROP = ConfigOption
      .key("hoodie.consistency.check.max_checks")
      .defaultValue(6)
      .withVersion("0.5.0")
      .withDescription("");

  // sleep time for OptimisticConsistencyGuard
  public static final ConfigOption<Long> OPTIMISTIC_CONSISTENCY_GUARD_SLEEP_TIME_MS_PROP = ConfigOption
      .key("hoodie.optimistic.consistency.guard.sleep_time_ms")
      .defaultValue(500L)
      .withVersion("0.6.0")
      .withDescription("");

  // config to enable OptimisticConsistencyGuard in finalizeWrite instead of FailSafeConsistencyGuard
  public static final ConfigOption<Boolean> ENABLE_OPTIMISTIC_CONSISTENCY_GUARD = ConfigOption
      .key("_hoodie.optimistic.consistency.guard.enable")
      .defaultValue(true)
      .withVersion("0.6.0")
      .withDescription("");

  private ConsistencyGuardConfig() {
    super();
  }

  public static ConsistencyGuardConfig.Builder newBuilder() {
    return new Builder();
  }

  public boolean isConsistencyCheckEnabled() {
    return getBoolean(props, CONSISTENCY_CHECK_ENABLED_PROP);
  }

  public int getMaxConsistencyChecks() {
    return getInt(props, MAX_CONSISTENCY_CHECKS_PROP);
  }

  public int getInitialConsistencyCheckIntervalMs() {
    return getInt(props, INITIAL_CONSISTENCY_CHECK_INTERVAL_MS_PROP);
  }

  public int getMaxConsistencyCheckIntervalMs() {
    return getInt(props, MAX_CONSISTENCY_CHECK_INTERVAL_MS_PROP);
  }

  public long getOptimisticConsistencyGuardSleepTimeMs() {
    return getLong(props, OPTIMISTIC_CONSISTENCY_GUARD_SLEEP_TIME_MS_PROP);
  }

  public boolean shouldEnableOptimisticConsistencyGuard() {
    return getBoolean(props, ENABLE_OPTIMISTIC_CONSISTENCY_GUARD);
  }

  /**
   * The builder used to build consistency configurations.
   */
  public static class Builder {

    private final ConsistencyGuardConfig consistencyGuardConfig = new ConsistencyGuardConfig();

    public Builder fromFile(File propertiesFile) throws IOException {
      try (FileReader reader = new FileReader(propertiesFile)) {
        consistencyGuardConfig.getProps().load(reader);
        return this;
      }
    }

    public Builder fromProperties(Properties props) {
      this.consistencyGuardConfig.getProps().putAll(props);
      return this;
    }

    public Builder withConsistencyCheckEnabled(boolean enabled) {
      consistencyGuardConfig.set(CONSISTENCY_CHECK_ENABLED_PROP, String.valueOf(enabled));
      return this;
    }

    public Builder withInitialConsistencyCheckIntervalMs(int initialIntevalMs) {
      consistencyGuardConfig.set(INITIAL_CONSISTENCY_CHECK_INTERVAL_MS_PROP, String.valueOf(initialIntevalMs));
      return this;
    }

    public Builder withMaxConsistencyCheckIntervalMs(int maxIntervalMs) {
      consistencyGuardConfig.set(MAX_CONSISTENCY_CHECK_INTERVAL_MS_PROP, String.valueOf(maxIntervalMs));
      return this;
    }

    public Builder withMaxConsistencyChecks(int maxConsistencyChecks) {
      consistencyGuardConfig.set(MAX_CONSISTENCY_CHECKS_PROP, String.valueOf(maxConsistencyChecks));
      return this;
    }

    public Builder withOptimisticConsistencyGuardSleepTimeMs(long sleepTimeMs) {
      consistencyGuardConfig.set(OPTIMISTIC_CONSISTENCY_GUARD_SLEEP_TIME_MS_PROP, String.valueOf(sleepTimeMs));
      return this;
    }

    public Builder withEnableOptimisticConsistencyGuard(boolean enableOptimisticConsistencyGuard) {
      consistencyGuardConfig.set(ENABLE_OPTIMISTIC_CONSISTENCY_GUARD, String.valueOf(enableOptimisticConsistencyGuard));
      return this;
    }

    public ConsistencyGuardConfig build() {
      consistencyGuardConfig.setDefaultValue(CONSISTENCY_CHECK_ENABLED_PROP);
      consistencyGuardConfig.setDefaultValue(INITIAL_CONSISTENCY_CHECK_INTERVAL_MS_PROP);
      consistencyGuardConfig.setDefaultValue(MAX_CONSISTENCY_CHECK_INTERVAL_MS_PROP);
      consistencyGuardConfig.setDefaultValue(MAX_CONSISTENCY_CHECKS_PROP);
      consistencyGuardConfig.setDefaultValue(OPTIMISTIC_CONSISTENCY_GUARD_SLEEP_TIME_MS_PROP);
      consistencyGuardConfig.setDefaultValue(ENABLE_OPTIMISTIC_CONSISTENCY_GUARD);
      return consistencyGuardConfig;
    }
  }
}
