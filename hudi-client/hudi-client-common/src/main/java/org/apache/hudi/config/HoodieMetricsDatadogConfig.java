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

import java.util.Properties;

import static org.apache.hudi.config.HoodieMetricsConfig.METRIC_PREFIX;

/**
 * Configs for Datadog reporter type.
 * <p>
 * {@link org.apache.hudi.metrics.MetricsReporterType#DATADOG}
 */
@Immutable
public class HoodieMetricsDatadogConfig extends DefaultHoodieConfig {

  public static final String DATADOG_PREFIX = METRIC_PREFIX + ".datadog";

  public static final ConfigOption<Integer> DATADOG_REPORT_PERIOD_SECONDS = ConfigOption
      .key(DATADOG_PREFIX + ".report.period.seconds")
      .defaultValue(30)
      .withDescription("");

  public static final ConfigOption<String> DATADOG_API_SITE = ConfigOption
      .key(DATADOG_PREFIX + ".api.site")
      .noDefaultValue()
      .withDescription("");

  public static final ConfigOption<String> DATADOG_API_KEY = ConfigOption
      .key(DATADOG_PREFIX + ".api.key")
      .noDefaultValue()
      .withDescription("");

  public static final ConfigOption<Boolean> DATADOG_API_KEY_SKIP_VALIDATION = ConfigOption
      .key(DATADOG_PREFIX + ".api.key.skip.validation")
      .defaultValue(false)
      .withDescription("");

  public static final ConfigOption<String> DATADOG_API_KEY_SUPPLIER = ConfigOption
      .key(DATADOG_PREFIX + ".api.key.supplier")
      .noDefaultValue()
      .withDescription("");

  public static final ConfigOption<Integer> DATADOG_API_TIMEOUT_SECONDS = ConfigOption
      .key(DATADOG_PREFIX + ".api.timeout.seconds")
      .defaultValue(3)
      .withDescription("");

  public static final ConfigOption<String> DATADOG_METRIC_PREFIX = ConfigOption
      .key(DATADOG_PREFIX + ".metric.prefix")
      .noDefaultValue()
      .withDescription("");

  public static final ConfigOption<String> DATADOG_METRIC_HOST = ConfigOption
      .key(DATADOG_PREFIX + ".metric.host")
      .noDefaultValue()
      .withDescription("");

  public static final ConfigOption<String> DATADOG_METRIC_TAGS = ConfigOption
      .key(DATADOG_PREFIX + ".metric.tags")
      .noDefaultValue()
      .withDescription("");

  private HoodieMetricsDatadogConfig(Properties props) {
    super(props);
  }

  public static HoodieMetricsDatadogConfig.Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    private final Properties props = new Properties();

    public Builder fromProperties(Properties props) {
      this.props.putAll(props);
      return this;
    }

    public Builder withDatadogReportPeriodSeconds(int period) {
      props.setProperty(DATADOG_REPORT_PERIOD_SECONDS.key(), String.valueOf(period));
      return this;
    }

    public Builder withDatadogApiSite(String apiSite) {
      props.setProperty(DATADOG_API_SITE.key(), apiSite);
      return this;
    }

    public Builder withDatadogApiKey(String apiKey) {
      props.setProperty(DATADOG_API_KEY.key(), apiKey);
      return this;
    }

    public Builder withDatadogApiKeySkipValidation(boolean skip) {
      props.setProperty(DATADOG_API_KEY_SKIP_VALIDATION.key(), String.valueOf(skip));
      return this;
    }

    public Builder withDatadogApiKeySupplier(String apiKeySupplier) {
      props.setProperty(DATADOG_API_KEY_SUPPLIER.key(), apiKeySupplier);
      return this;
    }

    public Builder withDatadogApiTimeoutSeconds(int timeout) {
      props.setProperty(DATADOG_API_TIMEOUT_SECONDS.key(), String.valueOf(timeout));
      return this;
    }

    public Builder withDatadogPrefix(String prefix) {
      props.setProperty(DATADOG_METRIC_PREFIX.key(), prefix);
      return this;
    }

    public Builder withDatadogHost(String host) {
      props.setProperty(DATADOG_METRIC_HOST.key(), host);
      return this;
    }

    public Builder withDatadogTags(String tags) {
      props.setProperty(DATADOG_METRIC_TAGS.key(), tags);
      return this;
    }

    public HoodieMetricsDatadogConfig build() {
      HoodieMetricsDatadogConfig config = new HoodieMetricsDatadogConfig(props);
      setDefaultValue(props, DATADOG_REPORT_PERIOD_SECONDS);
      setDefaultValue(props, DATADOG_API_KEY_SKIP_VALIDATION);
      setDefaultValue(props, DATADOG_API_TIMEOUT_SECONDS);
      return config;
    }
  }
}
