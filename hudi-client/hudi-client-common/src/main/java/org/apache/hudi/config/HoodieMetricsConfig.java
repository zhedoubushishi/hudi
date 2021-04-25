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
import org.apache.hudi.metrics.MetricsReporterType;

import javax.annotation.concurrent.Immutable;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * Fetch the configurations used by the Metrics system.
 */
@Immutable
public class HoodieMetricsConfig extends DefaultHoodieConfig {

  public static final String METRIC_PREFIX = "hoodie.metrics";

  public static final ConfigOption<Boolean> METRICS_ON = ConfigOption
      .key(METRIC_PREFIX + ".on")
      .defaultValue(false)
      .withDescription("");

  public static final ConfigOption<MetricsReporterType> METRICS_REPORTER_TYPE = ConfigOption
      .key(METRIC_PREFIX + ".reporter.type")
      .defaultValue(MetricsReporterType.GRAPHITE)
      .withDescription("");

  // Graphite
  public static final String GRAPHITE_PREFIX = METRIC_PREFIX + ".graphite";

  public static final ConfigOption<String> GRAPHITE_SERVER_HOST = ConfigOption
      .key(GRAPHITE_PREFIX + ".host")
      .defaultValue("localhost")
      .withDescription("");

  public static final ConfigOption<Integer> GRAPHITE_SERVER_PORT = ConfigOption
      .key(GRAPHITE_PREFIX + ".port")
      .defaultValue(4756)
      .withDescription("");

  // Jmx
  public static final String JMX_PREFIX = METRIC_PREFIX + ".jmx";

  public static final ConfigOption<String> JMX_HOST = ConfigOption
      .key(JMX_PREFIX + ".host")
      .defaultValue("localhost")
      .withDescription("");

  public static final ConfigOption<Integer> JMX_PORT = ConfigOption
      .key(JMX_PREFIX + ".port")
      .defaultValue(9889)
      .withDescription("");

  public static final ConfigOption<String> GRAPHITE_METRIC_PREFIX = ConfigOption
      .key(GRAPHITE_PREFIX + ".metric.prefix")
      .noDefaultValue()
      .withDescription("");

  // User defined
  public static final ConfigOption<String> METRICS_REPORTER_CLASS = ConfigOption
      .key(METRIC_PREFIX + ".reporter.class")
      .defaultValue("")
      .withDescription("");

  // Enable metrics collection from executors
  public static final ConfigOption<String> ENABLE_EXECUTOR_METRICS = ConfigOption
      .key(METRIC_PREFIX + ".executor.enable")
      .noDefaultValue()
      .withDescription("");

  private HoodieMetricsConfig(Properties props) {
    super(props);
  }

  public static HoodieMetricsConfig.Builder newBuilder() {
    return new Builder();
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

    public Builder on(boolean metricsOn) {
      props.setProperty(METRICS_ON.key(), String.valueOf(metricsOn));
      return this;
    }

    public Builder withReporterType(String reporterType) {
      props.setProperty(METRICS_REPORTER_TYPE.key(), reporterType);
      return this;
    }

    public Builder toGraphiteHost(String host) {
      props.setProperty(GRAPHITE_SERVER_HOST.key(), host);
      return this;
    }

    public Builder onGraphitePort(int port) {
      props.setProperty(GRAPHITE_SERVER_PORT.key(), String.valueOf(port));
      return this;
    }

    public Builder toJmxHost(String host) {
      props.setProperty(JMX_HOST.key(), host);
      return this;
    }

    public Builder onJmxPort(String port) {
      props.setProperty(JMX_PORT.key(), port);
      return this;
    }

    public Builder usePrefix(String prefix) {
      props.setProperty(GRAPHITE_METRIC_PREFIX.key(), prefix);
      return this;
    }

    public Builder withReporterClass(String className) {
      props.setProperty(METRICS_REPORTER_CLASS.key(), className);
      return this;
    }

    public Builder withExecutorMetrics(boolean enable) {
      props.setProperty(ENABLE_EXECUTOR_METRICS.key(), String.valueOf(enable));
      return this;
    }

    public HoodieMetricsConfig build() {
      HoodieMetricsConfig config = new HoodieMetricsConfig(props);
      setDefaultValue(props, METRICS_ON);
      setDefaultValue(props, METRICS_REPORTER_TYPE);
      setDefaultValue(props, GRAPHITE_SERVER_HOST);
      setDefaultValue(props, GRAPHITE_SERVER_PORT);
      setDefaultValue(props, JMX_HOST);
      setDefaultValue(props, JMX_PORT);
      MetricsReporterType reporterType = MetricsReporterType.valueOf(props.getProperty(METRICS_REPORTER_TYPE.key()));
      setDefaultOnCondition(props, reporterType == MetricsReporterType.DATADOG,
          HoodieMetricsDatadogConfig.newBuilder().fromProperties(props).build());
      setDefaultValue(props, METRICS_REPORTER_CLASS);
      setDefaultOnCondition(props, reporterType == MetricsReporterType.PROMETHEUS_PUSHGATEWAY,
              HoodieMetricsPrometheusConfig.newBuilder().fromProperties(props).build());
      setDefaultOnCondition(props, reporterType == MetricsReporterType.PROMETHEUS,
              HoodieMetricsPrometheusConfig.newBuilder().fromProperties(props).build());

      return config;
    }
  }

}
