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
      .withVersion("0.5.0")
      .withDescription("Turn on/off metrics reporting. off by default.");

  public static final ConfigOption<MetricsReporterType> METRICS_REPORTER_TYPE = ConfigOption
      .key(METRIC_PREFIX + ".reporter.type")
      .defaultValue(MetricsReporterType.GRAPHITE)
      .withVersion("0.5.0")
      .withDescription("Type of metrics reporter.");

  // Graphite
  public static final String GRAPHITE_PREFIX = METRIC_PREFIX + ".graphite";

  public static final ConfigOption<String> GRAPHITE_SERVER_HOST = ConfigOption
      .key(GRAPHITE_PREFIX + ".host")
      .defaultValue("localhost")
      .withVersion("0.5.0")
      .withDescription("Graphite host to connect to");

  public static final ConfigOption<Integer> GRAPHITE_SERVER_PORT = ConfigOption
      .key(GRAPHITE_PREFIX + ".port")
      .defaultValue(4756)
      .withVersion("0.5.0")
      .withDescription("Graphite port to connect to");

  // Jmx
  public static final String JMX_PREFIX = METRIC_PREFIX + ".jmx";

  public static final ConfigOption<String> JMX_HOST = ConfigOption
      .key(JMX_PREFIX + ".host")
      .defaultValue("localhost")
      .withVersion("0.5.1")
      .withDescription("Jmx host to connect to");

  public static final ConfigOption<Integer> JMX_PORT = ConfigOption
      .key(JMX_PREFIX + ".port")
      .defaultValue(9889)
      .withVersion("0.5.1")
      .withDescription("Jmx port to connect to");

  public static final ConfigOption<String> GRAPHITE_METRIC_PREFIX = ConfigOption
      .key(GRAPHITE_PREFIX + ".metric.prefix")
      .noDefaultValue()
      .withVersion("0.5.1")
      .withDescription("Standard prefix applied to all metrics. This helps to add datacenter, environment information for e.g");

  // User defined
  public static final ConfigOption<String> METRICS_REPORTER_CLASS = ConfigOption
      .key(METRIC_PREFIX + ".reporter.class")
      .defaultValue("")
      .withVersion("0.6.0")
      .withDescription("");

  // Enable metrics collection from executors
  public static final ConfigOption<String> ENABLE_EXECUTOR_METRICS = ConfigOption
      .key(METRIC_PREFIX + ".executor.enable")
      .noDefaultValue()
      .withVersion("0.7.0")
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
      set(props, METRICS_ON, String.valueOf(metricsOn));
      return this;
    }

    public Builder withReporterType(String reporterType) {
      set(props, METRICS_REPORTER_TYPE, reporterType);
      return this;
    }

    public Builder toGraphiteHost(String host) {
      set(props, GRAPHITE_SERVER_HOST, host);
      return this;
    }

    public Builder onGraphitePort(int port) {
      set(props, GRAPHITE_SERVER_PORT, String.valueOf(port));
      return this;
    }

    public Builder toJmxHost(String host) {
      set(props, JMX_HOST, host);
      return this;
    }

    public Builder onJmxPort(String port) {
      set(props, JMX_PORT, port);
      return this;
    }

    public Builder usePrefix(String prefix) {
      set(props, GRAPHITE_METRIC_PREFIX, prefix);
      return this;
    }

    public Builder withReporterClass(String className) {
      set(props, METRICS_REPORTER_CLASS, className);
      return this;
    }

    public Builder withExecutorMetrics(boolean enable) {
      set(props, ENABLE_EXECUTOR_METRICS, String.valueOf(enable));
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
      MetricsReporterType reporterType = MetricsReporterType.valueOf(getString(props, METRICS_REPORTER_TYPE));
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
