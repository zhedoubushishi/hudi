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
import org.apache.hudi.common.config.HoodieConfig;

import java.util.Properties;

import static org.apache.hudi.config.HoodieMetricsConfig.METRIC_PREFIX;

public class HoodieMetricsPrometheusConfig extends HoodieConfig {

  // Prometheus PushGateWay
  public static final String PUSHGATEWAY_PREFIX = METRIC_PREFIX + ".pushgateway";

  public static final ConfigOption<String> PUSHGATEWAY_HOST = ConfigOption
      .key(PUSHGATEWAY_PREFIX + ".host")
      .defaultValue("localhost")
      .withVersion("0.6.0")
      .withDocumentation("");

  public static final ConfigOption<Integer> PUSHGATEWAY_PORT = ConfigOption
      .key(PUSHGATEWAY_PREFIX + ".port")
      .defaultValue(9091)
      .withVersion("0.6.0")
      .withDocumentation("");

  public static final ConfigOption<Integer> PUSHGATEWAY_REPORT_PERIOD_SECONDS = ConfigOption
      .key(PUSHGATEWAY_PREFIX + ".report.period.seconds")
      .defaultValue(30)
      .withVersion("0.6.0")
      .withDocumentation("");

  public static final ConfigOption<Boolean> PUSHGATEWAY_DELETE_ON_SHUTDOWN = ConfigOption
      .key(PUSHGATEWAY_PREFIX + ".delete.on.shutdown")
      .defaultValue(true)
      .withVersion("0.6.0")
      .withDocumentation("");

  public static final ConfigOption<String> PUSHGATEWAY_JOB_NAME = ConfigOption
      .key(PUSHGATEWAY_PREFIX + ".job.name")
      .defaultValue("")
      .withVersion("0.6.0")
      .withDocumentation("");

  public static final ConfigOption<Boolean> PUSHGATEWAY_RANDOM_JOB_NAME_SUFFIX = ConfigOption
      .key(PUSHGATEWAY_PREFIX + ".random.job.name.suffix")
      .defaultValue(true)
      .withVersion("0.6.0")
      .withDocumentation("");

  // Prometheus HttpServer
  public static final String PROMETHEUS_PREFIX = METRIC_PREFIX + ".prometheus";

  public static final ConfigOption<Integer> PROMETHEUS_PORT = ConfigOption
      .key(PROMETHEUS_PREFIX + ".port")
      .defaultValue(9090)
      .withVersion("0.6.0")
      .withDocumentation("");

  private HoodieMetricsPrometheusConfig() {
    super();
  }

  public static HoodieMetricsPrometheusConfig.Builder newBuilder() {
    return new HoodieMetricsPrometheusConfig.Builder();
  }

  @Override
  public Properties getProps() {
    return super.getProps();
  }

  public static class Builder {

    private HoodieMetricsPrometheusConfig hoodieMetricsPrometheusConfig = new HoodieMetricsPrometheusConfig();

    public Builder fromProperties(Properties props) {
      this.hoodieMetricsPrometheusConfig.getProps().putAll(props);
      return this;
    }

    public HoodieMetricsPrometheusConfig build() {
      hoodieMetricsPrometheusConfig.setDefaultValue(PROMETHEUS_PORT);
      hoodieMetricsPrometheusConfig.setDefaultValue(PUSHGATEWAY_HOST);
      hoodieMetricsPrometheusConfig.setDefaultValue(PUSHGATEWAY_PORT);
      hoodieMetricsPrometheusConfig.setDefaultValue(PUSHGATEWAY_REPORT_PERIOD_SECONDS);
      hoodieMetricsPrometheusConfig.setDefaultValue(PUSHGATEWAY_DELETE_ON_SHUTDOWN);
      hoodieMetricsPrometheusConfig.setDefaultValue(PUSHGATEWAY_JOB_NAME);
      hoodieMetricsPrometheusConfig.setDefaultValue(PUSHGATEWAY_RANDOM_JOB_NAME_SUFFIX);
      return hoodieMetricsPrometheusConfig;
    }
  }
}
