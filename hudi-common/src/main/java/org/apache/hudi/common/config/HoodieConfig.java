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

import org.apache.hudi.common.util.Option;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

/**
 * This class deals with {@link org.apache.hudi.common.config.ConfigOption} and provides get/set functionalities.
 */
public class HoodieConfig implements Serializable {

  private static final Logger LOG = LogManager.getLogger(HoodieConfig.class);

  protected Properties props;

  public HoodieConfig() {
    this.props = new Properties();
  }

  public HoodieConfig(Properties props) {
    this.props = props;
  }

  public static void setDefaultOnCondition(Properties props, boolean condition, HoodieConfig config) {
    if (condition) {
      props.putAll(config.getProps());
    }
  }

  public static <T> void set(Properties props, ConfigOption<T> cfg, String val) {
    props.setProperty(cfg.key(), val);
  }

  public static <T> void setDefaultValue(Properties props, ConfigOption<T> configOption) {
    if (!contains(props, configOption)) {
      Option<T> inferValue = Option.empty();
      if (configOption.getInferFunc().isPresent()) {
        inferValue = configOption.getInferFunc().get().apply(props);
      }
      props.setProperty(configOption.key(), inferValue.isPresent() ? inferValue.get().toString() : configOption.defaultValue().toString());
    }
  }

  public static <T> void setDefaultValue(Properties props, ConfigOption<T> configOption, T defaultVal) {
    if (!contains(props, configOption)) {
      props.setProperty(configOption.key(), defaultVal.toString());
    }
  }

  public static <T> boolean contains(Map props, ConfigOption<T> configOption) {
    if (props.containsKey(configOption.key())) {
      return true;
    }
    return Arrays.stream(configOption.getAlternatives()).anyMatch(props::containsKey);
  }

  public static <T> Option<Object> getRawValue(Map props, ConfigOption<T> configOption) {
    if (props.containsKey(configOption.key())) {
      return Option.ofNullable(props.get(configOption.key()));
    }
    for (String alternative : configOption.getAlternatives()) {
      if (props.containsKey(alternative)) {
        LOG.warn(String.format("The configuration key '%s' has been deprecated "
                + "and may be removed in the future. Please use the new key '%s' instead.",
            alternative, configOption.key()));
        return Option.ofNullable(props.get(alternative));
      }
    }
    return Option.empty();
  }

  public static <T> String getString(Map props, ConfigOption<T> configOption) {
    Option<Object> rawValue = getRawValue(props, configOption);
    return rawValue.map(Object::toString).orElse(null);
  }

  public static <T> Integer getInt(Map props, ConfigOption<T> configOption) {
    Option<Object> rawValue = getRawValue(props, configOption);
    return rawValue.map(v -> Integer.parseInt(v.toString())).orElse(null);
  }

  public static <T> Boolean getBoolean(Map props, ConfigOption<T> configOption) {
    Option<Object> rawValue = getRawValue(props, configOption);
    return rawValue.map(v -> Boolean.parseBoolean(v.toString())).orElse(null);
  }

  public static <T> Long getLong(Map props, ConfigOption<T> configOption) {
    Option<Object> rawValue = getRawValue(props, configOption);
    return rawValue.map(v -> Long.parseLong(v.toString())).orElse(null);
  }

  public static <T> Float getFloat(Map props, ConfigOption<T> configOption) {
    Option<Object> rawValue = getRawValue(props, configOption);
    return rawValue.map(v -> Float.parseFloat(v.toString())).orElse(null);
  }

  public static <T> Double getDouble(Map props, ConfigOption<T> configOption) {
    Option<Object> rawValue = getRawValue(props, configOption);
    return rawValue.map(v -> Double.parseDouble(v.toString())).orElse(null);
  }

  public static <T> String getStringOrDefault(Map props, ConfigOption<T> configOption) {
    return getStringOrDefault(props, configOption, configOption.defaultValue().toString());
  }

  public static <T> String getStringOrDefault(Map props, ConfigOption<T> configOption, String defaultVal) {
    Option<Object> rawValue = getRawValue(props, configOption);
    return rawValue.orElse(defaultVal).toString();
  }

  public Properties getProps() {
    return props;
  }
}
