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

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
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

  public <T> void set(ConfigOption<T> cfg, String val) {
    props.setProperty(cfg.key(), val);
  }

  public <T> void setDefaultValue(ConfigOption<T> configOption) {
    if (!contains(configOption)) {
      Option<T> inferValue = Option.empty();
      if (configOption.getInferFunc().isPresent()) {
        inferValue = configOption.getInferFunc().get().apply(props);
      }
      props.setProperty(configOption.key(), inferValue.isPresent() ? inferValue.get().toString() : configOption.defaultValue().toString());
    }
  }

  public <T> void setDefaultValue(ConfigOption<T> configOption, T defaultVal) {
    if (!contains(configOption)) {
      props.setProperty(configOption.key(), defaultVal.toString());
    }
  }

  public <T> boolean contains(ConfigOption<T> configOption) {
    if (props.containsKey(configOption.key())) {
      return true;
    }
    return Arrays.stream(configOption.getAlternatives()).anyMatch(props::containsKey);
  }

  private <T> Option<Object> getRawValue(ConfigOption<T> configOption) {
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

  public <T> String getString(ConfigOption<T> configOption) {
    Option<Object> rawValue = getRawValue(configOption);
    return rawValue.map(Object::toString).orElse(null);
  }

  public <T> Integer getInt(ConfigOption<T> configOption) {
    Option<Object> rawValue = getRawValue(configOption);
    return rawValue.map(v -> Integer.parseInt(v.toString())).orElse(null);
  }

  public <T> Boolean getBoolean(ConfigOption<T> configOption) {
    Option<Object> rawValue = getRawValue(configOption);
    return rawValue.map(v -> Boolean.parseBoolean(v.toString())).orElse(null);
  }

  public <T> Long getLong(ConfigOption<T> configOption) {
    Option<Object> rawValue = getRawValue(configOption);
    return rawValue.map(v -> Long.parseLong(v.toString())).orElse(null);
  }

  public <T> Float getFloat(ConfigOption<T> configOption) {
    Option<Object> rawValue = getRawValue(configOption);
    return rawValue.map(v -> Float.parseFloat(v.toString())).orElse(null);
  }

  public <T> Double getDouble(ConfigOption<T> configOption) {
    Option<Object> rawValue = getRawValue(configOption);
    return rawValue.map(v -> Double.parseDouble(v.toString())).orElse(null);
  }

  public <T> String getStringOrDefault(ConfigOption<T> configOption) {
    return getStringOrDefault(configOption, configOption.defaultValue().toString());
  }

  public <T> String getStringOrDefault(ConfigOption<T> configOption, String defaultVal) {
    Option<Object> rawValue = getRawValue(configOption);
    return rawValue.map(Object::toString).orElse(defaultVal);
  }

  public Properties getProps() {
    return props;
  }

  public void setDefaultOnCondition(boolean condition, HoodieConfig config) {
    if (condition) {
      props.putAll(config.getProps());
    }
  }

  /*
  public <T> void set(ConfigOption<T> cfg, String val) {
    props.setProperty(cfg.key(), val);
  }

  public <T> void setDefaultValue(ConfigOption<T> configOption) {
    setDefaultValue(props, configOption);
  }

  public <T> void setDefaultValue(ConfigOption<T> configOption, T defaultVal) {
    setDefaultValue(props, configOption, defaultVal);
  }

  public <T> String getString(ConfigOption<T> configOption) {
    Option<Object> rawValue = getRawValue(props, configOption);
    return rawValue.map(Object::toString).orElse(null);
  }

  public <T> Integer getInt(ConfigOption<T> configOption) {
    return getInt(props, configOption);
  }

  public <T> Boolean getBoolean(ConfigOption<T> configOption) {
    return getBoolean(props, configOption);
  }

  public <T> Double getDouble(ConfigOption<T> configOption) {
    return getDouble(props, configOption);
  }

  public <T> Float getFloat(ConfigOption<T> configOption) {
    return getFloat(props, configOption);
  }

  public <T> Long getLong(ConfigOption<T> configOption) {
    return getLong(props, configOption);
  }

  public <T> String getStringOrDefault(ConfigOption<T> configOption) {
    return getStringOrDefault(props, configOption);
  }

  public <T> String getStringOrDefault(ConfigOption<T> configOption, String defaultVal) {
    return getStringOrDefault(props, configOption, defaultVal);
  }

  public <T> boolean contains(ConfigOption<T> configOption) {
    return contains(props, configOption);
  }
   */

  public void load(FSDataInputStream inputStream) throws IOException {
    props.load(inputStream);
  }

  public <T> String getStringOrThrow(ConfigOption<T> configOption, String errorMessage) throws HoodieException {
    Option<Object> rawValue = getRawValue(configOption);
    if (rawValue.isPresent()) {
      return rawValue.get().toString();
    } else {
      throw new HoodieException(errorMessage);
    }
  }
}
