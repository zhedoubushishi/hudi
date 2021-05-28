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

import java.util.List;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;

import java.io.Serializable;
import java.util.function.Function;
import java.util.Map;
import java.util.Objects;
import sun.security.krb5.Config;

/**
 * ConfigOption describes a configuration parameter. It contains the configuration
 * key, deprecated older versions of the key, and an optional default value for the configuration,
 * configuration descriptions and also the an infer mechanism to infer the configuration value
 * based on other configurations.
 *
 * @param <T> The type of the default value.
 */
public class ConfigOption<T> implements Serializable {

  private final String key;

  private final T defaultValue;

  private final String doc;

  private final Option<String> version;

  private final String[] alternatives;

  // provide the ability to infer config value based on other configs
  private final Option<Function<Map, Option<T>>> inferFunction;

  ConfigOption(String key, T defaultValue, String doc, Option<String> version,
      Option<Function<Map, Option<T>>> inferFunc, String... alternatives) {
    this.key = Objects.requireNonNull(key);
    this.defaultValue = defaultValue;
    this.doc = doc;
    this.version = version;
    this.inferFunction = inferFunc;
    this.alternatives = alternatives;
  }

  public String key() {
    return key;
  }

  public T defaultValue() {
    if (defaultValue == null) {
      throw new HoodieException("There's no default value for this config");
    }
    return defaultValue;
  }

  public boolean hasDefaultValue() {
    return defaultValue != null;
  }

  Option<Function<Map, Option<T>>> getInferFunc() {
    return inferFunction;
  }

  public String[] getAlternatives() {
    return alternatives;
  }

  public ConfigOption<T> withDocumentation(String doc) {
    Objects.requireNonNull(doc);
    return new ConfigOption<>(key, defaultValue, doc, version, inferFunction, alternatives);
  }

  public ConfigOption<T> withAlternatives(String... alternatives) {
    Objects.requireNonNull(alternatives);
    return new ConfigOption<>(key, defaultValue, doc, version, inferFunction, alternatives);
  }

  public ConfigOption<T> sinceVersion(String version) {
    Objects.requireNonNull(version);
    return new ConfigOption<>(key, defaultValue, doc, Option.of(version), inferFunction, alternatives);
  }

  public ConfigOption<T> withInferFunction(Function<Map, Option<T>> inferFunction) {
    Objects.requireNonNull(inferFunction);
    return new ConfigOption<>(key, defaultValue, doc, version, Option.of(inferFunction), alternatives);
  }

  /**
   * Create a OptionBuilder with key.
   *
   * @param key The key of the option
   * @return Return a OptionBuilder.
   */
  public static ConfigOption.OptionBuilder key(String key) {
    Objects.requireNonNull(key);
    return new ConfigOption.OptionBuilder(key);
  }

  @Override
  public String toString() {
    return String.format(
        "Key: '%s' , default: %s description: %s version: %s)",
        key, defaultValue, doc, version.isPresent() ? version.get() : "version is not defined");
  }

  /**
   * The OptionBuilder is used to build the ConfigOption.
   */
  public static final class OptionBuilder {

    private final String key;

    OptionBuilder(String key) {
      this.key = key;
    }

    public <T> ConfigOption<T> defaultValue(T value) {
      Objects.requireNonNull(value);
      ConfigOption<T> configOption = new ConfigOption<>(key, value, "", Option.empty(), Option.empty());
      registerConfig(configOption);
      return configOption;
    }

    public ConfigOption<String> noDefaultValue() {
      ConfigOption<String> configOption = new ConfigOption<>(key, null, "", Option.empty(), Option.empty());
      registerConfig(configOption);
      return configOption;
    }

    private void registerConfig(ConfigOption configOption) {
      String configClassName = Thread.currentThread().getStackTrace()[3].getClassName();
      try {
        Class<?> caller = Class.forName(configClassName);
        List<ConfigOption<?>> configRegistry = (List<ConfigOption<?>>) caller.getDeclaredField("configRegistry").get(null);
        configRegistry.add(configOption);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}