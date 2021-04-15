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
import java.util.function.Function;
import java.util.Objects;
import java.util.Properties;

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

  private final String description;

  private final String[] deprecatedNames;

  // provide the ability to infer config value based on other configs
  private final Function<Properties, T> inferFunction;

  ConfigOption(String key, T defaultValue, String description, Function<Properties, T> inferFunc, String... deprecatedNames) {
    this.key = Objects.requireNonNull(key);
    this.defaultValue = defaultValue;
    this.description = description;
    this.inferFunction = inferFunc;
    this.deprecatedNames = deprecatedNames;
  }

  public String key() {
    return key;
  }

  public T defaultValue() {
    return defaultValue;
  }

  Function<Properties, T> getInferFunc() {
    return inferFunction;
  }

  public ConfigOption<T> withDescription(String description) {
    return new ConfigOption<>(key, defaultValue, description, inferFunction, deprecatedNames);
  }

  public ConfigOption<T> withDeprecatedNames(String... deprecatedNames) {
    return new ConfigOption<>(key, defaultValue, description, inferFunction, deprecatedNames);
  }

  public ConfigOption<T> withInferFunction(Function<Properties, T> inferFunction) {
    return new ConfigOption<>(key, defaultValue, description, inferFunction, deprecatedNames);
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
      return new ConfigOption<>(key, value, "", null, "");
    }

    public ConfigOption<String> noDefaultValue() {
      return new ConfigOption<>(key, null, "", null, "");
    }
  }
}