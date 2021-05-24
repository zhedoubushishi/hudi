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

import org.apache.hudi.client.bootstrap.BootstrapMode;
import org.apache.hudi.client.bootstrap.selector.MetadataOnlyBootstrapModeSelector;
import org.apache.hudi.client.bootstrap.translator.IdentityBootstrapPartitionPathTranslator;
import org.apache.hudi.common.bootstrap.index.HFileBootstrapIndex;
import org.apache.hudi.common.config.ConfigOption;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.table.HoodieTableConfig;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * Bootstrap specific configs.
 */
public class HoodieBootstrapConfig extends HoodieConfig {

  public static final ConfigOption<String> BOOTSTRAP_BASE_PATH_PROP = ConfigOption
      .key("hoodie.bootstrap.base.path")
      .noDefaultValue()
      .withVersion("0.6.0")
      .withDocumentation("Base path of the dataset that needs to be bootstrapped as a Hudi table");

  public static final ConfigOption<String> BOOTSTRAP_MODE_SELECTOR = ConfigOption
      .key("hoodie.bootstrap.mode.selector")
      .defaultValue(MetadataOnlyBootstrapModeSelector.class.getCanonicalName())
      .withVersion("0.6.0")
      .withDocumentation("Selects the mode in which each file/partition in the bootstrapped dataset gets bootstrapped");

  public static final ConfigOption<String> FULL_BOOTSTRAP_INPUT_PROVIDER = ConfigOption
      .key("hoodie.bootstrap.full.input.provider")
      .defaultValue("org.apache.hudi.bootstrap.SparkParquetBootstrapDataProvider")
      .withVersion("0.6.0")
      .withDocumentation("Class to use for reading the bootstrap dataset partitions/files, for Bootstrap mode FULL_RECORD");

  public static final ConfigOption<String> BOOTSTRAP_KEYGEN_CLASS = ConfigOption
      .key("hoodie.bootstrap.keygen.class")
      .noDefaultValue()
      .withVersion("0.6.0")
      .withDocumentation("Key generator implementation to be used for generating keys from the bootstrapped dataset");

  public static final ConfigOption<String> BOOTSTRAP_PARTITION_PATH_TRANSLATOR_CLASS = ConfigOption
      .key("hoodie.bootstrap.partitionpath.translator.class")
      .defaultValue(IdentityBootstrapPartitionPathTranslator.class.getName())
      .withVersion("0.6.0")
      .withDocumentation("Translates the partition paths from the bootstrapped data into how is laid out as a Hudi table.");

  public static final ConfigOption<String> BOOTSTRAP_PARALLELISM = ConfigOption
      .key("hoodie.bootstrap.parallelism")
      .defaultValue("1500")
      .withVersion("0.6.0")
      .withDocumentation("Parallelism value to be used to bootstrap data into hudi");

  public static final ConfigOption<String> BOOTSTRAP_MODE_SELECTOR_REGEX = ConfigOption
      .key("hoodie.bootstrap.mode.selector.regex")
      .defaultValue(".*")
      .withVersion("0.6.0")
      .withDocumentation("Matches each bootstrap dataset partition against this regex and applies the mode below to it.");

  public static final ConfigOption<String> BOOTSTRAP_MODE_SELECTOR_REGEX_MODE = ConfigOption
      .key("hoodie.bootstrap.mode.selector.regex.mode")
      .defaultValue(BootstrapMode.METADATA_ONLY.name())
      .withVersion("0.6.0")
      .withDocumentation("Bootstrap mode to apply for partition paths, that match regex above. "
          + "METADATA_ONLY will generate just skeleton base files with keys/footers, avoiding full cost of rewriting the dataset. "
          + "FULL_RECORD will perform a full copy/rewrite of the data as a Hudi table.");

  public static final ConfigOption<String> BOOTSTRAP_INDEX_CLASS_PROP = ConfigOption
      .key("hoodie.bootstrap.index.class")
      .defaultValue(HFileBootstrapIndex.class.getName())
      .withVersion("0.6.0")
      .withDocumentation("");

  public HoodieBootstrapConfig(Properties props) {
    super(props);
  }

  public static Builder newBuilder() {
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

    public Builder withBootstrapBasePath(String basePath) {
      set(props, BOOTSTRAP_BASE_PATH_PROP, basePath);
      return this;
    }

    public Builder withBootstrapModeSelector(String partitionSelectorClass) {
      set(props, BOOTSTRAP_MODE_SELECTOR, partitionSelectorClass);
      return this;
    }

    public Builder withFullBootstrapInputProvider(String partitionSelectorClass) {
      set(props, FULL_BOOTSTRAP_INPUT_PROVIDER, partitionSelectorClass);
      return this;
    }

    public Builder withBootstrapKeyGenClass(String keyGenClass) {
      set(props, BOOTSTRAP_KEYGEN_CLASS, keyGenClass);
      return this;
    }

    public Builder withBootstrapPartitionPathTranslatorClass(String partitionPathTranslatorClass) {
      set(props, BOOTSTRAP_PARTITION_PATH_TRANSLATOR_CLASS, partitionPathTranslatorClass);
      return this;
    }

    public Builder withBootstrapParallelism(int parallelism) {
      set(props, BOOTSTRAP_PARALLELISM, String.valueOf(parallelism));
      return this;
    }

    public Builder withBootstrapModeSelectorRegex(String regex) {
      set(props, BOOTSTRAP_MODE_SELECTOR_REGEX, regex);
      return this;
    }

    public Builder withBootstrapModeForRegexMatch(BootstrapMode modeForRegexMatch) {
      set(props, BOOTSTRAP_MODE_SELECTOR_REGEX_MODE, modeForRegexMatch.name());
      return this;
    }

    public Builder fromProperties(Properties props) {
      this.props.putAll(props);
      return this;
    }

    public HoodieBootstrapConfig build() {
      HoodieBootstrapConfig config = new HoodieBootstrapConfig(props);
      setDefaultValue(props, BOOTSTRAP_PARALLELISM);
      setDefaultValue(props, BOOTSTRAP_PARTITION_PATH_TRANSLATOR_CLASS);
      setDefaultValue(props, BOOTSTRAP_MODE_SELECTOR);
      setDefaultValue(props, BOOTSTRAP_MODE_SELECTOR_REGEX);
      setDefaultValue(props, BOOTSTRAP_MODE_SELECTOR_REGEX_MODE);
      setDefaultValue(props, BOOTSTRAP_INDEX_CLASS_PROP, HoodieTableConfig.getDefaultBootstrapIndexClass(props));
      setDefaultValue(props, FULL_BOOTSTRAP_INPUT_PROVIDER);
      return config;
    }
  }
}
