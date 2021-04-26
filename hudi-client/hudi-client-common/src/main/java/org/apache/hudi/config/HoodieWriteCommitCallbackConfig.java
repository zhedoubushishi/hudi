/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * Write callback related config.
 */
public class HoodieWriteCommitCallbackConfig extends DefaultHoodieConfig {

  public static final String CALLBACK_PREFIX = "hoodie.write.commit.callback.";

  public static final ConfigOption<Boolean> CALLBACK_ON = ConfigOption
      .key(CALLBACK_PREFIX + "on")
      .defaultValue(false)
      .withDescription("");

  public static final ConfigOption<String> CALLBACK_CLASS_PROP = ConfigOption
      .key(CALLBACK_PREFIX + "class")
      .defaultValue("org.apache.hudi.callback.impl.HoodieWriteCommitHttpCallback")
      .withDescription("");

  // ***** HTTP callback configs *****
  public static final ConfigOption<String> CALLBACK_HTTP_URL_PROP = ConfigOption
      .key(CALLBACK_PREFIX + "http.url")
      .noDefaultValue()
      .withDescription("");

  public static final ConfigOption<String> CALLBACK_HTTP_API_KEY = ConfigOption
      .key(CALLBACK_PREFIX + "http.api.key")
      .defaultValue("hudi_write_commit_http_callback")
      .withDescription("");

  public static final ConfigOption<Integer> CALLBACK_HTTP_TIMEOUT_SECONDS = ConfigOption
      .key(CALLBACK_PREFIX + "http.timeout.seconds")
      .defaultValue(3)
      .withDescription("");

  private HoodieWriteCommitCallbackConfig(Properties props) {
    super(props);
  }

  public static HoodieWriteCommitCallbackConfig.Builder newBuilder() {
    return new HoodieWriteCommitCallbackConfig.Builder();
  }

  public static class Builder {

    private final Properties props = new Properties();

    public HoodieWriteCommitCallbackConfig.Builder fromFile(File propertiesFile) throws IOException {
      try (FileReader reader = new FileReader(propertiesFile)) {
        this.props.load(reader);
        return this;
      }
    }

    public HoodieWriteCommitCallbackConfig.Builder fromProperties(Properties props) {
      this.props.putAll(props);
      return this;
    }

    public HoodieWriteCommitCallbackConfig.Builder writeCommitCallbackOn(String callbackOn) {
      props.setProperty(CALLBACK_ON.key(), callbackOn);
      return this;
    }

    public HoodieWriteCommitCallbackConfig.Builder withCallbackClass(String callbackClass) {
      props.setProperty(CALLBACK_CLASS_PROP.key(), callbackClass);
      return this;
    }

    public HoodieWriteCommitCallbackConfig.Builder withCallbackHttpUrl(String url) {
      props.setProperty(CALLBACK_HTTP_URL_PROP.key(), url);
      return this;
    }

    public Builder withCallbackHttpTimeoutSeconds(String timeoutSeconds) {
      props.setProperty(CALLBACK_HTTP_TIMEOUT_SECONDS.key(), timeoutSeconds);
      return this;
    }

    public Builder withCallbackHttpApiKey(String apiKey) {
      props.setProperty(CALLBACK_HTTP_API_KEY.key(), apiKey);
      return this;
    }

    public HoodieWriteCommitCallbackConfig build() {
      HoodieWriteCommitCallbackConfig config = new HoodieWriteCommitCallbackConfig(props);
      setDefaultValue(props, CALLBACK_ON);
      setDefaultValue(props, CALLBACK_CLASS_PROP);
      setDefaultValue(props, CALLBACK_HTTP_API_KEY);
      setDefaultValue(props, CALLBACK_HTTP_TIMEOUT_SECONDS);

      return config;
    }
  }

}
