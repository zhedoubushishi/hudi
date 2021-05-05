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
      .withVersion("0.6.0")
      .withDescription("Turn callback on/off. off by default.");

  public static final ConfigOption<String> CALLBACK_CLASS_PROP = ConfigOption
      .key(CALLBACK_PREFIX + "class")
      .defaultValue("org.apache.hudi.callback.impl.HoodieWriteCommitHttpCallback")
      .withVersion("0.6.0")
      .withDescription("Full path of callback class and must be a subclass of HoodieWriteCommitCallback class, "
          + "org.apache.hudi.callback.impl.HoodieWriteCommitHttpCallback by default");

  // ***** HTTP callback configs *****
  public static final ConfigOption<String> CALLBACK_HTTP_URL_PROP = ConfigOption
      .key(CALLBACK_PREFIX + "http.url")
      .noDefaultValue()
      .withVersion("0.6.0")
      .withDescription("Callback host to be sent along with callback messages");

  public static final ConfigOption<String> CALLBACK_HTTP_API_KEY = ConfigOption
      .key(CALLBACK_PREFIX + "http.api.key")
      .defaultValue("hudi_write_commit_http_callback")
      .withVersion("0.6.0")
      .withDescription("Http callback API key. hudi_write_commit_http_callback by default");

  public static final ConfigOption<Integer> CALLBACK_HTTP_TIMEOUT_SECONDS = ConfigOption
      .key(CALLBACK_PREFIX + "http.timeout.seconds")
      .defaultValue(3)
      .withVersion("0.6.0")
      .withDescription("Callback timeout in seconds. 3 by default");

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
      set(props, CALLBACK_ON, callbackOn);
      return this;
    }

    public HoodieWriteCommitCallbackConfig.Builder withCallbackClass(String callbackClass) {
      set(props, CALLBACK_CLASS_PROP, callbackClass);
      return this;
    }

    public HoodieWriteCommitCallbackConfig.Builder withCallbackHttpUrl(String url) {
      set(props, CALLBACK_HTTP_URL_PROP, url);
      return this;
    }

    public Builder withCallbackHttpTimeoutSeconds(String timeoutSeconds) {
      set(props, CALLBACK_HTTP_TIMEOUT_SECONDS, timeoutSeconds);
      return this;
    }

    public Builder withCallbackHttpApiKey(String apiKey) {
      set(props, CALLBACK_HTTP_API_KEY, apiKey);
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
