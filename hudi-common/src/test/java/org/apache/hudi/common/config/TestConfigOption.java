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

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class TestConfigOption {

  private static ConfigOption<String> FAKE_STRING_CONFIG = ConfigOption
      .key("test.fake.string.config")
      .defaultValue("1")
      .withDescription("Fake config only for testing");

  private static ConfigOption<String> FAKE_BOOLEAN_CONFIG = ConfigOption
      .key("test.fake.boolean.config")
      .defaultValue("false")
      .withDescription("Fake config only for testing");

  @Test
  public void testGetTypedValue() {
    HoodieConfig hoodieConfig1 = new HoodieConfig();
    assertNull(hoodieConfig1.getInt(FAKE_STRING_CONFIG));
    hoodieConfig1.set(FAKE_STRING_CONFIG, "5");
    assertEquals(5, hoodieConfig1.getInt(FAKE_STRING_CONFIG));

    assertNull(hoodieConfig1.getBoolean(FAKE_BOOLEAN_CONFIG));
    hoodieConfig1.set(FAKE_BOOLEAN_CONFIG, "true");
    assertEquals(true, hoodieConfig1.getBoolean(FAKE_BOOLEAN_CONFIG));
  }

  @Test
  public void testGetOrDefault() {
    Properties props = new Properties();
    props.put("test.unknown.config", "abc");
    HoodieConfig hoodieConfig2 = new HoodieConfig(props);
    assertEquals("1", hoodieConfig2.getStringOrDefault(FAKE_STRING_CONFIG));
    assertEquals("2", hoodieConfig2.getStringOrDefault(FAKE_STRING_CONFIG, "2"));
  }
}
