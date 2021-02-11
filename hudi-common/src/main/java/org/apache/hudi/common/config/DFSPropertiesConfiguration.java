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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

/**
 * A simplified versions of Apache commons - PropertiesConfiguration, that supports limited field types and hierarchical
 * configurations within the same folder as the root file.
 *
 * Includes denoted by the same include=filename.properties syntax, with relative path from root file's folder. Lines
 * beginning with '#' are ignored as comments. Final values for properties are resolved by the order in which they are
 * specified in the files, with included files treated as if they are inline.
 *
 * Note: Not reusing commons-configuration since it has too many conflicting runtime deps.
 */
public class DFSPropertiesConfiguration {

  private static final Logger LOG = LogManager.getLogger(DFSPropertiesConfiguration.class);

  private static final String DEFAULT_PROPERTIES_FILE = "hudi-defaults.conf";

  public static final String CONF_FILE_DIR_ENV_NAME = "HUDI_CONF_DIR";

  // props read from hudi-defaults.conf
  private static final TypedProperties GLOBAL_PROPS = loadGlobalProps();

  // props read from user defined configuration file or input stream
  private final TypedProperties externalProps;

  // Keep track of files visited, to detect loops
  private final Set<String> visitedFilePaths;

  private final FileSystem fs;

  private Path currentFilePath;

  public DFSPropertiesConfiguration(FileSystem fs, Path filePath) {
    this.externalProps = new TypedProperties();
    this.visitedFilePaths = new HashSet<>();
    this.fs = fs;
    this.currentFilePath = filePath;
    addPropsFromFile(filePath);
  }

  public DFSPropertiesConfiguration() {
    this.externalProps = new TypedProperties();
    this.visitedFilePaths = new HashSet<>();
    this.fs = null;
    this.currentFilePath = null;
  }

  /**
   * Load global props from hudi-defaults.conf which is under CONF_FILE_DIR_ENV_NAME folder.
   * @return Typed Properties
   */
  public static TypedProperties loadGlobalProps() {
    DFSPropertiesConfiguration conf = new DFSPropertiesConfiguration();
    Path defaultConfPath = getDefaultConfPath();
    if (defaultConfPath != null) {
      conf.addPropsFromFile(defaultConfPath);
    }
    return conf.getConfig();
  }

  /**
   * Add properties from external configuration files.
   *
   * @param filePath File path for configuration file
   */
  public void addPropsFromFile(Path filePath) {
    try {
      if (visitedFilePaths.contains(filePath.toString())) {
        throw new IllegalStateException("Loop detected; file " + filePath + " already referenced");
      }
      visitedFilePaths.add(filePath.toString());
      currentFilePath = filePath;
      FileSystem fileSystem = fs != null ? fs : filePath.getFileSystem(new Configuration());
      BufferedReader reader = new BufferedReader(new InputStreamReader(fileSystem.open(filePath)));
      addPropsFromInputStream(reader);
    } catch (IOException ioe) {
      LOG.error("Error reading in properties from dfs", ioe);
      throw new IllegalArgumentException("Cannot read properties from dfs", ioe);
    }
  }

  /**
   * Add properties from input stream.
   *
   * @param reader Buffered Reader
   * @throws IOException
   */
  public void addPropsFromInputStream(BufferedReader reader) throws IOException {
    try {
      String line;
      while ((line = reader.readLine()) != null) {
        if (!isValidLine(line)) {
          continue;
        }
        String[] split = splitProperty(line);
        if (line.startsWith("include=") || line.startsWith("include =")) {
          Path includeFilePath = new Path(currentFilePath.getParent(), split[1]);
          addPropsFromFile(includeFilePath);
        } else {
          externalProps.setProperty(split[0], split[1]);
        }
      }
    } finally {
      reader.close();
    }
  }

  public TypedProperties getConfig() {
    return externalProps;
  }

  public static TypedProperties getGlobalConfig() {
    final TypedProperties globalProps = new TypedProperties();
    globalProps.putAll(GLOBAL_PROPS);
    return globalProps;
  }

  private static Path getDefaultConfPath() {
    String confDir = System.getenv(CONF_FILE_DIR_ENV_NAME);
    if (confDir == null) {
      LOG.warn("Cannot find " + CONF_FILE_DIR_ENV_NAME + ", please set it as the dir of " + DEFAULT_PROPERTIES_FILE);
      return null;
    }
    return new Path(confDir + File.separator + DEFAULT_PROPERTIES_FILE);
  }

  private String[] splitProperty(String line) {
    line = line.replaceAll("\\s+"," ");
    String delimiter = line.contains("=") ? "=" : " ";
    int ind = line.indexOf(delimiter);
    String k = line.substring(0, ind).trim();
    String v = line.substring(ind + 1).trim();
    return new String[] {k, v};
  }

  private boolean isValidLine(String line) {
    if (line.startsWith("#") || line.equals("")) {
      return false;
    }
    return line.contains("=") || line.matches(".*\\s.*");
  }
}
