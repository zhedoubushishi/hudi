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

  // singleton mode
  private static final DFSPropertiesConfiguration INSTANCE = new DFSPropertiesConfiguration();

  private TypedProperties externalProps;

  // Keep track of files visited, to detect loops
  private Set<String> visitedFiles;

  private Path currentFilePath;  // TODO get last element of visitedFiles

  private DFSPropertiesConfiguration() {
    this.externalProps = new TypedProperties();
    this.visitedFiles = new HashSet<>();
    this.currentFilePath = null;
    addPropsFromDefaultConfigFile();
  }

  public static DFSPropertiesConfiguration getInstance() {
    return INSTANCE;
  }

  private String[] splitProperty(String line) {
    line = line.trim();
    String delimiter = line.contains("=") ? "=" : "\\s+";
    return new String[] { line.split(delimiter)[0].trim(), line.split(delimiter)[1].trim()};
  }

  public void addPropsFromDefaultConfigFile() {
    try {
      Path defaultConfPath = getDefaultConfPath();
      FileSystem fs = defaultConfPath.getFileSystem(new Configuration());
      addPropsFromFile(fs, defaultConfPath);
    } catch (IOException e) {
      LOG.warn("Failed to load properties from " + DEFAULT_PROPERTIES_FILE);
    }
  }

  /**
   * Add properties from external configuration files.
   *
   * @param filePath File path for configuration file
   */
  public void addPropsFromFile(FileSystem fs, Path filePath) {
    try {
      if (visitedFiles.contains(filePath.getName())) {   // TODO name is not enough
        LOG.info(filePath + " already referenced, skipped");
        return;
        // throw new IllegalStateException("Loop detected; file " + file + " already referenced");
      }
      currentFilePath = filePath;
      visitedFiles.add(filePath.getName());
      BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(filePath)));
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
        System.out.println("wenningd => " + line);
        String[] split = splitProperty(line);
        if (line.startsWith("include=") || line.startsWith("include =")) {
          Path includeFilePath = new Path(currentFilePath.getParent(), split[1]);
          addPropsFromFile(includeFilePath.getFileSystem(new Configuration()), includeFilePath);
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

  private Path getDefaultConfPath() throws IOException {
    String confDir = System.getenv("HUDI_CONF_DIR");
    if (confDir == null) {
      LOG.warn("Cannot find HUDI_CONF_DIR, please set it as the dir of " + DEFAULT_PROPERTIES_FILE);
      throw new IOException("HUDI_CONF_DIR is not set");
    }
    return new Path(confDir + File.separator + DEFAULT_PROPERTIES_FILE);
  }

  public void clean() {
    externalProps = new TypedProperties();
    visitedFiles = new HashSet<>();
    currentFilePath = null;
  }

  private boolean isValidLine(String line) {
    if (line.startsWith("#") || line.equals("")) {
      return false;
    }
    return line.contains("=") || line.matches(".*\\s.*");
  }
}
