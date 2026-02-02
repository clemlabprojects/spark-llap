/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hortonworks.spark.sql.hive.llap.catalog;

import java.util.Map;
import org.apache.spark.sql.connector.catalog.Identifier;

/**
 * Spark catalog identifier backed by HWC options.
 */
public class HWCIdentifier implements Identifier {
  private final Map<String, String> options;

  /**
   * Creates an identifier from the provided options map.
   *
   * @param options options containing table and database information
   */
  public HWCIdentifier(Map<String, String> options) {
    this.options = options;
  }

  @Override
  public String[] namespace() {
    String tableName = options.getOrDefault("table", "");
    String[] tableParts = tableName.split("\\.");
    String databaseName = tableParts.length == 2
        ? tableParts[0]
        : (options.containsKey("database")
            ? options.get("database")
            : options.getOrDefault("default.db", "default"));
    return new String[] {databaseName};
  }

  @Override
  public String name() {
    String tableName = options.getOrDefault("table", "");
    String[] tableParts = tableName.split("\\.");
    return tableParts[tableParts.length - 1];
  }

  /**
   * Returns the underlying options map.
   *
   * @return options map
   */
  public Map<String, String> getOptions() {
    return options;
  }
}
