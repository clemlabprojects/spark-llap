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

package com.hortonworks.spark.sql.hive.llap;

import java.util.Map;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.types.StructType;

/**
 * Input partition for a single JDBC query.
 */
public class JdbcInputPartition implements InputPartition {
  private final String queryString;
  private final Map<String, String> optionMap;
  private final StructType schema;

  /**
   * No-arg constructor for serialization frameworks.
   */
  public JdbcInputPartition() {
    this.queryString = null;
    this.optionMap = null;
    this.schema = null;
  }

  /**
   * Create a JDBC input partition.
   *
   * @param queryString SQL query for this partition
   * @param optionMap data source options
   * @param schema schema for the returned rows
   */
  JdbcInputPartition(String queryString, Map<String, String> optionMap, StructType schema) {
    this.queryString = queryString;
    this.optionMap = optionMap;
    this.schema = schema;
  }

  /**
   * @return SQL query for this partition
   */
  String getQueryString() {
    return queryString;
  }

  /**
   * @return data source options
   */
  Map<String, String> getOptionMap() {
    return optionMap;
  }

  /**
   * @return schema for the returned rows
   */
  StructType getSchema() {
    return schema;
  }
}
