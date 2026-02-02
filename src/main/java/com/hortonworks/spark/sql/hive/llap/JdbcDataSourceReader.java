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

import java.util.Arrays;
import java.util.Map;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;
/**
 * Scan builder with filter and column pruning support for JDBC reads.
 */
public class JdbcDataSourceReader implements ScanBuilder,
  SupportsPushDownRequiredColumns,
  SupportsPushDownFilters {

  private StructType prunedSchema;
  private final StructType baseSchema;
  private Filter[] pushedFilterArray = new Filter[0];
  private final Map<String, String> optionMap;

  /**
   * Create a reader for the JDBC data source.
   *
   * @param schema full table schema
   * @param optionMap data source options
   */
  JdbcDataSourceReader(StructType schema, Map<String, String> optionMap) {
    this.optionMap = optionMap;
    this.baseSchema = schema;
    this.prunedSchema = schema;
  }

  @Override
  /**
   * {@inheritDoc}
   */
  public Filter[] pushFilters(Filter[] filters) {
    // Keep only filters that Spark can translate into Hive filter expressions.
    pushedFilterArray = Arrays.stream(filters)
      .filter(filter ->
        FilterPushdown$.MODULE$.buildFilterExpression(baseSchema, filter).isDefined())
      .toArray(Filter[]::new);
    return Arrays.stream(filters)
      .filter(filter ->
        !FilterPushdown$.MODULE$.buildFilterExpression(baseSchema, filter).isDefined())
      .toArray(Filter[]::new);
  }

  @Override
  /**
   * {@inheritDoc}
   */
  public Filter[] pushedFilters() {
    return pushedFilterArray;
  }

  @Override
  /**
   * {@inheritDoc}
   */
  public void pruneColumns(StructType requiredSchema) {
    this.prunedSchema = requiredSchema;
  }

  @Override
  /**
   * {@inheritDoc}
   */
  public Scan build() {
    return new JDBCDataSourceScan(baseSchema, prunedSchema, optionMap, pushedFilterArray);
  }
}
