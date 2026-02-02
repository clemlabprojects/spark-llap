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

package com.hortonworks.spark.sql.hive.llap.streaming;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.spark.sql.connector.catalog.SessionConfigSupport;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * Spark DataSource V2 table for HWC streaming writes.
 */
public class HiveStreamingDataSource implements Table, SupportsWrite, SessionConfigSupport {
  private StructType tableSchema;
  private Map<String, String> options;

  /**
   * Infers schema from Spark options.
   *
   * @param options case-insensitive options map
   * @return table schema
   */
  @Override
  public StructType inferSchema(CaseInsensitiveStringMap options) {
    this.options = options.asCaseSensitiveMap();
    return schema();
  }

  /**
   * Creates a table definition for the given schema.
   *
   * @param schema table schema
   * @param transforms partition transforms
   * @param properties table properties
   * @return this table instance
   */
  @Override
  public Table getTable(StructType schema, Transform[] transforms, Map<String, String> properties) {
    this.tableSchema = schema;
    return this;
  }

  /**
   * Returns the name for this table implementation.
   *
   * @return table name
   */
  @Override
  public String name() {
    return getClass().toString();
  }

  /**
   * Returns the table schema.
   *
   * @return table schema
   */
  @Override
  public StructType schema() {
    return tableSchema;
  }

  /**
   * Returns supported table capabilities.
   *
   * @return capabilities set
   */
  @Override
  public Set<TableCapability> capabilities() {
    Set<TableCapability> capabilities = new HashSet<>();
    capabilities.add(TableCapability.STREAMING_WRITE);
    return capabilities;
  }

  /**
   * Builds a Spark write builder.
   *
   * @param logicalWriteInfo Spark write info
   * @return write builder
   */
  @Override
  public WriteBuilder newWriteBuilder(LogicalWriteInfo logicalWriteInfo) {
    return new HiveStreamingDataSourceStreamWriterBuilder(logicalWriteInfo);
  }

  /**
   * Returns the config key prefix for Spark options.
   *
   * @return key prefix
   */
  @Override
  public String keyPrefix() {
    return "hive.warehouse";
  }
}
