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

package com.hortonworks.hwc;

/**
 * Fluent builder for creating Hive tables via HWC.
 */
public interface CreateTableBuilder {

  /**
   * Adds IF NOT EXISTS to the CREATE TABLE statement.
   *
   * @return this builder for chaining
   */
  CreateTableBuilder ifNotExists();

  /**
   * Propagates execution exceptions to the caller instead of swallowing them.
   *
   * @return this builder for chaining
   */
  CreateTableBuilder propagateException();

  /**
   * Sets the database name.
   *
   * @param name database name
   * @return this builder for chaining
   */
  CreateTableBuilder database(String name);

  /**
   * Sets the storage format for the table.
   *
   * @param format file format (for example, ORC, Parquet)
   * @return this builder for chaining
   */
  CreateTableBuilder fileFormat(String format);

  /**
   * Adds a column to the table schema.
   *
   * @param name column name
   * @param type column type
   * @return this builder for chaining
   */
  CreateTableBuilder column(String name, String type);

  /**
   * Adds a partition column to the table schema.
   *
   * @param name partition column name
   * @param type partition column type
   * @return this builder for chaining
   */
  CreateTableBuilder partition(String name, String type);

  /**
   * Adds a table option.
   *
   * @param key option key
   * @param value option value
   * @return this builder for chaining
   */
  CreateTableBuilder option(String key, String value);

  /**
   * Adds a table property.
   *
   * @param key property key
   * @param value property value
   * @return this builder for chaining
   */
  CreateTableBuilder prop(String key, String value);

  /**
   * Adds a bucket specification for the table.
   *
   * @param numBuckets number of buckets
   * @param columns columns to bucket by
   * @return this builder for chaining
   */
  CreateTableBuilder clusterBy(long numBuckets, String... columns);

  /**
   * Executes the CREATE TABLE statement.
   */
  void create();
}
