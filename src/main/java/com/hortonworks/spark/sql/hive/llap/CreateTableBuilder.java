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

import com.hortonworks.spark.sql.hive.llap.util.HWCOptions;
import com.hortonworks.spark.sql.hive.llap.util.HiveQlUtil;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builder for constructing a Hive {@code CREATE TABLE} statement for HWC.
 */
public class CreateTableBuilder implements com.hortonworks.hwc.CreateTableBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(CreateTableBuilder.class);

  private final HiveWarehouseSession hiveWarehouseSession;
  private String databaseName;
  private final String tableName;
  private HiveWarehouseDataWriterHelper.FileFormat fileFormat;
  private HWCOptions hwcOptions;
  private boolean ifNotExists;
  private final List<Pair<String, String>> columnDefinitions = new ArrayList<>();
  private final List<Pair<String, String>> partitionDefinitions = new ArrayList<>();
  private final Map<String, String> optionMap = new HashMap<>();
  private final List<Pair<String, String>> tableProperties = new ArrayList<>();
  private String[] clusterColumns;
  private Long bucketCount;
  private boolean propagateException;

  /**
   * Creates a builder with the basic table definition information.
   *
   * @param hiveWarehouseSession active HWC session used for execution
   * @param databaseName target database name
   * @param tableName target table name
   * @param fileFormat storage format
   * @param hwcOptions options from the DataFrame writer
   */
  public CreateTableBuilder(HiveWarehouseSession hiveWarehouseSession,
                            String databaseName,
                            String tableName,
                            HiveWarehouseDataWriterHelper.FileFormat fileFormat,
                            HWCOptions hwcOptions) {
    this.hiveWarehouseSession = hiveWarehouseSession;
    this.tableName = tableName;
    this.fileFormat = fileFormat;
    this.databaseName = databaseName;
    this.hwcOptions = hwcOptions;
  }

  /**
   * Marks the CREATE TABLE as {@code IF NOT EXISTS}.
   *
   * @return this builder for chaining
   */
  @Override
  public CreateTableBuilder ifNotExists() {
    this.ifNotExists = true;
    return this;
  }

  /**
   * Propagates SQL exceptions rather than swallowing them.
   *
   * @return this builder for chaining
   */
  @Override
  public CreateTableBuilder propagateException() {
    this.propagateException = true;
    return this;
  }

  /**
   * Sets the database name for the table.
   *
   * @param databaseName database name
   * @return this builder for chaining
   */
  @Override
  public CreateTableBuilder database(String databaseName) {
    this.databaseName = databaseName;
    return this;
  }

  /**
   * Sets the file format for the table.
   *
   * @param fileFormat file format name
   * @return this builder for chaining
   */
  @Override
  public CreateTableBuilder fileFormat(String fileFormat) {
    this.fileFormat = HiveWarehouseDataWriterHelper.FileFormat.getFormat(fileFormat);
    return this;
  }

  /**
   * Adds a column definition.
   *
   * @param columnName column name
   * @param columnType column type
   * @return this builder for chaining
   */
  @Override
  public CreateTableBuilder column(String columnName, String columnType) {
    this.columnDefinitions.add(Pair.of(columnName, columnType));
    return this;
  }

  /**
   * Adds a partition definition.
   *
   * @param partitionName partition column name
   * @param partitionType partition column type
   * @return this builder for chaining
   */
  @Override
  public CreateTableBuilder partition(String partitionName, String partitionType) {
    this.partitionDefinitions.add(Pair.of(partitionName, partitionType));
    return this;
  }

  /**
   * Adds a table property.
   *
   * @param propertyName property key
   * @param propertyValue property value
   * @return this builder for chaining
   */
  @Override
  public CreateTableBuilder prop(String propertyName, String propertyValue) {
    this.tableProperties.add(Pair.of(propertyName, propertyValue));
    return this;
  }

  /**
   * Adds an option for the builder.
   *
   * @param optionName option key
   * @param optionValue option value
   * @return this builder for chaining
   */
  @Override
  public CreateTableBuilder option(String optionName, String optionValue) {
    this.optionMap.put(optionName, optionValue);
    return this;
  }

  /**
   * Sets the bucketing specification for the table.
   *
   * @param bucketCount number of buckets
   * @param clusterColumns columns to cluster on
   * @return this builder for chaining
   */
  @Override
  public CreateTableBuilder clusterBy(long bucketCount, String... clusterColumns) {
    this.bucketCount = bucketCount;
    this.clusterColumns = clusterColumns;
    return this;
  }

  /**
   * Executes the generated CREATE TABLE statement.
   */
  @Override
  public void create() {
    // Rebuild HWC options in case any options were added after construction.
    this.hwcOptions = new HWCOptions(this.optionMap);
    if (this.hiveWarehouseSession != null) {
      this.hiveWarehouseSession.executeUpdate(this.toString(), this.propagateException);
    }
  }

  @Override
  public String toString() {
    StringBuilder queryBuilder = new StringBuilder();
    queryBuilder.append(HiveQlUtil.createTablePrelude(this.databaseName, this.tableName, this.ifNotExists));

    if (!this.columnDefinitions.isEmpty()) {
      List<String> columnSpecs = new ArrayList<>();
      for (Pair<String, String> columnDefinition : this.columnDefinitions) {
        columnSpecs.add(columnDefinition.getKey() + " " + columnDefinition.getValue());
      }
      queryBuilder.append(HiveQlUtil.columnSpec(String.join(",", columnSpecs)));
    }

    if (!this.partitionDefinitions.isEmpty()) {
      List<String> partitionSpecs = new ArrayList<>();
      for (Pair<String, String> partitionDefinition : this.partitionDefinitions) {
        partitionSpecs.add(partitionDefinition.getKey() + " " + partitionDefinition.getValue());
      }
      queryBuilder.append(HiveQlUtil.partitionSpec(String.join(",", partitionSpecs)));
    }

    if (this.clusterColumns != null) {
      queryBuilder.append(HiveQlUtil.bucketSpec(String.join(",", this.clusterColumns), this.bucketCount));
    }

    if (this.fileFormat == HiveWarehouseDataWriterHelper.FileFormat.TEXT) {
      queryBuilder.append(HiveQlUtil.getRowFormatString(this.hwcOptions));
    }

    queryBuilder.append(" STORED AS ").append(this.fileFormat).append(' ');

    // Build transactional properties based on options and file format.
    List<Pair<String, String>> computedTableProperties = new ArrayList<>();
    computedTableProperties.add(Pair.of("transactional", this.hwcOptions.transactional));
    switch (this.hwcOptions.transactionalProperties) {
      case HWCOptions.NONE:
        if (this.fileFormat != HiveWarehouseDataWriterHelper.FileFormat.ORC) {
          computedTableProperties.add(Pair.of("transactional_properties", "insert_only"));
        }
        break;
      case HWCOptions.INSERT_ONLY:
        computedTableProperties.add(Pair.of("transactional_properties", "insert_only"));
        break;
      case HWCOptions.DEFAULT:
        if (this.fileFormat != HiveWarehouseDataWriterHelper.FileFormat.ORC) {
          throw new IllegalArgumentException(
              "Transactional property default can't be set for " + this.fileFormat + " format. ");
        }
        break;
      default:
        throw new IllegalArgumentException(
            "Invalid transactional property " + this.hwcOptions.transactionalProperties
                + " specified for " + this.tableName);
    }

    queryBuilder.append(HiveQlUtil.getTablePropertiesString(this.tableProperties, computedTableProperties));
    String query = queryBuilder.toString();
    LOG.debug("Builder String : {}", query);
    return query;
  }
}
