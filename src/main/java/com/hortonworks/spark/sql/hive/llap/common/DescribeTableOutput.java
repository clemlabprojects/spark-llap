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

package com.hortonworks.spark.sql.hive.llap.common;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Holds the parsed output of a DESCRIBE TABLE operation.
 */
public class DescribeTableOutput {
  private List<Column> columns;
  private List<Column> partitionedColumns;
  private List<Column> detailedTableInfoColumns;
  private List<Column> storageInfoColumns;
  private boolean considerDefaultCols = false;

  /**
   * Returns the table columns, excluding default columns if configured.
   *
   * @return list of columns
   */
  public List<Column> getColumns() {
    if (considerDefaultCols) {
      return columns.stream().filter(Column::isNotDefault).collect(Collectors.toList());
    }
    return columns;
  }

  /**
   * Sets the column list.
   *
   * @param columns column list
   */
  public void setColumns(List<Column> columns) {
    this.columns = columns;
  }

  /**
   * Returns partitioned columns.
   *
   * @return partition columns
   */
  public List<Column> getPartitionedColumns() {
    return partitionedColumns;
  }

  /**
   * Sets partitioned columns.
   *
   * @param partitionedColumns partition column list
   */
  public void setPartitionedColumns(List<Column> partitionedColumns) {
    this.partitionedColumns = partitionedColumns;
  }

  /**
   * Returns detailed table info columns.
   *
   * @return detailed table info columns
   */
  public List<Column> getDetailedTableInfoColumns() {
    return detailedTableInfoColumns;
  }

  /**
   * Returns storage info columns.
   *
   * @return storage info columns
   */
  public List<Column> getStorageInfoColumns() {
    return storageInfoColumns;
  }

  /**
   * Sets detailed table info columns.
   *
   * @param detailedTableInfoColumns detailed table info columns
   */
  public void setDetailedTableInfoColumns(List<Column> detailedTableInfoColumns) {
    this.detailedTableInfoColumns = detailedTableInfoColumns;
  }

  /**
   * Sets storage info columns.
   *
   * @param storageInfoColumns storage info columns
   */
  public void setStorageInfoColumns(List<Column> storageInfoColumns) {
    this.storageInfoColumns = storageInfoColumns;
  }

  /**
   * Marks columns that have default values based on the given list.
   *
   * @param defaultColumns column names with defaults
   */
  public void setDefaultColumns(List<String> defaultColumns) {
    // Identify columns with default values and flip the filter flag.
    for (Column column : columns) {
      if (!defaultColumns.stream().anyMatch(name -> name.equalsIgnoreCase(column.getName()))) {
        continue;
      }
      column.setDefault(true);
      considerDefaultCols = true;
    }
  }

  /**
   * Indicates whether default columns should be excluded.
   *
   * @return {@code true} if default columns are excluded
   */
  public boolean considerDefaultCols() {
    return considerDefaultCols;
  }

  /**
   * Sets whether default columns are excluded.
   *
   * @param considerDefaultCols toggle for excluding default columns
   */
  public void setConsiderDefaultCols(boolean considerDefaultCols) {
    this.considerDefaultCols &= considerDefaultCols;
  }

  /**
   * Finds a storage-info column by name.
   *
   * @param columnName column name
   * @param throwIfMissing whether to throw when missing
   * @return column or {@code null} when missing and not throwing
   */
  public Column findByColNameInStorageInfo(String columnName, boolean throwIfMissing) {
    return findByColName(storageInfoColumns, columnName, throwIfMissing);
  }

  /**
   * Finds a storage-info column by data type name.
   *
   * @param dataTypeName data type name
   * @param throwIfMissing whether to throw when missing
   * @return column or {@code null} when missing and not throwing
   */
  public Column findByDatatypeNameInStorageInfo(String dataTypeName, boolean throwIfMissing) {
    return findByDataTypeName(storageInfoColumns, dataTypeName, throwIfMissing);
  }

  private Column findByColName(List<Column> columns, String columnName, boolean throwIfMissing) {
    Optional<Column> match = columns.stream()
      .filter(column -> column.getName().equalsIgnoreCase(columnName))
      .findFirst();
    if (match.isPresent()) {
      return match.get();
    }
    if (throwIfMissing) {
      throw new IllegalArgumentException(
        "Column with name: " + columnName + " cannot be found in describe table output");
    }
    return null;
  }

  private Column findByDataTypeName(List<Column> columns, String dataTypeName, boolean throwIfMissing) {
    Optional<Column> match = Optional.empty();
    for (Column column : columns) {
      if (column.getDataType() == null || !column.getDataType().equalsIgnoreCase(dataTypeName)) {
        continue;
      }
      match = Optional.of(column);
      break;
    }
    if (match.isPresent()) {
      return match.get();
    }
    if (throwIfMissing) {
      throw new IllegalArgumentException(
        "Column with dataType name: " + dataTypeName + " cannot be found in describe table output");
    }
    return null;
  }

  @Override
  public String toString() {
    return "DescribeTableOutput{columns=" + columns +
      ", partitionedColumns=" + partitionedColumns +
      ", detailedTableInfoColumns=" + detailedTableInfoColumns +
      ", storageInfoColumns=" + storageInfoColumns + '}';
  }
}
