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

package com.hortonworks.spark.sql.hive.llap.util;

import com.hortonworks.spark.sql.hive.llap.query.builder.CreateTableBuilder;
import com.hortonworks.spark.sql.hive.llap.writers.HiveWarehouseDataWriterHelper;
import java.util.HashMap;
import org.apache.spark.sql.types.CalendarIntervalType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.NullType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Schema utilities for mapping Spark types to Hive types and building table definitions.
 */
public class SchemaUtil {
  private static final String HIVE_TYPE_STRING = "HIVE_TYPE_STRING";

  /**
   * Returns column names for a Spark schema.
   *
   * @param schemaDefinition Spark schema
   * @return column names array
   */
  public static String[] columnNames(StructType schemaDefinition) {
    String[] columnNames = new String[schemaDefinition.length()];
    int columnIndex = 0;
    for (StructField structField : schemaDefinition.fields()) {
      columnNames[columnIndex] = structField.name();
      columnIndex++;
    }
    return columnNames;
  }

  /**
   * Builds a Hive CREATE TABLE statement from a Spark DataFrame schema.
   *
   * @param schemaDefinition Spark schema
   * @param databaseName database name
   * @param tableName table name
   * @return CREATE TABLE statement
   */
  public static String buildHiveCreateTableQueryFromSparkDFSchema(StructType schemaDefinition,
                                                                  String databaseName,
                                                                  String tableName) {
    CreateTableBuilder createTableBuilder = new CreateTableBuilder(
      null,
      databaseName,
      tableName,
      HiveWarehouseDataWriterHelper.FileFormat.ORC,
      new HWCOptions(new HashMap<>()));
    for (StructField structField : schemaDefinition.fields()) {
      createTableBuilder.column(structField.name(), getHiveType(structField.dataType(), structField.metadata()));
    }
    return createTableBuilder.toString();
  }

  /**
   * Maps a Spark data type and metadata to a Hive type string.
   *
   * @param dataType Spark data type
   * @param metadata Spark metadata
   * @return Hive type string
   */
  public static String getHiveType(DataType dataType, Metadata metadata) {
    if (dataType instanceof MapType
      || dataType instanceof CalendarIntervalType
      || dataType instanceof NullType) {
      throw new IllegalArgumentException("Data type not supported currently: " + dataType);
    }
    String hiveTypeString = dataType.catalogString();
    if (dataType instanceof StringType && metadata != null && metadata.contains(HIVE_TYPE_STRING)) {
      hiveTypeString = metadata.getString(HIVE_TYPE_STRING);
    }
    return hiveTypeString;
  }

  /**
   * Parses a table name into database and table components.
   *
   * @param databaseName database name to use if table is unqualified
   * @param tableName table name, optionally qualified
   * @return table reference
   */
  public static TableRef getDbTableNames(String databaseName, String tableName) {
    String[] tableNameParts = tableName.split("\\.");
    if (tableNameParts.length == 1) {
      return new TableRef(databaseName, tableName);
    }
    if (tableNameParts.length == 2) {
      return new TableRef(tableNameParts[0], tableNameParts[1]);
    }
    throw new IllegalArgumentException("Table name should be specified as either <table> or <db.table>");
  }

  /**
   * Simple table reference holder.
   */
  public static class TableRef {
    public final String databaseName;
    public final String tableName;

    /**
     * Creates a table reference.
     *
     * @param databaseName database name
     * @param tableName table name
     */
    public TableRef(String databaseName, String tableName) {
      this.databaseName = databaseName;
      this.tableName = tableName;
    }

    /**
     * @return fully qualified name
     */
    public String getFullyQualifiedName() {
      return databaseName + "." + tableName;
    }
  }
}
