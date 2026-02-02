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

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility helpers for building HiveQL statements and formatting rows.
 */
public class HiveQlUtil {
  private static final Logger LOG = LoggerFactory.getLogger(HiveQlUtil.class);
  public static final String HIVE_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
  private static SimpleDateFormat timestampFormatter = null;

  /**
   * Builds a projection list with backticks around column names.
   *
   * @param columnNames column names to project
   * @return projection string
   */
  public static String projections(String[] columnNames) {
    return "`" + String.join("` , `", columnNames) + "`";
  }

  /**
   * Creates a SELECT * statement for a fully qualified table.
   *
   * @param databaseName database name
   * @param tableName table name
   * @return SELECT * statement
   */
  public static String selectStar(String databaseName, String tableName) {
    return String.format("SELECT * FROM %s.%s", databaseName, tableName);
  }

  /**
   * Creates a SELECT * statement for a table name.
   *
   * @param tableName table name
   * @return SELECT * statement
   */
  public static String selectStar(String tableName) {
    return String.format("SELECT * FROM %s", tableName);
  }

  /**
   * Wraps a query with a projection, alias, and optional filter.
   *
   * @param projectionSql projection clause
   * @param querySql source query
   * @param aliasName alias for the subquery
   * @param whereClause optional WHERE clause
   * @return formatted query
   */
  public static String selectProjectAliasFilter(String projectionSql, String querySql, String aliasName, String whereClause) {
    return String.format("select %s from (%s) as %s %s", projectionSql, querySql, aliasName, whereClause);
  }

  /**
   * Builds a USE database statement.
   *
   * @param databaseName database name
   * @return USE statement
   */
  public static String useDatabase(String databaseName) {
    return String.format("USE %s", databaseName);
  }

  /**
   * @return SHOW DATABASES statement
   */
  public static String showDatabases() {
    return "SHOW DATABASES";
  }

  /**
   * Builds a SHOW TABLES statement.
   *
   * @param databaseName database name
   * @return SHOW TABLES statement
   */
  public static String showTables(String databaseName) {
    return String.format("SHOW TABLES IN %s", databaseName);
  }

  /**
   * Builds a DESCRIBE statement for a table.
   *
   * @param databaseName database name
   * @param tableName table name
   * @return DESCRIBE statement
   */
  public static String describeTable(String databaseName, String tableName) {
    return String.format("DESCRIBE %s.%s", databaseName, tableName);
  }

  /**
   * Builds a DROP DATABASE statement.
   *
   * @param databaseName database name
   * @param ifExists whether to include IF EXISTS
   * @param cascade whether to include CASCADE
   * @return DROP DATABASE statement
   */
  public static String dropDatabase(String databaseName, boolean ifExists, boolean cascade) {
    return String.format("DROP DATABASE %s %s %s", orBlank(ifExists, "IF EXISTS"), databaseName,
      orBlank(cascade, "CASCADE"));
  }

  /**
   * Builds a DROP TABLE statement.
   *
   * @param tableName table name
   * @param ifExists whether to include IF EXISTS
   * @param purge whether to include PURGE
   * @return DROP TABLE statement
   */
  public static String dropTable(String tableName, boolean ifExists, boolean purge) {
    return String.format("DROP TABLE %s %s %s", orBlank(ifExists, "IF EXISTS"), tableName,
      orBlank(purge, "PURGE"));
  }

  /**
   * Builds a CREATE DATABASE statement.
   *
   * @param databaseName database name
   * @param ifNotExists whether to include IF NOT EXISTS
   * @return CREATE DATABASE statement
   */
  public static String createDatabase(String databaseName, boolean ifNotExists) {
    return String.format("CREATE DATABASE %s %s", orBlank(ifNotExists, "IF NOT EXISTS"), databaseName);
  }

  /**
   * Wraps a column specification in parentheses.
   *
   * @param columnSpecSql column specification
   * @return formatted column spec
   */
  public static String columnSpec(String columnSpecSql) {
    return String.format(" (%s) ", columnSpecSql);
  }

  /**
   * Wraps a partition specification in PARTITIONED BY clause.
   *
   * @param partitionSpecSql partition specification
   * @return formatted partition spec
   */
  public static String partitionSpec(String partitionSpecSql) {
    return String.format(" PARTITIONED BY(%s) ", partitionSpecSql);
  }

  /**
   * Builds a bucketing clause.
   *
   * @param bucketColumns bucket columns
   * @param numBuckets number of buckets
   * @return formatted bucketing clause
   */
  public static String bucketSpec(String bucketColumns, long numBuckets) {
    return String.format(" CLUSTERED BY (%s) INTO %s BUCKETS ", bucketColumns, numBuckets);
  }

  /**
   * Wraps table properties.
   *
   * @param propertiesSql table properties string
   * @return formatted table properties clause
   */
  public static String tblProperties(String propertiesSql) {
    return String.format(" TBLPROPERTIES (%s) ", propertiesSql);
  }

  /**
   * Builds a CREATE TABLE prelude for a fully qualified table.
   *
   * @param databaseName database name
   * @param tableName table name
   * @param ifNotExists whether to include IF NOT EXISTS
   * @return CREATE TABLE prelude
   */
  public static String createTablePrelude(String databaseName, String tableName, boolean ifNotExists) {
    return String.format("CREATE TABLE %s %s.%s ", orBlank(ifNotExists, "IF NOT EXISTS"), databaseName, tableName);
  }

  /**
   * Builds the row format clause based on HWC options.
   *
   * @param options HWC options
   * @return row format clause
   */
  public static String getRowFormatString(HWCOptions options) {
    String fieldDelimiter = options.fieldDelimiter != null ? options.fieldDelimiter : ",";
    String lineDelimiter = options.lineDelimiter != null ? options.lineDelimiter : "\\n";
    if (fieldDelimiter.length() == 1) {
      return " ROW FORMAT DELIMITED FIELDS TERMINATED BY '" + fieldDelimiter +
        "' LINES TERMINATED BY '" + lineDelimiter + "'";
    }
    return " ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe' " +
      "WITH SERDEPROPERTIES ('field.delim'='" + fieldDelimiter + "') ";
  }

  /**
   * Builds table properties from two lists of key/value pairs.
   *
   * @param properties primary properties
   * @param additionalProperties additional properties
   * @return formatted table properties clause
   */
  public static String getTablePropertiesString(List<Pair<String, String>> properties,
                                                List<Pair<String, String>> additionalProperties) {
    ArrayList<String> propertyEntries = new ArrayList<>();
    for (Pair<String, String> propertyEntry : properties) {
      propertyEntries.add(String.format("\"%s\"=\"%s\"", propertyEntry.getKey(), propertyEntry.getValue()));
    }
    for (Pair<String, String> propertyEntry : additionalProperties) {
      propertyEntries.add(String.format("\"%s\"=\"%s\"", propertyEntry.getKey(), propertyEntry.getValue()));
    }
    return tblProperties(String.join(",", propertyEntries));
  }

  private static String orBlank(boolean condition, String token) {
    return condition ? token : "";
  }

  /**
   * Builds a LOAD DATA statement.
   *
   * @param path data path
   * @param databaseName database name
   * @param tableName table name
   * @param overwrite whether to overwrite
   * @param partitionSpec partition spec
   * @return LOAD DATA statement
   */
  public static String loadInto(String path, String databaseName, String tableName, boolean overwrite,
                                String partitionSpec) {
    StringBuilder statementBuilder = new StringBuilder("LOAD DATA INPATH ")
      .append(wrapWithSingleQuotes(path));
    if (overwrite) {
      statementBuilder.append(" OVERWRITE ");
    }
    statementBuilder.append(" INTO TABLE ").append(databaseName).append(".").append(tableName);
    if (StringUtils.isNotBlank(partitionSpec)) {
      statementBuilder.append(" PARTITION (").append(partitionSpec).append(")");
    }
    return statementBuilder.toString();
  }

  /**
   * Wraps a value in single quotes.
   *
   * @param value string value
   * @return quoted value
   */
  public static String wrapWithSingleQuotes(String value) {
    return "'" + value + "'";
  }

  /**
   * Normalizes an SQL alias by replacing invalid characters.
   *
   * @param rawAlias alias to normalize
   * @return normalized alias
   */
  public static String getCleanSqlAlias(String rawAlias) {
    return rawAlias.replaceAll("[^A-Za-z0-9]", "_");
  }

  /**
   * Generates a random alias safe for use in HiveQL.
   *
   * @return random alias
   */
  public static String randomAlias() {
    return "q_" + UUID.randomUUID().toString().replaceAll("[^A-Za-z0-9 ]", "");
  }

  /**
   * Escapes a value for HiveQL.
   *
   * @param value raw value
   * @param escapeChar escape character
   * @return escaped value
   */
  public static String escapeForQl(Object value, String escapeChar) {
    return value == null ? null : StringUtils.replace(value.toString(), ",", escapeChar + ",");
  }

  /**
   * Formats a record into an object array, applying HiveQL escape rules.
   *
   * @param schemaDefinition schema definition
   * @param internalRow Spark internal row
   * @param escapeChar escape character
   * @param quoteChar quote character
   * @return formatted values
   */
  @Deprecated
  public static Object[] formatRecord(StructType schemaDefinition, InternalRow internalRow,
                                      String escapeChar, String quoteChar) {
    StructField[] schemaFields = schemaDefinition.fields();
    Object[] fieldValues = new Object[internalRow.numFields()];
    for (int i = 0; i < internalRow.numFields(); ++i) {
      DataType fieldDataType = schemaFields[i].dataType();
      Object fieldValue = internalRow.get(i, fieldDataType);
      if (DataTypes.StringType.equals(fieldDataType) && fieldValue != null) {
        if (escapeChar != null) {
          fieldValue = StringUtils.replace(fieldValue.toString(), ",", escapeChar + ",");
        }
        if (quoteChar != null) {
          fieldValue = quoteChar + fieldValue + quoteChar;
        }
        fieldValue = UTF8String.fromString(fieldValue.toString());
      } else if (DataTypes.TimestampType.equals(fieldDataType) && fieldValue instanceof Long) {
        long timestampMillis = TimeUnit.MILLISECONDS.convert((Long) fieldValue, TimeUnit.MICROSECONDS);
        Date timestampDate = new Date(timestampMillis);
        if (timestampFormatter == null) {
          timestampFormatter = new SimpleDateFormat(HIVE_DATE_FORMAT);
        }
        fieldValue = UTF8String.fromString(timestampFormatter.format(timestampDate));
      }
      fieldValues[i] = fieldValue;
    }
    return fieldValues;
  }
}
