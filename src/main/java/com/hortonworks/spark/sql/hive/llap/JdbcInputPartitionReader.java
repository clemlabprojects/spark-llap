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

import com.hortonworks.spark.sql.hive.llap.util.JobUtil;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Map;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Partition reader that executes a JDBC query and converts rows to Spark internal rows.
 */
public class JdbcInputPartitionReader implements PartitionReader<InternalRow> {
  private final String queryString;
  private final StructType resultSchema;
  private Connection connection;
  private PreparedStatement preparedStatement;
  private ResultSet resultSet;
  private final int columnCount;
  private final Object[] rowValues;
  private static final Logger LOG = LoggerFactory.getLogger(JdbcInputPartitionReader.class);

  /**
   * Create a reader for the given query.
   *
   * @param queryString SQL query to execute
   * @param optionMap data source options
   * @param resultSchema schema of the query output
   * @throws Exception when JDBC setup fails
   */
  JdbcInputPartitionReader(String queryString,
                           Map<String, String> optionMap,
                           StructType resultSchema) throws Exception {
    this.queryString = queryString;
    this.resultSchema = resultSchema;
    this.connection = getConnection(optionMap);
    this.preparedStatement = this.connection.prepareStatement(queryString);
    int maxRows = Integer.parseInt(HWConf.MAX_EXEC_RESULTS.getFromOptionsMap(optionMap));
    this.preparedStatement.setMaxRows(maxRows);
    String databaseName = HWConf.DEFAULT_DB.getFromOptionsMap(optionMap);
    useDatabase(this.connection, databaseName);
    this.resultSet = this.preparedStatement.executeQuery();
    this.columnCount = this.resultSet.getMetaData().getColumnCount();
    this.rowValues = new Object[this.columnCount];
    LOG.info("Execution via JDBC connection for query: {}", queryString);
  }

  /**
   * Create a JDBC connection using the provided options.
   *
   * @param optionMap data source options
   * @return JDBC connection
   * @throws SQLException when the connection cannot be created
   */
  private Connection getConnection(Map<String, String> optionMap) throws SQLException {
    JobUtil.replaceSparkHiveDriver();
    String jdbcUrl = optionMap.get("hs2.jdbc.url.for.executor");
    String user = HWConf.USER.getFromOptionsMap(optionMap);
    String password = HWConf.PASSWORD.getFromOptionsMap(optionMap);
    return DriverManager.getConnection(jdbcUrl, user, password);
  }

  /**
   * Set the active database for the current connection.
   *
   * @param connection JDBC connection
   * @param databaseName database to use
   * @throws SQLException when the USE command fails
   */
  private void useDatabase(Connection connection, String databaseName) throws SQLException {
    if (databaseName != null) {
      PreparedStatement useStatement = connection.prepareStatement("USE " + databaseName);
      useStatement.execute();
      useStatement.close();
    }
  }

  /**
   * Normalize JDBC values into Spark internal types.
   *
   * @param value raw JDBC value
   * @param dataType Spark data type for the column
   * @return normalized value
   */
  private Object normalizeColumnValue(Object value, DataType dataType) {
    if (value instanceof String) {
      value = UTF8String.fromString((String) value);
    } else if (value instanceof BigDecimal) {
      value = Decimal.apply((BigDecimal) value);
    } else if (value instanceof Double && dataType instanceof FloatType) {
      value = ((Double) value).floatValue();
    } else {
      if (value instanceof Date) {
        return (int) ((Date) value).toLocalDate().toEpochDay();
      }
      if (value instanceof Timestamp) {
        return DateTimeUtils.fromJavaTimestamp((Timestamp) value);
      }
    }
    return value;
  }

  /**
   * Close the result set if open.
   */
  private void closeResultSet() {
    try {
      if (resultSet != null && !resultSet.isClosed()) {
        resultSet.close();
      }
      resultSet = null;
    } catch (SQLException e) {
      LOG.warn("Failed to close ResultSet for query: {}", queryString);
    }
  }

  /**
   * Close the prepared statement if open.
   */
  private void closePreparedStatement() {
    try {
      if (preparedStatement != null && !preparedStatement.isClosed()) {
        preparedStatement.close();
      }
      preparedStatement = null;
    } catch (SQLException e) {
      LOG.warn("Failed to close PreparedStatement for query: {}", queryString);
    }
  }

  /**
   * Close the JDBC connection if open.
   */
  private void closeConnection() {
    LOG.debug("Closing JDBC connection");
    try {
      if (connection != null && !connection.isClosed()) {
        connection.close();
      }
      connection = null;
    } catch (SQLException e) {
      LOG.warn("Failed to close JDBC Connection for query: {}", queryString, e);
    }
  }

  @Override
  /**
   * {@inheritDoc}
   */
  public boolean next() throws IOException {
    try {
      boolean hasNext = resultSet.next();
      if (hasNext) {
        for (int i = 0; i < columnCount; ++i) {
          DataType dataType = null;
          if (resultSchema.size() > i) {
            dataType = resultSchema.apply(i).dataType();
          }
          rowValues[i] = normalizeColumnValue(resultSet.getObject(i + 1), dataType);
        }
      }
      return hasNext;
    } catch (SQLException e) {
      LOG.error("Failed to traverse the ResultSet for the query: {}", queryString);
      throw new IOException(e);
    }
  }

  @Override
  /**
   * {@inheritDoc}
   */
  public InternalRow get() {
    return new GenericInternalRow(rowValues);
  }

  @Override
  /**
   * {@inheritDoc}
   */
  public void close() throws IOException {
    LOG.info("Closing resources for the query: {}", queryString);
    closeResultSet();
    closePreparedStatement();
    closeConnection();
  }
}
