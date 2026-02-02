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

package com.hortonworks.spark.sql.hive.llap.wrapper;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import org.apache.hive.jdbc.HivePreparedStatement;

/**
 * JDBC {@link PreparedStatement} wrapper that logs Hive query progress.
 */
public class PreparedStatementWrapper extends StatementWrapper implements PreparedStatement {
  private final PreparedStatement wrappedPreparedStatement;

  /**
   * Creates a wrapper around a JDBC prepared statement.
   *
   * @param preparedStatement underlying prepared statement
   */
  public PreparedStatementWrapper(PreparedStatement preparedStatement) {
    super(preparedStatement);
    this.wrappedPreparedStatement = preparedStatement;
  }

  /** {@inheritDoc} */
  @Override
  public ResultSet executeQuery() throws SQLException {
    HivePreparedStatement hivePreparedStatement = unwrap(HivePreparedStatement.class);
    startLogger(hivePreparedStatement);
    return wrappedPreparedStatement.executeQuery();
  }

  /** {@inheritDoc} */
  @Override
  public int executeUpdate() throws SQLException {
    HivePreparedStatement hivePreparedStatement = unwrap(HivePreparedStatement.class);
    startLogger(hivePreparedStatement);
    return wrappedPreparedStatement.executeUpdate();
  }

  /** {@inheritDoc} */
  @Override
  public boolean execute() throws SQLException {
    HivePreparedStatement hivePreparedStatement = unwrap(HivePreparedStatement.class);
    startLogger(hivePreparedStatement);
    return wrappedPreparedStatement.execute();
  }

  /** {@inheritDoc} */
  @Override
  public void setNull(int parameterIndex, int sqlType) throws SQLException {
    wrappedPreparedStatement.setNull(parameterIndex, sqlType);
  }

  /** {@inheritDoc} */
  @Override
  public void setBoolean(int parameterIndex, boolean value) throws SQLException {
    wrappedPreparedStatement.setBoolean(parameterIndex, value);
  }

  /** {@inheritDoc} */
  @Override
  public void setByte(int parameterIndex, byte value) throws SQLException {
    wrappedPreparedStatement.setByte(parameterIndex, value);
  }

  /** {@inheritDoc} */
  @Override
  public void setShort(int parameterIndex, short value) throws SQLException {
    wrappedPreparedStatement.setShort(parameterIndex, value);
  }

  /** {@inheritDoc} */
  @Override
  public void setInt(int parameterIndex, int value) throws SQLException {
    wrappedPreparedStatement.setInt(parameterIndex, value);
  }

  /** {@inheritDoc} */
  @Override
  public void setLong(int parameterIndex, long value) throws SQLException {
    wrappedPreparedStatement.setLong(parameterIndex, value);
  }

  /** {@inheritDoc} */
  @Override
  public void setFloat(int parameterIndex, float value) throws SQLException {
    wrappedPreparedStatement.setFloat(parameterIndex, value);
  }

  /** {@inheritDoc} */
  @Override
  public void setDouble(int parameterIndex, double value) throws SQLException {
    wrappedPreparedStatement.setDouble(parameterIndex, value);
  }

  /** {@inheritDoc} */
  @Override
  public void setBigDecimal(int parameterIndex, BigDecimal value) throws SQLException {
    wrappedPreparedStatement.setBigDecimal(parameterIndex, value);
  }

  /** {@inheritDoc} */
  @Override
  public void setString(int parameterIndex, String value) throws SQLException {
    wrappedPreparedStatement.setString(parameterIndex, value);
  }

  /** {@inheritDoc} */
  @Override
  public void setBytes(int parameterIndex, byte[] value) throws SQLException {
    wrappedPreparedStatement.setBytes(parameterIndex, value);
  }

  /** {@inheritDoc} */
  @Override
  public void setDate(int parameterIndex, Date value) throws SQLException {
    wrappedPreparedStatement.setDate(parameterIndex, value);
  }

  /** {@inheritDoc} */
  @Override
  public void setTime(int parameterIndex, Time value) throws SQLException {
    wrappedPreparedStatement.setTime(parameterIndex, value);
  }

  /** {@inheritDoc} */
  @Override
  public void setTimestamp(int parameterIndex, Timestamp value) throws SQLException {
    wrappedPreparedStatement.setTimestamp(parameterIndex, value);
  }

  /** {@inheritDoc} */
  @Override
  public void setAsciiStream(int parameterIndex, InputStream inputStream, int length)
      throws SQLException {
    wrappedPreparedStatement.setAsciiStream(parameterIndex, inputStream);
  }

  /** {@inheritDoc} */
  @Override
  @Deprecated
  public void setUnicodeStream(int parameterIndex, InputStream inputStream, int length)
      throws SQLException {
    wrappedPreparedStatement.setUnicodeStream(parameterIndex, inputStream, length);
  }

  /** {@inheritDoc} */
  @Override
  public void setBinaryStream(int parameterIndex, InputStream inputStream, int length)
      throws SQLException {
    wrappedPreparedStatement.setBinaryStream(parameterIndex, inputStream);
  }

  /** {@inheritDoc} */
  @Override
  public void clearParameters() throws SQLException {
    wrappedPreparedStatement.clearParameters();
  }

  /** {@inheritDoc} */
  @Override
  public void setObject(int parameterIndex, Object value, int targetSqlType) throws SQLException {
    wrappedPreparedStatement.setObject(parameterIndex, value, targetSqlType);
  }

  /** {@inheritDoc} */
  @Override
  public void setObject(int parameterIndex, Object value) throws SQLException {
    wrappedPreparedStatement.setObject(parameterIndex, value);
  }

  /** {@inheritDoc} */
  @Override
  public void addBatch() throws SQLException {
    wrappedPreparedStatement.addBatch();
  }

  /** {@inheritDoc} */
  @Override
  public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
    wrappedPreparedStatement.setCharacterStream(parameterIndex, reader, length);
  }

  /** {@inheritDoc} */
  @Override
  public void setRef(int parameterIndex, Ref value) throws SQLException {
    wrappedPreparedStatement.setRef(parameterIndex, value);
  }

  /** {@inheritDoc} */
  @Override
  public void setBlob(int parameterIndex, Blob value) throws SQLException {
    wrappedPreparedStatement.setBlob(parameterIndex, value);
  }

  /** {@inheritDoc} */
  @Override
  public void setClob(int parameterIndex, Clob value) throws SQLException {
    wrappedPreparedStatement.setClob(parameterIndex, value);
  }

  /** {@inheritDoc} */
  @Override
  public void setArray(int parameterIndex, Array value) throws SQLException {
    wrappedPreparedStatement.setArray(parameterIndex, value);
  }

  /** {@inheritDoc} */
  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    return wrappedPreparedStatement.getMetaData();
  }

  /** {@inheritDoc} */
  @Override
  public void setDate(int parameterIndex, Date value, Calendar calendar) throws SQLException {
    wrappedPreparedStatement.setDate(parameterIndex, value);
  }

  /** {@inheritDoc} */
  @Override
  public void setTime(int parameterIndex, Time value, Calendar calendar) throws SQLException {
    wrappedPreparedStatement.setTime(parameterIndex, value, calendar);
  }

  /** {@inheritDoc} */
  @Override
  public void setTimestamp(int parameterIndex, Timestamp value, Calendar calendar)
      throws SQLException {
    wrappedPreparedStatement.setTimestamp(parameterIndex, value, calendar);
  }

  /** {@inheritDoc} */
  @Override
  public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
    wrappedPreparedStatement.setNull(parameterIndex, sqlType, typeName);
  }

  /** {@inheritDoc} */
  @Override
  public void setURL(int parameterIndex, URL value) throws SQLException {
    wrappedPreparedStatement.setURL(parameterIndex, value);
  }

  /** {@inheritDoc} */
  @Override
  public ParameterMetaData getParameterMetaData() throws SQLException {
    return wrappedPreparedStatement.getParameterMetaData();
  }

  /** {@inheritDoc} */
  @Override
  public void setRowId(int parameterIndex, RowId value) throws SQLException {
    wrappedPreparedStatement.setRowId(parameterIndex, value);
  }

  /** {@inheritDoc} */
  @Override
  public void setNString(int parameterIndex, String value) throws SQLException {
    wrappedPreparedStatement.setNString(parameterIndex, value);
  }

  /** {@inheritDoc} */
  @Override
  public void setNCharacterStream(int parameterIndex, Reader reader, long length)
      throws SQLException {
    wrappedPreparedStatement.setNCharacterStream(parameterIndex, reader, length);
  }

  /** {@inheritDoc} */
  @Override
  public void setNClob(int parameterIndex, NClob value) throws SQLException {
    wrappedPreparedStatement.setNClob(parameterIndex, value);
  }

  /** {@inheritDoc} */
  @Override
  public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
    wrappedPreparedStatement.setClob(parameterIndex, reader, length);
  }

  /** {@inheritDoc} */
  @Override
  public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
    wrappedPreparedStatement.setBlob(parameterIndex, inputStream, length);
  }

  /** {@inheritDoc} */
  @Override
  public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
    wrappedPreparedStatement.setNClob(parameterIndex, reader, length);
  }

  /** {@inheritDoc} */
  @Override
  public void setSQLXML(int parameterIndex, SQLXML value) throws SQLException {
    wrappedPreparedStatement.setSQLXML(parameterIndex, value);
  }

  /** {@inheritDoc} */
  @Override
  public void setObject(
      int parameterIndex, Object value, int targetSqlType, int scaleOrLength)
      throws SQLException {
    wrappedPreparedStatement.setObject(parameterIndex, value, targetSqlType, scaleOrLength);
  }

  /** {@inheritDoc} */
  @Override
  public void setAsciiStream(int parameterIndex, InputStream inputStream, long length)
      throws SQLException {
    wrappedPreparedStatement.setAsciiStream(parameterIndex, inputStream, length);
  }

  /** {@inheritDoc} */
  @Override
  public void setBinaryStream(int parameterIndex, InputStream inputStream, long length)
      throws SQLException {
    wrappedPreparedStatement.setBinaryStream(parameterIndex, inputStream, length);
  }

  /** {@inheritDoc} */
  @Override
  public void setCharacterStream(int parameterIndex, Reader reader, long length)
      throws SQLException {
    wrappedPreparedStatement.setCharacterStream(parameterIndex, reader, length);
  }

  /** {@inheritDoc} */
  @Override
  public void setAsciiStream(int parameterIndex, InputStream inputStream) throws SQLException {
    wrappedPreparedStatement.setAsciiStream(parameterIndex, inputStream);
  }

  /** {@inheritDoc} */
  @Override
  public void setBinaryStream(int parameterIndex, InputStream inputStream) throws SQLException {
    wrappedPreparedStatement.setBinaryStream(parameterIndex, inputStream);
  }

  /** {@inheritDoc} */
  @Override
  public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
    wrappedPreparedStatement.setCharacterStream(parameterIndex, reader);
  }

  /** {@inheritDoc} */
  @Override
  public void setNCharacterStream(int parameterIndex, Reader reader) throws SQLException {
    wrappedPreparedStatement.setNCharacterStream(parameterIndex, reader);
  }

  /** {@inheritDoc} */
  @Override
  public void setClob(int parameterIndex, Reader reader) throws SQLException {
    wrappedPreparedStatement.setClob(parameterIndex, reader);
  }

  /** {@inheritDoc} */
  @Override
  public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
    wrappedPreparedStatement.setBlob(parameterIndex, inputStream);
  }

  /** {@inheritDoc} */
  @Override
  public void setNClob(int parameterIndex, Reader reader) throws SQLException {
    wrappedPreparedStatement.setNClob(parameterIndex, reader);
  }
}
