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

package com.hortonworks.spark.sql.hive.llap.writers.streaming;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hive.serde2.lazy.LazyUtils;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.CalendarIntervalType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.NullType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Serializable;

/**
 * Formats Spark {@link InternalRow} records into Hive streaming delimited text.
 */
public class StreamingRecordFormatter implements Serializable {
  public static final byte DEFAULT_FIELD_DELIMITER = 1;
  public static final byte DEFAULT_COLLECTION_DELIMITER = 2;
  public static final byte DEFAULT_MAP_KEY_DELIMITER = 3;
  public static final String DEFAULT_NULL_REPRESENTATION_STRING = "\\N";
  public static final String HIVE_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
  public static final LocalDate UNIX_EPOCH_DATE = LocalDate.parse("1970-01-01");

  private static final int DEFAULT_BUFFER_SIZE = 1024;
  private static SimpleDateFormat timestampFormatter;

  private final byte[] separators;
  private final byte fieldDelimiter;
  private final byte collectionDelimiter;
  private final byte mapKeyDelimiter;
  private final StructType schema;
  private final String nullRepresentationString;
  private final boolean escaped;
  private final byte escapeChar;
  private final boolean[] needsEscape;
  private final Properties tableProperties;

  private StreamingRecordFormatter(Builder builder) {
    this.fieldDelimiter = builder.fieldDelimiter;
    this.collectionDelimiter = builder.collectionDelimiter;
    this.mapKeyDelimiter = builder.mapKeyDelimiter;
    this.nullRepresentationString = builder.nullRepresentationString;
    this.tableProperties = builder.tableProperties;
    this.schema = builder.schema;
    this.needsEscape = new boolean[256];

    // Build the separator list expected by Hive's LazySimpleSerDe.
    this.separators = buildSeparators(fieldDelimiter, collectionDelimiter, mapKeyDelimiter);

    String escapeDelimiterProperty = tableProperties.getProperty("escape.delim");
    this.escaped = escapeDelimiterProperty != null;
    if (escaped) {
      this.escapeChar = LazyUtils.getByte(escapeDelimiterProperty, (byte) '\\');
      this.needsEscape[escapeChar & 0xFF] = true;
      for (byte separator : separators) {
        this.needsEscape[separator & 0xFF] = true;
      }

      boolean escapeCrlf =
          Boolean.parseBoolean(tableProperties.getProperty("serialization.escape.crlf"));
      if (escapeCrlf) {
        this.needsEscape[13] = true;
        this.needsEscape[10] = true;
      }
    } else {
      this.escapeChar = 0;
    }
  }

  private static byte[] buildSeparators(
      byte fieldDelimiter,
      byte collectionDelimiter,
      byte mapKeyDelimiter) {
    List<Byte> separatorsList = new ArrayList<>();
    separatorsList.add(fieldDelimiter);
    separatorsList.add(collectionDelimiter);
    separatorsList.add(mapKeyDelimiter);
    for (byte separator = 4; separator <= 8; separator++) {
      separatorsList.add(separator);
    }
    separatorsList.add((byte) 11);
    for (byte separator = 14; separator <= 26; separator++) {
      separatorsList.add(separator);
    }
    for (byte separator = 28; separator <= 31; separator++) {
      separatorsList.add(separator);
    }
    for (int separator = Byte.MIN_VALUE; separator < 0; separator++) {
      separatorsList.add((byte) separator);
    }
    byte[] separators = new byte[separatorsList.size()];
    for (int index = 0; index < separatorsList.size(); index++) {
      separators[index] = separatorsList.get(index);
    }
    return separators;
  }

  /**
   * Formats a row into a delimited byte stream.
   *
   * @param internalRow row to format
   * @return byte output stream containing the formatted row
   * @throws IOException if escaping fails
   */
  public ByteArrayOutputStream format(InternalRow internalRow) throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream(DEFAULT_BUFFER_SIZE);
    for (int fieldIndex = 0; fieldIndex < internalRow.numFields(); fieldIndex++) {
      if (fieldIndex > 0) {
        outputStream.write(fieldDelimiter);
      }
      DataType fieldType = schema.fields()[fieldIndex].dataType();
      formatValue(internalRow.get(fieldIndex, fieldType), fieldType, 1, outputStream);
    }
    return outputStream;
  }

  private void formatValue(
      Object value,
      DataType dataType,
      int separatorDepth,
      ByteArrayOutputStream outputStream) throws IOException {
    ensureSupportedDataType(dataType);
    if (value == null) {
      outputStream.write(nullRepresentationString.getBytes());
      return;
    }

    byte separator = separators[separatorDepth];
    if (dataType instanceof StructType) {
      StructField[] structFields = ((StructType) dataType).fields();
      InternalRow nestedRow = (InternalRow) value;
      for (int fieldIndex = 0; fieldIndex < structFields.length; fieldIndex++) {
        if (fieldIndex > 0) {
          outputStream.write(separator);
        }
        DataType nestedFieldType = structFields[fieldIndex].dataType();
        Object nestedValue = nestedRow.get(fieldIndex, nestedFieldType);
        formatValue(nestedValue, nestedFieldType, separatorDepth + 1, outputStream);
      }
    } else if (dataType instanceof ArrayType) {
      DataType elementType = ((ArrayType) dataType).elementType();
      ArrayData arrayData = (ArrayData) value;
      for (int elementIndex = 0; elementIndex < arrayData.numElements(); elementIndex++) {
        if (elementIndex > 0) {
          outputStream.write(separator);
        }
        Object elementValue = arrayData.get(elementIndex, elementType);
        formatValue(elementValue, elementType, separatorDepth + 1, outputStream);
      }
    } else {
      writePrimitive(value, dataType, outputStream);
    }
  }

  private void ensureSupportedDataType(DataType dataType) {
    if (dataType instanceof MapType
        || dataType instanceof CalendarIntervalType
        || dataType instanceof NullType) {
      throw new IllegalArgumentException("MapType/CalendarIntervalType/NullType not yet supported");
    }
  }

  private void writePrimitive(Object value, DataType dataType, ByteArrayOutputStream outputStream)
      throws IOException {
    if (DataTypes.StringType.equals(dataType)) {
      outputStream.write(formatStringType(value).getBytes());
    } else if (DataTypes.TimestampType.equals(dataType)) {
      String timestampValue = formatTimeStampType(value);
      if (timestampValue != null) {
        outputStream.write(timestampValue.getBytes());
      } else {
        outputStream.write(nullRepresentationString.getBytes());
      }
    } else if (DataTypes.DateType.equals(dataType)) {
      if (value instanceof Integer) {
        LocalDate dateValue = UNIX_EPOCH_DATE.plusDays((Integer) value);
        outputStream.write(dateValue.toString().getBytes());
      } else {
        outputStream.write(nullRepresentationString.getBytes());
      }
    } else if (DataTypes.BinaryType.equals(dataType)) {
      outputStream.write(Base64.getEncoder().encode((byte[]) value));
    } else {
      outputStream.write(value.toString().getBytes());
    }
  }

  private String formatTimeStampType(Object value) {
    if (value instanceof Long) {
      long epochMillis = TimeUnit.MILLISECONDS.convert((Long) value, TimeUnit.MICROSECONDS);
      Date timestampDate = new Date(epochMillis);
      if (timestampFormatter == null) {
        timestampFormatter = new SimpleDateFormat(HIVE_DATE_FORMAT);
      }
      return timestampFormatter.format(timestampDate);
    }
    return null;
  }

  private String formatStringType(Object value) throws IOException {
    ByteArrayOutputStream escapedOutput = new ByteArrayOutputStream();
    byte[] inputBytes = value.toString().getBytes();
    LazyUtils.writeEscaped(
        escapedOutput, inputBytes, 0, inputBytes.length, escaped, escapeChar, needsEscape);
    String formattedValue = new String(escapedOutput.toByteArray());
    String quoteDelimiter = tableProperties.getProperty("quote.delim", null);
    if (quoteDelimiter != null) {
      formattedValue = quoteDelimiter + formattedValue + quoteDelimiter;
    }
    return formattedValue;
  }

  /** Builder for {@link StreamingRecordFormatter}. */
  public static class Builder {
    private byte fieldDelimiter = DEFAULT_FIELD_DELIMITER;
    private byte collectionDelimiter = DEFAULT_COLLECTION_DELIMITER;
    private byte mapKeyDelimiter = DEFAULT_MAP_KEY_DELIMITER;
    private String nullRepresentationString = DEFAULT_NULL_REPRESENTATION_STRING;
    private final StructType schema;
    private Properties tableProperties;

    /**
     * Creates a builder for the given schema.
     *
     * @param schema table schema
     */
    public Builder(StructType schema) {
      this.schema = schema;
    }

    /**
     * Sets the field delimiter.
     *
     * @param fieldDelimiter field delimiter byte
     * @return builder instance
     */
    public Builder withFieldDelimiter(byte fieldDelimiter) {
      this.fieldDelimiter = fieldDelimiter;
      return this;
    }

    /**
     * Sets the collection delimiter.
     *
     * @param collectionDelimiter collection delimiter byte
     * @return builder instance
     */
    public Builder withCollectionDelimiter(byte collectionDelimiter) {
      this.collectionDelimiter = collectionDelimiter;
      return this;
    }

    /**
     * Sets the map key delimiter.
     *
     * @param mapKeyDelimiter map key delimiter byte
     * @return builder instance
     */
    public Builder withMapKeyDelimiter(byte mapKeyDelimiter) {
      this.mapKeyDelimiter = mapKeyDelimiter;
      return this;
    }

    /**
     * Sets the null representation string.
     *
     * @param nullRepresentationString null representation
     * @return builder instance
     */
    public Builder withNullRepresentationString(String nullRepresentationString) {
      this.nullRepresentationString = nullRepresentationString;
      return this;
    }

    /**
     * Sets Hive table properties required for escaping and quoting.
     *
     * @param tableProperties Hive table properties
     * @return builder instance
     */
    public Builder withTableProperties(Properties tableProperties) {
      this.tableProperties = tableProperties;
      return this;
    }

    /**
     * Builds a {@link StreamingRecordFormatter} instance.
     *
     * @return formatter
     */
    public StreamingRecordFormatter build() {
      return new StreamingRecordFormatter(this);
    }
  }
}
