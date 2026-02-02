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

import com.hortonworks.spark.sql.hive.llap.HWConf;
import com.hortonworks.spark.sql.hive.llap.writers.HiveWarehouseDataWriterHelper;
import java.util.Map;

/**
 * Parsed HWC option values sourced from a user-provided options map.
 */
public class HWCOptions {
  public HiveWarehouseDataWriterHelper.FileFormat fileFormat;
  public String fieldDelimiter;
  public String lineDelimiter;
  public String transactional;
  public String database;
  public String table;
  public String transactionalProperties;
  private final Map<String, String> optionMap;

  public static final String TRANSACTIONAL = "transactional";
  public static final String TRANSACTIONAL_PROPERTIES = "transactional_properties";
  public static final String DEFAULT = "default";
  public static final String INSERT_ONLY = "insert_only";
  public static final String NONE = "val_not_provided";
  public static final String FILE_FORMAT = "fileformat";
  public static final String TEXT_FIELD_SEPARATOR = "sep";
  public static final String TEXT_NEWLINE_CHARACTER = "newline";
  public static final String TABLE = "table";
  public static final String DATABASE = "database";
  public static final String SER_DE_LIBRARY = "SerDe Library:";
  public static final String ORC_SERDE = "OrcSerde";
  public static final String PARQUET_HIVE_SERDE = "ParquetHiveSerDe";
  public static final String AVRO_SERDE = "AvroSerDe";
  public static final String LAZY_SIMPLE_SERDE = "LazySimpleSerDe";
  public static final String MULTI_DELIMIT_SERDE = "MultiDelimitSerDe";

  /**
   * Builds an options wrapper around the provided option map.
   *
   * @param optionMap raw options from the DataFrame writer
   */
  public HWCOptions(Map<String, String> optionMap) {
    this.optionMap = optionMap;
    parseOptions();
  }

  private void parseOptions() {
    // Parse the file format first so downstream options can rely on it.
    String fileFormatValue = optionMap.get(FILE_FORMAT);
    if (fileFormatValue != null) {
      fileFormat = HiveWarehouseDataWriterHelper.FileFormat.getFormat(fileFormatValue);
    }
    fieldDelimiter = optionMap.get(TEXT_FIELD_SEPARATOR);
    lineDelimiter = optionMap.get(TEXT_NEWLINE_CHARACTER);
    transactional = optionMap.getOrDefault(TRANSACTIONAL, "true");
    transactionalProperties = optionMap.getOrDefault(TRANSACTIONAL_PROPERTIES, NONE);
    database = optionMap.getOrDefault(DATABASE, HWConf.DEFAULT_DB.getFromOptionsMap(optionMap));
    table = optionMap.get(TABLE);
  }
}
