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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point for converting a logical Spark write into a streaming HWC write.
 */
public class HiveStreamingStreamWriter implements Write {
  private static final Logger LOG = LoggerFactory.getLogger(HiveStreamingStreamWriter.class);

  private final String queryId;
  private final StructType schema;
  private final Map<String, String> optionMap;

  /**
   * Create a streaming write adapter.
   *
   * @param queryId Spark query id
   * @param schema input schema
   * @param optionMap data source options
   */
  public HiveStreamingStreamWriter(String queryId,
                                   StructType schema,
                                   Map<String, String> optionMap) {
    this.queryId = queryId;
    this.schema = schema;
    this.optionMap = optionMap;
  }

  @Override
  /**
   * {@inheritDoc}
   */
  public StreamingWrite toStreaming() {
    String databaseName = optionMap.containsKey("default.db")
      ? optionMap.get("default.db")
      : optionMap.getOrDefault("database", "default");
    String tableName = optionMap.getOrDefault("table", null);
    String partitionSpec = optionMap.getOrDefault("partition", null);
    List<String> partitionColumns =
      partitionSpec == null ? null : Arrays.asList(partitionSpec.split(","));
    String metastoreUri = optionMap.getOrDefault("metastoreUri", "thrift://localhost:9083");
    String metastoreKerberosPrincipal =
      optionMap.getOrDefault("metastoreKrbPrincipal", null);

    LOG.info(
      "OPTIONS - database: {} table: {} partition: {} metastoreUri: {} metastoreKerberosPrincipal: {}",
      databaseName,
      tableName,
      partitionSpec,
      metastoreUri,
      metastoreKerberosPrincipal);

    return new HiveStreamingDataSourceStreamWriter(
      queryId,
      schema,
      databaseName,
      tableName,
      partitionColumns,
      metastoreUri,
      metastoreKerberosPrincipal);
  }
}
