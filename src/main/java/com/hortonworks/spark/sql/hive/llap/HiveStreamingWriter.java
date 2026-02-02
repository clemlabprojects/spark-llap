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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Streaming writer that adapts Spark streaming writes into HWC streaming sinks.
 */
public class HiveStreamingWriter implements Write {
  private static final Logger LOG = LoggerFactory.getLogger(HiveStreamingWriter.class);
  private static final long DEFAULT_COMMIT_INTERVAL_ROWS = 10000L;

  private final String queryId;
  private final StructType schema;
  private final Map<String, String> options;

  /**
   * Creates a streaming writer.
   *
   * @param queryId Spark streaming query id
   * @param schema DataFrame schema
   * @param options writer options
   */
  public HiveStreamingWriter(String queryId, StructType schema, Map<String, String> options) {
    this.queryId = queryId;
    this.schema = schema;
    this.options = options;
  }

  /**
   * Builds the {@link BatchWrite} implementation used by Spark for micro-batches.
   *
   * @return batch write implementation
   */
  @Override
  public BatchWrite toBatch() {
    String databaseName = options.containsKey("default.db")
        ? options.get("default.db")
        : options.getOrDefault("database", "default");
    String tableName = options.getOrDefault("table", null);
    String partitionSpec = options.getOrDefault("partition", null);
    List<String> partitionList = partitionSpec == null ? null : Arrays.asList(partitionSpec.split(","));
    String metastoreUri = options.getOrDefault("metastoreUri", "thrift://localhost:9083");
    String commitIntervalValue = options.getOrDefault(
        "commitIntervalRows",
        String.valueOf(DEFAULT_COMMIT_INTERVAL_ROWS));
    long commitIntervalRows = Long.parseLong(commitIntervalValue);
    String metastoreKerberosPrincipal = options.getOrDefault("metastoreKrbPrincipal", null);

    LOG.info(
        "OPTIONS - database: {} table: {} partition: {} commitIntervalRows: {} metastoreUri: {} metastoreKrbPrincipal: {}",
        databaseName,
        tableName,
        partitionSpec,
        commitIntervalRows,
        metastoreUri,
        metastoreKerberosPrincipal);

    return new HiveStreamingDataSourceWriter(
        this.queryId,
        this.schema,
        commitIntervalRows,
        databaseName,
        tableName,
        partitionList,
        metastoreUri,
        metastoreKerberosPrincipal);
  }
}
