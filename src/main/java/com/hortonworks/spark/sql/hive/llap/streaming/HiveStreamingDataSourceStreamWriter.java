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

import com.hortonworks.spark.sql.hive.llap.HiveStreamingDataWriterFactory;
import java.util.List;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Streaming write implementation for the HWC streaming data source.
 */
public class HiveStreamingDataSourceStreamWriter implements StreamingWrite {
  private static final Logger LOG =
    LoggerFactory.getLogger(HiveStreamingDataSourceStreamWriter.class);

  private final String jobId;
  private final StructType schema;
  private final String databaseName;
  private final String tableName;
  private final List<String> partitionColumns;
  private final String metastoreUri;
  private final String metastoreKerberosPrincipal;

  /**
   * Create a streaming writer that delegates to HWC streaming data writers.
   *
   * @param jobId Spark job identifier
   * @param schema input schema
   * @param databaseName Hive database name
   * @param tableName Hive table name
   * @param partitionColumns partition column names
   * @param metastoreUri Hive metastore URI
   * @param metastoreKerberosPrincipal Kerberos principal for HMS
   */
  public HiveStreamingDataSourceStreamWriter(String jobId,
                                             StructType schema,
                                             String databaseName,
                                             String tableName,
                                             List<String> partitionColumns,
                                             String metastoreUri,
                                             String metastoreKerberosPrincipal) {
    this.jobId = jobId;
    this.schema = schema;
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.partitionColumns = partitionColumns;
    this.metastoreUri = metastoreUri;
    this.metastoreKerberosPrincipal = metastoreKerberosPrincipal;
  }

  @Override
  /**
   * {@inheritDoc}
   */
  public StreamingDataWriterFactory createStreamingWriterFactory(PhysicalWriteInfo writeInfo) {
    // Epoch id is managed by the streaming framework.
    return new HiveStreamingDataWriterFactory(
      jobId,
      schema,
      -1L,
      databaseName,
      tableName,
      partitionColumns,
      metastoreUri,
      metastoreKerberosPrincipal);
  }

  @Override
  /**
   * {@inheritDoc}
   */
  public void commit(long epochId, WriterCommitMessage[] messages) {
    LOG.info("Commit job {}", jobId);
  }

  @Override
  /**
   * {@inheritDoc}
   */
  public void abort(long epochId, WriterCommitMessage[] messages) {
    LOG.info("Abort job {}", jobId);
  }
}
