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

import com.hortonworks.spark.sql.hive.llap.writers.streaming.HiveStreamingDataWriterFactory;
import java.util.List;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Spark DataSource V2 writer for HWC streaming writes.
 */
public class HiveStreamingDataSourceWriter implements BatchWrite {
  private static final Logger LOG = LoggerFactory.getLogger(HiveStreamingDataSourceWriter.class);

  private final String jobId;
  private final StructType tableSchema;
  private final String databaseName;
  private final String tableName;
  private final List<String> partitionValues;
  private final long commitIntervalRows;
  private final String metastoreUri;
  private final String metastoreKerberosPrincipal;

  /**
   * Creates a streaming writer.
   *
   * @param jobId Spark job id
   * @param schema table schema
   * @param commitIntervalRows commit interval in rows
   * @param databaseName database name
   * @param tableName table name
   * @param partitionValues partition values
   * @param metastoreUri metastore URI
   * @param metastoreKerberosPrincipal metastore Kerberos principal
   */
  public HiveStreamingDataSourceWriter(
      String jobId,
      StructType schema,
      long commitIntervalRows,
      String databaseName,
      String tableName,
      List<String> partitionValues,
      String metastoreUri,
      String metastoreKerberosPrincipal) {
    this.jobId = jobId;
    this.tableSchema = schema;
    this.commitIntervalRows = commitIntervalRows;
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.partitionValues = partitionValues;
    this.metastoreUri = metastoreUri;
    this.metastoreKerberosPrincipal = metastoreKerberosPrincipal;
  }

  /**
   * Always uses Spark's commit coordinator.
   *
   * @return true
   */
  @Override
  public boolean useCommitCoordinator() {
    return true;
  }

  /**
   * No-op commit callback.
   *
   * @param writerCommitMessage commit message
   */
  @Override
  public void onDataWriterCommit(WriterCommitMessage writerCommitMessage) {
    // no-op
  }

  /**
   * Creates executor writers for streaming output.
   *
   * @param physicalWriteInfo Spark write info
   * @return writer factory
   */
  @Override
  public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo physicalWriteInfo) {
    return new HiveStreamingDataWriterFactory(
        jobId,
        tableSchema,
        commitIntervalRows,
        databaseName,
        tableName,
        partitionValues,
        metastoreUri,
        metastoreKerberosPrincipal);
  }

  /**
   * Logs a successful commit.
   *
   * @param writerCommitMessages commit messages
   */
  @Override
  public void commit(WriterCommitMessage[] writerCommitMessages) {
    LOG.info("Commit job {}", jobId);
  }

  /**
   * Logs an aborted job.
   *
   * @param writerCommitMessages commit messages
   */
  @Override
  public void abort(WriterCommitMessage[] writerCommitMessages) {
    LOG.info("Abort job {}", jobId);
  }
}
