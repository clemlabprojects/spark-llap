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

import java.util.List;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory;
import org.apache.spark.sql.types.StructType;

/**
 * Factory for HWC streaming data writers.
 */
public class HiveStreamingDataWriterFactory implements DataWriterFactory,
  StreamingDataWriterFactory {

  private final String jobId;
  private final StructType schema;
  private final long commitIntervalRows;
  private final String databaseName;
  private final String tableName;
  private final List<String> partitionColumns;
  private final String metastoreUri;
  private final String metastoreKerberosPrincipal;

  /**
   * Create a streaming data writer factory.
   *
   * @param jobId Spark job id
   * @param schema input schema
   * @param commitIntervalRows commit interval in rows
   * @param databaseName Hive database name
   * @param tableName Hive table name
   * @param partitionColumns partition columns
   * @param metastoreUri Hive metastore URI
   * @param metastoreKerberosPrincipal HMS Kerberos principal
   */
  public HiveStreamingDataWriterFactory(String jobId,
                                        StructType schema,
                                        long commitIntervalRows,
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
    this.commitIntervalRows = commitIntervalRows;
    this.metastoreUri = metastoreUri;
    this.metastoreKerberosPrincipal = metastoreKerberosPrincipal;
  }

  @Override
  /**
   * {@inheritDoc}
   */
  public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
    return new HiveStreamingDataWriter(
      jobId,
      schema,
      commitIntervalRows,
      partitionId,
      taskId,
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
  public DataWriter<InternalRow> createWriter(int partitionId, long taskId, long epochId) {
    return new HiveStreamingDataWriter(
      jobId,
      schema,
      commitIntervalRows,
      partitionId,
      taskId,
      epochId,
      databaseName,
      tableName,
      partitionColumns,
      metastoreUri,
      metastoreKerberosPrincipal);
  }
}
