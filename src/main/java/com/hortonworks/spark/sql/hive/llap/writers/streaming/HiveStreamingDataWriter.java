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

import com.hortonworks.spark.sql.hive.llap.writers.SimpleWriterCommitMessage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hive.streaming.HiveStreamingConnection;
import org.apache.hive.streaming.StreamingConnection;
import org.apache.hive.streaming.StreamingException;
import org.apache.hive.streaming.StrictDelimitedInputWriter;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Streaming data writer that writes to Hive ACID tables using HWC streaming APIs.
 */
public class HiveStreamingDataWriter implements DataWriter<InternalRow> {
  private static final Logger LOG = LoggerFactory.getLogger(HiveStreamingDataWriter.class);

  private final String jobId;
  private final StructType schema;
  private final int partitionId;
  private final long taskId;
  private final long epochId;
  private final boolean hasEpochId;
  private final String databaseName;
  private final String tableName;
  private final List<String> partitionValues;
  private final String metastoreUri;
  private StreamingConnection streamingConnection;
  private final long commitAfterRowCount;
  private long rowsWritten = 0L;
  private final String metastoreKerberosPrincipal;
  private StreamingRecordFormatter formatter;

  /**
   * Create a streaming data writer with an epoch id.
   *
   * @param jobId Spark job id
   * @param schema input schema
   * @param commitAfterRowCount commit interval in rows
   * @param partitionId Spark partition id
   * @param taskId Spark task id
   * @param epochId Spark epoch id
   * @param databaseName Hive database name
   * @param tableName Hive table name
   * @param partitionValues static partition values
   * @param metastoreUri Hive metastore URI
   * @param metastoreKerberosPrincipal HMS Kerberos principal
   */
  public HiveStreamingDataWriter(String jobId,
                                 StructType schema,
                                 long commitAfterRowCount,
                                 int partitionId,
                                 long taskId,
                                 long epochId,
                                 String databaseName,
                                 String tableName,
                                 List<String> partitionValues,
                                 String metastoreUri,
                                 String metastoreKerberosPrincipal) {
    this.jobId = jobId;
    this.schema = schema;
    this.partitionId = partitionId;
    this.taskId = taskId;
    this.epochId = epochId;
    this.hasEpochId = true;
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.partitionValues = partitionValues;
    this.metastoreUri = metastoreUri;
    this.commitAfterRowCount = commitAfterRowCount;
    this.metastoreKerberosPrincipal = metastoreKerberosPrincipal;
    initializeStreamingConnection();
  }

  /**
   * Create a streaming data writer without an epoch id.
   *
   * @param jobId Spark job id
   * @param schema input schema
   * @param commitAfterRowCount commit interval in rows
   * @param partitionId Spark partition id
   * @param taskId Spark task id
   * @param databaseName Hive database name
   * @param tableName Hive table name
   * @param partitionValues static partition values
   * @param metastoreUri Hive metastore URI
   * @param metastoreKerberosPrincipal HMS Kerberos principal
   */
  public HiveStreamingDataWriter(String jobId,
                                 StructType schema,
                                 long commitAfterRowCount,
                                 int partitionId,
                                 long taskId,
                                 String databaseName,
                                 String tableName,
                                 List<String> partitionValues,
                                 String metastoreUri,
                                 String metastoreKerberosPrincipal) {
    this.jobId = jobId;
    this.schema = schema;
    this.partitionId = partitionId;
    this.taskId = taskId;
    this.epochId = 0L;
    this.hasEpochId = false;
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.partitionValues = partitionValues;
    this.metastoreUri = metastoreUri;
    this.commitAfterRowCount = commitAfterRowCount;
    this.metastoreKerberosPrincipal = metastoreKerberosPrincipal;
    initializeStreamingConnection();
  }

  private void initializeStreamingConnection() {
    try {
      StrictDelimitedInputWriter recordWriter = StrictDelimitedInputWriter.newBuilder()
        .withFieldDelimiter('\u0001')
        .withCollectionDelimiter('\u0002')
        .withMapKeyDelimiter('\u0003')
        .build();
      HiveConf hiveConf = new HiveConf();
      hiveConf.set(MetastoreConf.ConfVars.THRIFT_URIS.getHiveName(), metastoreUri);
      hiveConf.setVar(HiveConf.ConfVars.HIVE_CLASSLOADER_SHADE_PREFIX, "shadehive");
      hiveConf.set(MetastoreConf.ConfVars.CATALOG_DEFAULT.getHiveName(), "hive");
      if (metastoreKerberosPrincipal != null) {
        hiveConf.set(MetastoreConf.ConfVars.KERBEROS_PRINCIPAL.getHiveName(), metastoreKerberosPrincipal);
      }
      LOG.info("Creating hive streaming connection..");
      streamingConnection = HiveStreamingConnection.newBuilder()
        .withDatabase(databaseName)
        .withTable(tableName)
        .withStaticPartitionValues(partitionValues)
        .withRecordWriter(recordWriter)
        .withHiveConf(hiveConf)
        .withAgentInfo(jobId + "(" + partitionId + ")")
        .connect();
      Table table = streamingConnection.getTable();
      formatter = new StreamingRecordFormatter.Builder(schema)
        .withFieldDelimiter((byte) 1)
        .withCollectionDelimiter((byte) 2)
        .withMapKeyDelimiter((byte) 3)
        .withNullRepresentationString("\\N")
        .withTableProperties(table.getMetadata())
        .build();
      streamingConnection.beginTransaction();
      LOG.info("{} created hive streaming connection.", streamingConnection.getAgentInfo());
    } catch (StreamingException streamingException) {
      throw new RuntimeException("Unable to create hive streaming connection", streamingException);
    }
  }

  @Override
  /**
   * {@inheritDoc}
   */
  public void write(InternalRow internalRow) throws IOException {
    try {
      streamingConnection.write(formatRecordForHive(internalRow).toByteArray());
      ++rowsWritten;
      if (rowsWritten > 0L && commitAfterRowCount > 0L
          && rowsWritten % commitAfterRowCount == 0L) {
        LOG.info("Committing transaction after rows: {}", rowsWritten);
        streamingConnection.commitTransaction();
        streamingConnection.beginTransaction();
      }
    } catch (StreamingException streamingException) {
      throw new IOException(streamingException);
    }
  }

  private ByteArrayOutputStream formatRecordForHive(InternalRow internalRow) throws IOException {
    return formatter.format(internalRow);
  }

  @Override
  /**
   * {@inheritDoc}
   */
  public WriterCommitMessage commit() throws IOException {
    try {
      streamingConnection.commitTransaction();
    } catch (StreamingException streamingException) {
      throw new IOException(streamingException);
    }
    String commitMessage = buildCommitMessage();
    streamingConnection.close();
    LOG.info("Closing streaming connection on commit. Msg: {} rowsWritten: {}",
      commitMessage, rowsWritten);
    return new SimpleWriterCommitMessage(commitMessage, null);
  }

  @Override
  /**
   * {@inheritDoc}
   */
  public void abort() throws IOException {
    if (streamingConnection != null) {
      try {
        streamingConnection.abortTransaction();
      } catch (StreamingException streamingException) {
        throw new IOException(streamingException);
      }
      String abortMessage = buildCommitMessage();
      streamingConnection.close();
      LOG.info("Closing streaming connection on abort. Msg: {} rowsWritten: {}",
        abortMessage, rowsWritten);
    }
  }

  @Override
  /**
   * {@inheritDoc}
   */
  public void close() throws IOException {
  }

  private String buildCommitMessage() {
    if (hasEpochId) {
      return "Committed jobId: " + jobId
        + " partitionId: " + partitionId
        + " taskId: " + taskId
        + " epochId: " + epochId
        + " connectionStats: " + streamingConnection.getConnectionStats();
    }
    return "Committed jobId: " + jobId
      + " partitionId: " + partitionId
      + " taskId: " + taskId
      + " connectionStats: " + streamingConnection.getConnectionStats();
  }
}
