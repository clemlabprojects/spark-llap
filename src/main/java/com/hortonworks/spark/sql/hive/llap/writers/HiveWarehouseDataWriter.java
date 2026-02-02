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

package com.hortonworks.spark.sql.hive.llap.writers;

import com.hortonworks.spark.sql.hive.llap.util.JobUtil;
import com.hortonworks.spark.sql.hive.llap.util.SparkToHiveRecordMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.execution.datasources.BasicWriteJobStatsTracker;
import org.apache.spark.sql.execution.datasources.BasicWriteTaskStats;
import org.apache.spark.sql.execution.datasources.OutputWriter;
import org.apache.spark.sql.execution.datasources.WriteTaskStatsTracker;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Data writer for Hive Warehouse Connector batch writes.
 */
public class HiveWarehouseDataWriter implements DataWriter<InternalRow> {
  private static final Logger LOG = LoggerFactory.getLogger(HiveWarehouseDataWriter.class);

  private final String jobId;
  private Configuration hadoopConfiguration;
  private final int partitionId;
  private final long taskId;
  private final FileSystem fileSystem;
  private final Path filePath;
  protected OutputWriter outputWriter;
  private final SparkToHiveRecordMapper sparkToHiveRecordMapper;
  private final WriteTaskStatsTracker taskStatsTracker;
  private final BasicWriteJobStatsTracker basicWriteJobStatsTracker;
  private final HiveWarehouseDataWriterHelper dataWriterHelper;
  private final TimeZone jvmDefaultTimeZone = TimeZone.getDefault();
  private final TimeZone sparkSessionTimeZone;
  private final boolean requiresTimeZoneConversion;
  private final List<Integer> timestampFieldIndexList;

  /**
   * Create a data writer instance.
   *
   * @param configuration Hadoop configuration
   * @param jobId Spark job id
   * @param schema input schema
   * @param fileFormat output file format
   * @param optionMap data source options
   * @param partitionId Spark partition id
   * @param taskId Spark task id
   * @param fileSystem Hadoop file system
   * @param filePath output file path
   * @param sparkToHiveRecordMapper record mapper
   * @param basicWriteJobStatsTracker write stats tracker
   * @param orcVectorizedWriterBatchSize ORC vectorized batch size
   */
  public HiveWarehouseDataWriter(Configuration configuration,
                                 String jobId,
                                 StructType schema,
                                 HiveWarehouseDataWriterHelper.FileFormat fileFormat,
                                 Map<String, String> optionMap,
                                 int partitionId,
                                 long taskId,
                                 FileSystem fileSystem,
                                 Path filePath,
                                 SparkToHiveRecordMapper sparkToHiveRecordMapper,
                                 BasicWriteJobStatsTracker basicWriteJobStatsTracker,
                                 int orcVectorizedWriterBatchSize) {
    this.hadoopConfiguration = configuration;
    this.jobId = jobId;
    this.partitionId = partitionId;
    this.taskId = taskId;
    this.fileSystem = fileSystem;
    this.filePath = filePath;
    this.sparkToHiveRecordMapper = sparkToHiveRecordMapper;
    this.dataWriterHelper = new HiveWarehouseDataWriterHelper(
      configuration,
      schema,
      fileFormat,
      optionMap,
      filePath,
      sparkToHiveRecordMapper,
      orcVectorizedWriterBatchSize);
    this.hadoopConfiguration = this.dataWriterHelper.getConf();
    TaskAttemptContextImpl taskAttemptContext = new TaskAttemptContextImpl(
      this.hadoopConfiguration,
      new TaskAttemptID());
    this.outputWriter = this.dataWriterHelper.getOutputWriter(taskAttemptContext);
    this.basicWriteJobStatsTracker = basicWriteJobStatsTracker;
    this.taskStatsTracker = basicWriteJobStatsTracker.newTaskInstance();
    this.taskStatsTracker.newFile(String.valueOf(filePath));
    this.sparkSessionTimeZone = JobUtil.getSparkSessionTimezoneAtExecutor(configuration);
    this.timestampFieldIndexList = new ArrayList<>();
    for (int fieldIndex = 0; fieldIndex < schema.fields().length; ++fieldIndex) {
      if (schema.fields()[fieldIndex].dataType() instanceof TimestampType) {
        this.timestampFieldIndexList.add(fieldIndex);
      }
    }
    this.requiresTimeZoneConversion =
      !this.jvmDefaultTimeZone.equals(this.sparkSessionTimeZone)
        && !this.timestampFieldIndexList.isEmpty();
    logDebug("CREATED...");
  }

  @Override
  /**
   * {@inheritDoc}
   */
  public void write(InternalRow internalRow) throws IOException {
    if (requiresTimeZoneConversion) {
      // Convert timestamp values from Spark session timezone to JVM default timezone.
      for (Integer timestampFieldIndex : timestampFieldIndexList) {
        int fieldIndex = timestampFieldIndex.intValue();
        internalRow.setLong(
          fieldIndex,
          JobUtil.convertMicros(
            internalRow.getLong(fieldIndex),
            sparkSessionTimeZone,
            jvmDefaultTimeZone));
      }
    }
    outputWriter.write(sparkToHiveRecordMapper.mapToHiveColumns(internalRow));
    taskStatsTracker.newRow(outputWriter.path(), internalRow);
  }

  @Override
  /**
   * {@inheritDoc}
   */
  public WriterCommitMessage commit() throws IOException {
    outputWriter.close();
    BasicWriteTaskStats basicWriteTaskStats =
      (BasicWriteTaskStats) taskStatsTracker.getFinalStats(0L);
    LOG.debug("Stats={}", basicWriteTaskStats);
    logDebug("COMMITTING...");
    return new SimpleWriterCommitMessage(
      String.format("COMMIT %s_%s_%s", jobId, partitionId, taskId),
      filePath,
      basicWriteTaskStats.numRows());
  }

  @Override
  /**
   * {@inheritDoc}
   */
  public void abort() throws IOException {
    LOG.info("Driver sent abort for {}_{}_{}", jobId, partitionId, taskId);
    logInfo("ABORT RECEIVED FROM DRIVER...");
    try {
      outputWriter.close();
    } finally {
      fileSystem.delete(filePath, false);
    }
  }

  @Override
  /**
   * {@inheritDoc}
   */
  public void close() throws IOException {
  }

  private void logInfo(String message) {
    LOG.info("HiveWarehouseDataWriter: {}, path: {}, msg:{} ", this, filePath, message);
  }

  private void logDebug(String message) {
    LOG.debug("HiveWarehouseDataWriter: {}, path: {}, msg:{} ", this, filePath, message);
  }
}
