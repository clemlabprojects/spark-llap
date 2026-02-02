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
import com.hortonworks.spark.sql.hive.llap.util.SerializableHadoopConfiguration;
import com.hortonworks.spark.sql.hive.llap.util.SparkToHiveRecordMapper;
import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.execution.datasources.BasicWriteJobStatsTracker;
import org.apache.spark.sql.types.StructType;

/**
 * Factory that creates {@link HiveWarehouseDataWriter} instances.
 */
public class HiveWarehouseDataWriterFactory implements DataWriterFactory {
  protected final String jobId;
  protected final StructType schema;
  private final HiveWarehouseDataWriterHelper.FileFormat fileFormat;
  private final Path basePath;
  private final Map<String, String> optionMap;
  private final SerializableHadoopConfiguration serializableConfiguration;
  private final SparkToHiveRecordMapper sparkToHiveRecordMapper;
  private final BasicWriteJobStatsTracker basicWriteJobStatsTracker;
  private final int orcVectorizedWriterBatchSize;

  /**
   * Create a data writer factory.
   *
   * @param jobId Spark job id
   * @param schema input schema
   * @param fileFormat target file format
   * @param basePath output base path
   * @param optionMap data source options
   * @param serializableConfiguration Hadoop configuration wrapper
   * @param sparkToHiveRecordMapper Spark-to-Hive record mapper
   * @param basicWriteJobStatsTracker write stats tracker
   */
  public HiveWarehouseDataWriterFactory(String jobId,
                                        StructType schema,
                                        HiveWarehouseDataWriterHelper.FileFormat fileFormat,
                                        Path basePath,
                                        Map<String, String> optionMap,
                                        SerializableHadoopConfiguration serializableConfiguration,
                                        SparkToHiveRecordMapper sparkToHiveRecordMapper,
                                        BasicWriteJobStatsTracker basicWriteJobStatsTracker) {
    this.jobId = jobId;
    this.schema = schema;
    this.fileFormat = fileFormat;
    this.basePath = basePath;
    this.optionMap = optionMap;
    this.serializableConfiguration = serializableConfiguration;
    this.sparkToHiveRecordMapper = sparkToHiveRecordMapper;
    this.basicWriteJobStatsTracker = basicWriteJobStatsTracker;
    this.orcVectorizedWriterBatchSize =
      JobUtil.getSparkSession().sqlContext().conf().orcVectorizedWriterBatchSize();
  }

  @Override
  /**
   * {@inheritDoc}
   */
  public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
    Path filePath = new Path(basePath, String.format("%s_%s_%s", jobId, partitionId, taskId));
    FileSystem fileSystem = null;
    try {
      fileSystem = filePath.getFileSystem(serializableConfiguration.get());
    } catch (IOException ioException) {
      ioException.printStackTrace();
    }
    return getDataWriter(
      serializableConfiguration.get(),
      jobId,
      schema,
      fileFormat,
      optionMap,
      partitionId,
      taskId,
      fileSystem,
      filePath,
      sparkToHiveRecordMapper,
      basicWriteJobStatsTracker,
      orcVectorizedWriterBatchSize);
  }

  /**
   * Create a new data writer for the given partition and task.
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
   * @param sparkToHiveRecordMapper Spark-to-Hive record mapper
   * @param basicWriteJobStatsTracker write stats tracker
   * @param orcVectorizedWriterBatchSize ORC vectorized batch size
   * @return data writer instance
   */
  protected DataWriter<InternalRow> getDataWriter(
      Configuration configuration,
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
    return new HiveWarehouseDataWriter(
      configuration,
      jobId,
      schema,
      fileFormat,
      optionMap,
      partitionId,
      taskId,
      fileSystem,
      filePath,
      sparkToHiveRecordMapper,
      basicWriteJobStatsTracker,
      orcVectorizedWriterBatchSize);
  }
}
