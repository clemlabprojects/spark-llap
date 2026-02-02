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

import com.hortonworks.spark.sql.hive.llap.util.SparkToHiveRecordMapper;
import java.io.IOException;
import java.util.TimeZone;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.orc.OrcConf;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.spark.sql.avro.AvroOptions;
import org.apache.spark.sql.avro.AvroOutputWriter;
import org.apache.spark.sql.avro.SchemaConverters;
import org.apache.spark.sql.catalyst.csv.CSVOptions;
import org.apache.spark.sql.catalyst.util.CompressionCodecs;
import org.apache.spark.sql.execution.datasources.OutputWriter;
import org.apache.spark.sql.execution.datasources.csv.CsvOutputWriter;
import org.apache.spark.sql.execution.datasources.orc.OrcOptions;
import org.apache.spark.sql.execution.datasources.orc.OrcOutputWriter;
import org.apache.spark.sql.execution.datasources.parquet.ParquetOptions;
import org.apache.spark.sql.execution.datasources.parquet.ParquetOutputWriter;
import org.apache.spark.sql.execution.datasources.parquet.ParquetWriteSupport;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import scala.collection.immutable.Map;

/**
 * Helper for configuring output writers based on file format.
 */
public class HiveWarehouseDataWriterHelper {
  private Configuration hadoopConfiguration;
  private final StructType sparkSchema;
  private final FileFormat outputFileFormat;
  private final java.util.Map<String, String> optionMap;
  private final Map<String, String> optionMapScala;
  private final String outputPath;
  private TaskAttemptContext taskAttemptContext;
  private final SparkToHiveRecordMapper sparkToHiveRecordMapper;
  private Schema outputAvroSchema;
  private final int orcVectorizedWriterBatchSize;

  /**
   * Create a writer helper.
   *
   * @param configuration Hadoop configuration
   * @param schema input schema
   * @param fileFormat output file format
   * @param optionMap data source options
   * @param outputPath base output path
   * @param sparkToHiveRecordMapper record mapper
   * @param orcVectorizedWriterBatchSize ORC vectorized batch size
   */
  public HiveWarehouseDataWriterHelper(Configuration configuration,
                                       StructType schema,
                                       FileFormat fileFormat,
                                       java.util.Map<String, String> optionMap,
                                       Path outputPath,
                                       SparkToHiveRecordMapper sparkToHiveRecordMapper,
                                       int orcVectorizedWriterBatchSize) {
    this.hadoopConfiguration = configuration;
    this.sparkSchema = schema;
    this.outputFileFormat = fileFormat;
    this.outputPath = outputPath.toString();
    this.optionMap = optionMap;
    this.sparkToHiveRecordMapper = sparkToHiveRecordMapper;
    DataFrameWriterUtils dataFrameWriterUtils = new DataFrameWriterUtils();
    this.optionMapScala = dataFrameWriterUtils.MapConverter(optionMap);
    this.orcVectorizedWriterBatchSize = orcVectorizedWriterBatchSize;
  }

  /**
   * Configure the Hadoop configuration based on the output file format.
   */
  public void setConf() {
    switch (outputFileFormat) {
      case ORC:
        setOrcConf();
        break;
      case PARQUET:
        setParquetConf();
        break;
      case AVRO:
        setAvroConf();
        break;
      case TEXT:
        setTextConf();
        break;
      default:
        break;
    }
  }

  /**
   * Return the configured Hadoop configuration.
   *
   * @return Hadoop configuration configured for the output file format
   */
  public Configuration getConf() {
    setConf();
    return hadoopConfiguration;
  }

  private void setTextConf() {
    String compression = optionMap.getOrDefault("compression", optionMap.getOrDefault("codec", "none"));
    String codecClassName = CompressionCodecs.getCodecClassName(compression);
    CompressionCodecs.setCodecConfiguration(hadoopConfiguration, codecClassName);
  }

  private void setAvroConf() {
    AvroOptions avroOptions = new AvroOptions(optionMapScala, hadoopConfiguration);
    outputAvroSchema = SchemaConverters.toAvroType(
      (DataType) sparkSchema,
      false,
      avroOptions.recordName(),
      avroOptions.recordNamespace());
    Job job = null;
    try {
      job = Job.getInstance(hadoopConfiguration);
      job.setOutputKeyClass(AvroKey.class);
      hadoopConfiguration = job.getConfiguration();
    } catch (IOException ioException) {
      ioException.printStackTrace();
    }
    hadoopConfiguration.set("avro.schema.output.key", outputAvroSchema.toString());
    if ("uncompressed".equals(avroOptions.compression())) {
      hadoopConfiguration.setBoolean("mapred.output.compress", false);
    } else {
      hadoopConfiguration.setBoolean("mapred.output.compress", true);
      String compression = avroOptions.compression();
      switch (compression) {
        case "deflate":
          int deflateLevel = Integer.parseInt(SQLConf.AVRO_DEFLATE_LEVEL().defaultValueString());
          hadoopConfiguration.setInt("avro.mapred.deflate.level", deflateLevel);
          compression = "deflate";
          break;
        case "snappy":
        case "bzip2":
        case "xz":
          break;
        default:
          throw new IllegalArgumentException("Invalid compression codec: " + compression);
      }
      hadoopConfiguration.set("avro.output.codec", compression);
    }
  }

  private void setParquetConf() {
    Job job = null;
    try {
      job = Job.getInstance(hadoopConfiguration);
      job.setOutputFormatClass(ParquetOutputFormat.class);
      hadoopConfiguration = job.getConfiguration();
    } catch (IOException ioException) {
      ioException.printStackTrace();
    }
    ParquetOutputFormat.setWriteSupportClass(job, ParquetWriteSupport.class);
    ParquetWriteSupport.setSchema(sparkSchema, hadoopConfiguration);
    hadoopConfiguration.setIfUnset(SQLConf.PARQUET_WRITE_LEGACY_FORMAT().key(), "false");
    hadoopConfiguration.setIfUnset(
      SQLConf.PARQUET_OUTPUT_COMMITTER_CLASS().key(),
      "org.apache.parquet.hadoop.ParquetOutputCommitter");
    hadoopConfiguration.setIfUnset(SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE().key(), "TIMESTAMP_MICROS");
    ParquetOptions parquetOptions = new ParquetOptions(optionMapScala, new SQLConf());
    hadoopConfiguration.set("parquet.compression", parquetOptions.compressionCodecClassName());
  }

  private void setOrcConf() {
    hadoopConfiguration.set(
      "orc.mapred.output.schema",
      sparkToHiveRecordMapper.getSchemaInHiveColumnsOrder().catalogString());
    OrcOptions orcOptions = new OrcOptions(optionMapScala, new SQLConf());
    hadoopConfiguration.set(OrcConf.COMPRESS.getAttribute(), orcOptions.compressionCodec());
  }

  /**
   * Create an output writer for the configured file format.
   *
   * @param taskAttemptContext Hadoop task attempt context
   * @return output writer implementation
   */
  public OutputWriter getOutputWriter(TaskAttemptContext taskAttemptContext) {
    this.taskAttemptContext = taskAttemptContext;
    switch (outputFileFormat) {
      case ORC:
        return getOrcOutputWriter();
      case PARQUET:
        return getParquetOutputWriter();
      case AVRO:
        return getAvroOutputWriter();
      case TEXT:
        return getTextOutputWriter();
      default:
        break;
    }
    throw new IllegalArgumentException(
      "Output Writer for " + outputFileFormat + " is not supported");
  }

  private OutputWriter getTextOutputWriter() {
    CSVOptions csvOptions =
      new CSVOptions(optionMapScala, true, TimeZone.getDefault().getID(), "");
    return new CsvOutputWriter(outputPath, sparkSchema, taskAttemptContext, csvOptions);
  }

  private OutputWriter getAvroOutputWriter() {
    return new AvroOutputWriter(outputPath, taskAttemptContext, sparkSchema, true, outputAvroSchema);
  }

  private OutputWriter getParquetOutputWriter() {
    return new ParquetOutputWriter(outputPath, taskAttemptContext);
  }

  private OutputWriter getOrcOutputWriter() {
    return new OrcOutputWriter(outputPath, sparkSchema, taskAttemptContext, orcVectorizedWriterBatchSize);
  }

  /**
   * Supported output file formats.
   */
  public enum FileFormat {
    ORC,
    PARQUET,
    AVRO,
    TEXT;

    @Override
    public String toString() {
      switch (this) {
        case ORC:
          return "orc";
        case PARQUET:
          return "parquet";
        case AVRO:
          return "avro";
        case TEXT:
          return "textfile";
        default:
          return null;
      }
    }

    /**
     * Resolve a {@link FileFormat} from a string.
     *
     * @param formatName format name
     * @return resolved file format
     */
    public static FileFormat getFormat(String formatName) {
      switch (formatName.toLowerCase()) {
        case "orc":
          return ORC;
        case "parquet":
          return PARQUET;
        case "avro":
          return AVRO;
        case "textfile":
          return TEXT;
        default:
          throw new IllegalArgumentException("Format '" + formatName + "' is not a supported format.");
      }
    }
  }
}
