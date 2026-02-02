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

import com.google.common.base.Preconditions;
import com.hortonworks.spark.sql.hive.llap.util.HWCOptions;
import com.hortonworks.spark.sql.hive.llap.util.QueryExecutionUtil;
import com.hortonworks.spark.sql.hive.llap.util.SchemaUtil;
import com.hortonworks.spark.sql.hive.llap.util.SerializableHadoopConfiguration;
import com.hortonworks.spark.sql.hive.llap.util.SparkToHiveRecordMapper;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.execution.datasources.BasicWriteJobStatsTracker;
import org.apache.spark.sql.execution.datasources.BasicWriteJobStatsTracker$;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.SerializableConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Spark DataSource V2 batch writer for Hive Warehouse Connector tables.
 */
public class HiveWarehouseDataSourceWriter implements BatchWrite {
  private static final Logger LOG = LoggerFactory.getLogger(HiveWarehouseDataSourceWriter.class);
  private final String jobId;
  private final SaveMode saveMode;
  private final Path stagingPath;
  private final Configuration hadoopConfiguration;
  private final Map<String, String> writeOptions;
  private final HWCOptions hwcOptions;
  private final boolean strictColumnNamesMappingEnabled;
  private final boolean considerDefaultColumns;
  private final HiveWarehouseDataWriterHelper.FileFormat defaultTableFormat;

  private StructType schema;
  private HiveWarehouseDataWriterHelper.FileFormat fileFormat;
  private boolean tableExists;
  private String tableName;
  private String databaseName;
  private SparkToHiveRecordMapper sparkToHiveRecordMapper;
  private DescribeTableOutput describeTableOutput;
  private BasicWriteJobStatsTracker writeJobStatsTracker;

  /**
   * Creates a writer for a Spark write job.
   *
   * @param options Spark write options
   * @param jobId Spark job id
   * @param schema Spark output schema
   * @param outputPath base output path
   * @param configuration Hadoop configuration
   * @param saveMode Spark save mode
   */
  public HiveWarehouseDataSourceWriter(
      Map<String, String> options,
      String jobId,
      StructType schema,
      Path outputPath,
      Configuration configuration,
      SaveMode saveMode) {
    this.writeOptions = options;
    this.jobId = jobId;
    this.schema = schema;
    this.saveMode = saveMode;
    this.stagingPath = new Path(outputPath, jobId);
    this.hadoopConfiguration = configuration;
    this.hwcOptions = new HWCOptions(options);
    this.defaultTableFormat = HiveWarehouseDataWriterHelper.FileFormat.getFormat(
        HWConf.DEFAULT_WRITE_FORMAT.getFromOptionsMap(options));
    this.strictColumnNamesMappingEnabled =
        BooleanUtils.toBoolean(HWConf.WRITE_PATH_STRICT_COLUMN_NAMES_MAPPING.getFromOptionsMap(options));
    this.considerDefaultColumns =
        BooleanUtils.toBoolean(HWConf.ENFORCE_DEFAULT_CONSTRAINTS.getFromOptionsMap(options));

    populateDatabaseAndTableNames();
    checkExistingStagingPath();
  }

  /**
   * Creates a DataWriterFactory for Spark executors.
   *
   * @param physicalWriteInfo Spark physical write info
   * @return writer factory
   */
  @Override
  public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo physicalWriteInfo) {
    String[] hiveColumnOrder = null;
    try (Connection connection = getConnection()) {
      ensureDatabaseExists(connection);
      this.tableExists = DefaultJDBCWrapper.tableExists(connection, databaseName, tableName);
      if (tableExists) {
        describeTableOutput = DefaultJDBCWrapper.describeTable(connection, databaseName, tableName);
        describeTableOutput.setConsiderDefaultCols(considerDefaultColumns);
        hiveColumnOrder = buildHiveColumnOrder(describeTableOutput);
      }
      resolveTableFormat();
    } catch (Exception exception) {
      throw new RuntimeException(exception.getMessage(), exception);
    }

    this.writeJobStatsTracker = createWriteJobStatsTracker(hadoopConfiguration);
    this.sparkToHiveRecordMapper = new SparkToHiveRecordMapper(schema, hiveColumnOrder);
    this.schema = sparkToHiveRecordMapper.getSchemaInHiveColumnsOrder();

    return new HiveWarehouseDataWriterFactory(
        jobId,
        schema,
        fileFormat,
        stagingPath,
        writeOptions,
        new SerializableHadoopConfiguration(hadoopConfiguration),
        sparkToHiveRecordMapper,
        writeJobStatsTracker);
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
   * Commits all writers and loads data into Hive.
   *
   * @param writerCommitMessages commit messages from executors
   */
  @Override
  public void commit(WriterCommitMessage[] writerCommitMessages) {
    boolean shouldLoadData = shouldLoadDataAndCleanStagingDir(writerCommitMessages);
    try (Connection connection = getConnection()) {
      handleWriteWithSaveMode(databaseName, tableName, connection, shouldLoadData);
    } catch (SQLException sqlException) {
      throw new RuntimeException(sqlException.getMessage(), sqlException);
    } finally {
      cleanupStagingPath();
      LOG.info("Commit job {}", jobId);
    }
  }

  /**
   * Aborts the write and deletes staging data.
   *
   * @param writerCommitMessages commit messages from executors
   */
  @Override
  public void abort(WriterCommitMessage[] writerCommitMessages) {
    cleanupStagingPath();
    LOG.error("Aborted DataWriter job {}", jobId);
  }

  private void populateDatabaseAndTableNames() {
    SchemaUtil.TableRef tableRef = SchemaUtil.getDbTableNames(hwcOptions.database, hwcOptions.table);
    this.tableName = tableRef.tableName;
    this.databaseName = tableRef.databaseName;
  }

  private void checkExistingStagingPath() {
    try {
      if (stagingPath.getFileSystem(hadoopConfiguration).exists(stagingPath)) {
        logInfo("DataSourceWriter found target directory already exists: " + stagingPath);
      }
    } catch (IOException ioException) {
      throw new RuntimeException(
          "Unable to access the file system for the path (" + stagingPath + ") due to "
              + ioException.getMessage(),
          ioException);
    }
  }

  private void ensureDatabaseExists(Connection connection) {
    Preconditions.checkArgument(
        DefaultJDBCWrapper.databaseExists(connection, databaseName),
        "Database " + databaseName + " doesn't exist");
  }

  private BasicWriteJobStatsTracker createWriteJobStatsTracker(Configuration configuration) {
    SerializableConfiguration serializableConfiguration = new SerializableConfiguration(configuration);
    return new BasicWriteJobStatsTracker(
        serializableConfiguration, BasicWriteJobStatsTracker$.MODULE$.metrics());
  }

  private String[] buildHiveColumnOrder(DescribeTableOutput tableDescription) {
    List<Column> tableColumns = tableDescription.getColumns();
    List<Column> partitionColumns = tableDescription.getPartitionedColumns();
    String[] hiveColumnOrder = new String[tableColumns.size() + partitionColumns.size()];
    int columnIndex = 0;
    for (Column column : tableColumns) {
      hiveColumnOrder[columnIndex++] = column.getName();
    }
    for (Column partitionColumn : partitionColumns) {
      hiveColumnOrder[columnIndex++] = partitionColumn.getName();
    }
    return hiveColumnOrder;
  }

  private void resolveFieldDelimiter() {
    String tableFieldDelimiter =
        describeTableOutput.findByDatatypeNameInStorageInfo("field.delim", true).getComment();
    if (hwcOptions.fieldDelimiter != null) {
      Preconditions.checkArgument(
          hwcOptions.fieldDelimiter.equals(tableFieldDelimiter),
          "Field Delimiter provided '" + hwcOptions.fieldDelimiter
              + "' doesn't match table " + tableName
              + "'s existing field delimiter '" + tableFieldDelimiter + "'");
    }
    writeOptions.putIfAbsent("sep", tableFieldDelimiter);
  }

  private void resolveTableFormat() {
    if (tableExists) {
      String serdeLibrary =
          describeTableOutput.findByColNameInStorageInfo("SerDe Library:", true).getDataType();
      if (serdeLibrary.endsWith("OrcSerde")) {
        fileFormat = HiveWarehouseDataWriterHelper.FileFormat.ORC;
      }
      if (serdeLibrary.endsWith("ParquetHiveSerDe")) {
        fileFormat = HiveWarehouseDataWriterHelper.FileFormat.PARQUET;
      }
      if (serdeLibrary.endsWith("AvroSerDe")) {
        fileFormat = HiveWarehouseDataWriterHelper.FileFormat.AVRO;
      }
      if (serdeLibrary.endsWith("LazySimpleSerDe") || serdeLibrary.endsWith("MultiDelimitSerDe")) {
        fileFormat = HiveWarehouseDataWriterHelper.FileFormat.TEXT;
      }
      Preconditions.checkNotNull(fileFormat,
          "Writes are supported for ORC, Parquet, Avro and Text tables only.");
      if (hwcOptions.fileFormat != null) {
        Preconditions.checkArgument(
            fileFormat.equals(hwcOptions.fileFormat),
            "Table " + tableName + "'s format is " + fileFormat
                + " and doesn't match the specified format " + hwcOptions.fileFormat);
      }
      if (HiveWarehouseDataWriterHelper.FileFormat.TEXT.equals(fileFormat)) {
        resolveFieldDelimiter();
      }
    } else {
      fileFormat = hwcOptions.fileFormat == null ? defaultTableFormat : hwcOptions.fileFormat;
    }
  }

  private boolean shouldLoadDataAndCleanStagingDir(WriterCommitMessage[] writerCommitMessages) {
    long totalRowsWritten = 0L;
    for (WriterCommitMessage commitMessage : writerCommitMessages) {
      totalRowsWritten += ((SimpleWriterCommitMessage) commitMessage).getNumRowsWritten();
    }

    LOG.info("Rows written by all executors: {}", totalRowsWritten);
    if (totalRowsWritten <= 0L) {
      return false;
    }

    Map<String, Path> committedFilesByName = new HashMap<>(writerCommitMessages.length);
    Map<String, Long> committedRowsByName = new HashMap<>(writerCommitMessages.length);
    for (WriterCommitMessage commitMessage : writerCommitMessages) {
      SimpleWriterCommitMessage simpleMessage = (SimpleWriterCommitMessage) commitMessage;
      Path committedFilePath = simpleMessage.getWrittenFilePath();
      long rowsWritten = simpleMessage.getNumRowsWritten();
      String fileName = committedFilePath.getName();
      committedFilesByName.put(fileName, committedFilePath);
      committedRowsByName.put(fileName, rowsWritten);
      logInfo("Committed File " + committedFilePath);
    }

    try {
      FileSystem fileSystem = stagingPath.getFileSystem(hadoopConfiguration);
      RemoteIterator<LocatedFileStatus> fileIterator = fileSystem.listFiles(stagingPath, false);
      while (fileIterator.hasNext()) {
        LocatedFileStatus fileStatus = fileIterator.next();
        Path filePath = fileStatus.getPath();
        Long rowsWritten = committedRowsByName.get(filePath.getName());
        boolean isValidFile = !fileStatus.isDirectory()
            && committedFilesByName.containsKey(filePath.getName())
            && rowsWritten != null
            && rowsWritten != 0L;
        if (isValidFile) {
          continue;
        }
        logInfo("Deleting invalid file/directory " + filePath);
        fileSystem.delete(filePath, true);
      }
    } catch (IOException ioException) {
      throw new IllegalStateException(
          "Failed to traverse the target directory " + stagingPath + " due to "
              + ioException.getMessage(),
          ioException);
    }

    return true;
  }

  private void handleWriteWithSaveMode(
      String databaseName,
      String tableName,
      Connection connection,
      boolean shouldLoadData) {
    boolean shouldCreateTable = false;
    boolean overwriteData = false;
    boolean loadData = shouldLoadData;

    switch (saveMode) {
      case ErrorIfExists:
        if (tableExists) {
          throw new IllegalArgumentException(
              "Table[" + tableName + "] already exists, please specify a different SaveMode");
        }
        shouldCreateTable = true;
        break;
      case Overwrite:
        overwriteData = true;
        // Fall through to Append behavior for table creation if missing.
      case Append:
        if (!tableExists) {
          shouldCreateTable = true;
        }
        break;
      case Ignore:
        if (tableExists) {
          shouldCreateTable = false;
          loadData = false;
        } else {
          shouldCreateTable = true;
        }
        break;
      default:
        break;
    }

    LOG.info(
        "Handling write: database:{}, table:{}, savemode:{}, tableExists:{}, createTable:{}, loadData:{}",
        databaseName,
        tableName,
        saveMode,
        tableExists,
        shouldCreateTable,
        loadData);

    if (loadData || shouldCreateTable) {
      boolean validateAgainstHiveColumns = tableExists && strictColumnNamesMappingEnabled;
      DataWriteQueryBuilder queryBuilder = new DataWriteQueryBuilder(
          databaseName,
          tableName,
          stagingPath.toString(),
          jobId,
          sparkToHiveRecordMapper.getSchemaInHiveColumnsOrder())
          .withOverwriteData(overwriteData)
          .withPartitionSpec(writeOptions.get("partition"))
          .withCreateTableQuery(shouldCreateTable)
          .withStorageFormat(fileFormat)
          .withValidateAgainstHiveColumns(validateAgainstHiveColumns)
          .withDescribeTableOutput(describeTableOutput)
          .withLoadData(loadData)
          .withOptions(hwcOptions);

      List<String> queries = queryBuilder.build();
      if (queryBuilder.isDynamicPartitionPresent()) {
        DefaultJDBCWrapper.setSessionLevelProps(
            connection,
            "hive.exec.dynamic.partition=true",
            "hive.exec.dynamic.partition.mode=nonstrict");
      }
      for (String query : queries) {
        LOG.info("Load data query: {}", query);
        DefaultJDBCWrapper.executeUpdate(connection, databaseName, query, true);
      }
    }
  }

  private Connection getConnection() {
    return QueryExecutionUtil.getConnection(writeOptions);
  }

  private void cleanupStagingPath() {
    try {
      stagingPath.getFileSystem(hadoopConfiguration).delete(stagingPath, true);
    } catch (Exception exception) {
      LOG.warn("Failed to cleanup temp dir {}", stagingPath);
    }
  }

  private void logInfo(String message) {
    LOG.info("HiveWarehouseDataSourceWriter: {}, msg:{}", this, message);
  }
}
