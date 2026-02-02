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
import com.hortonworks.spark.sql.hive.llap.util.HiveQlUtil;
import com.hortonworks.spark.sql.hive.llap.util.SchemaUtil;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.antlr.runtime.tree.Tree;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Builds the list of SQL statements needed to write data into a Hive table.
 */
public class DataWriteQueryBuilder {
  private final String databaseName;
  private final String tableName;
  private final String sourceFilePath;
  private final String jobId;
  private final StructType dataFrameSchema;
  private boolean overwriteData = false;
  private String partitionSpec;
  private HiveWarehouseDataWriterHelper.FileFormat storageFormat = HiveWarehouseDataWriterHelper.FileFormat.ORC;
  private boolean validateAgainstHiveColumns;
  private boolean dynamicPartitionPresent = false;
  private LinkedHashMap<String, String> partitions;
  private boolean createTable = false;
  private boolean loadData = false;
  private DescribeTableOutput describeTableOutput;
  private HWCOptions hwcOptions;

  /**
   * Creates a query builder for a given write operation.
   *
   * @param databaseName target database name
   * @param tableName target table name
   * @param sourceFilePath source data path
   * @param jobId job identifier used for temporary table naming
   * @param dataFrameSchema schema of the Spark DataFrame being written
   */
  public DataWriteQueryBuilder(String databaseName,
                               String tableName,
                               String sourceFilePath,
                               String jobId,
                               StructType dataFrameSchema) {
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.sourceFilePath = sourceFilePath;
    this.jobId = jobId;
    this.dataFrameSchema = dataFrameSchema;
  }

  /**
   * Enables or disables CREATE TABLE statement generation.
   *
   * @param createTable whether to generate CREATE TABLE
   * @return this builder for chaining
   */
  public DataWriteQueryBuilder withCreateTableQuery(boolean createTable) {
    this.createTable = createTable;
    return this;
  }

  /**
   * Enables or disables LOAD/INSERT statements for the write.
   *
   * @param loadData whether to generate data-load statements
   * @return this builder for chaining
   */
  public DataWriteQueryBuilder withLoadData(boolean loadData) {
    this.loadData = loadData;
    return this;
  }

  /**
   * Enables or disables overwrite mode.
   *
   * @param overwriteData whether to overwrite existing data
   * @return this builder for chaining
   */
  public DataWriteQueryBuilder withOverwriteData(boolean overwriteData) {
    this.overwriteData = overwriteData;
    return this;
  }

  /**
   * Sets the partition specification string for the write.
   *
   * @param partitionSpec partition spec string
   * @return this builder for chaining
   */
  public DataWriteQueryBuilder withPartitionSpec(String partitionSpec) {
    this.partitionSpec = partitionSpec;
    return this;
  }

  /**
   * Sets the storage format used for the temporary table or CREATE TABLE.
   *
   * @param storageFormat file format
   * @return this builder for chaining
   */
  public DataWriteQueryBuilder withStorageFormat(HiveWarehouseDataWriterHelper.FileFormat storageFormat) {
    this.storageFormat = storageFormat;
    return this;
  }

  /**
   * Enables validation of DataFrame columns against Hive table columns.
   *
   * @param validateAgainstHiveColumns whether to validate column order
   * @return this builder for chaining
   */
  public DataWriteQueryBuilder withValidateAgainstHiveColumns(boolean validateAgainstHiveColumns) {
    this.validateAgainstHiveColumns = validateAgainstHiveColumns;
    return this;
  }

  /**
   * Sets the DESCRIBE TABLE output used for validation.
   *
   * @param describeTableOutput table metadata output
   * @return this builder for chaining
   */
  public DataWriteQueryBuilder withDescribeTableOutput(DescribeTableOutput describeTableOutput) {
    this.describeTableOutput = describeTableOutput;
    return this;
  }

  /**
   * Sets HWC options used for CREATE TABLE generation.
   *
   * @param hwcOptions parsed options
   * @return this builder for chaining
   */
  public DataWriteQueryBuilder withOptions(HWCOptions hwcOptions) {
    this.hwcOptions = hwcOptions;
    return this;
  }

  /**
   * Builds the ordered list of SQL statements to execute.
   *
   * @return list of SQL statements
   */
  public List<String> build() {
    parsePartitionSpec();

    List<String> queryList = new ArrayList<>();

    if (this.validateAgainstHiveColumns) {
      Preconditions.checkState(this.describeTableOutput != null,
          "describeTableOutput cannot be null when validateAgainstHiveColumns is true");
      ensureDataFrameNonPartitionColumnsMatchHive();
      ensureDataFrameColumnsOrderAgainstHivePartitions();
    }

    if (this.createTable) {
      ensureDynamicPartitionColumnsAtEndWhenCreatingTable();
      queryList.add(buildCreateTableQuery());
    }

    if (this.loadData) {
      boolean requiresTempTable = this.dynamicPartitionPresent
          || (this.describeTableOutput != null && this.describeTableOutput.considerDefaultCols());
      if (requiresTempTable) {
        // Use a temp table for dynamic partitions or default-constraint handling.
        queryList.add(createTempTableQuery());
        queryList.add(insertOverwriteQuery());
      } else {
        queryList.add(HiveQlUtil.loadInto(
            this.sourceFilePath,
            this.databaseName,
            this.tableName,
            this.overwriteData,
            isPartitionPresent() ? this.partitionSpec : null));
      }
    }

    return queryList;
  }

  private void ensureDataFrameNonPartitionColumnsMatchHive() {
    List<Column> hiveColumns = this.describeTableOutput.getColumns();
    String[] dataFrameColumns = this.dataFrameSchema.fieldNames();
    String errorMessage = String.format(
        "Number of columns in dataframe:%s should be >= no of non partitioned columns in hive: %s",
        dataFrameColumns.length,
        hiveColumns.size());
    Preconditions.checkArgument(hiveColumns.size() <= dataFrameColumns.length, errorMessage);

    String mismatchMessageTemplate =
        "Hive column: %s cannot be found at same index: %s in dataframe. Found %s. Aborting as "
            + "this may lead to loading of incorrect data.";
    for (int i = 0; i < hiveColumns.size(); ++i) {
      String hiveColumnName = hiveColumns.get(i).getName();
      Preconditions.checkArgument(hiveColumnName.equalsIgnoreCase(dataFrameColumns[i]),
          String.format(mismatchMessageTemplate, hiveColumnName, i, dataFrameColumns[i]));
    }
  }

  private void ensureDataFrameColumnsOrderAgainstHivePartitions() {
    if (this.describeTableOutput.getPartitionedColumns() != null
        && !this.describeTableOutput.getPartitionedColumns().isEmpty()
        && this.partitions == null) {
      ensureDataFrameColumnsOrderAgainstHivePartitions(columnName -> true);
    } else if (this.dynamicPartitionPresent) {
      ensureDataFrameColumnsOrderAgainstHivePartitions(this::isDynamicPartitionCol);
    }
  }

  private void ensureDataFrameColumnsOrderAgainstHivePartitions(Predicate<String> columnFilter) {
    Object[] dataFrameColumns = this.dataFrameSchema.fieldNames();
    int dataFrameColumnCount = dataFrameColumns.length;
    List<Column> hivePartitionColumns = this.describeTableOutput.getPartitionedColumns();
    int hivePartitionCount = hivePartitionColumns.size();
    String errorMessage = String.format(
        "Number of columns in dataframe: %s should be greater than partitioned columns in hive "
            + "table definition: %s",
        Arrays.toString(dataFrameColumns),
        hivePartitionColumns);
    Preconditions.checkArgument(dataFrameColumnCount > hivePartitionCount, errorMessage);

    int hiveIndex = hivePartitionCount - 1;
    int dataFrameIndex = dataFrameColumnCount - 1;
    while (hiveIndex >= 0) {
      String hivePartitionName = hivePartitionColumns.get(hiveIndex).getName();
      if (columnFilter.test(hivePartitionName)) {
        Object dataFrameColumn = dataFrameColumns[dataFrameIndex];
        String mismatchMessage = String.format(
            "Order of dynamic partition columns in schema must match to that given in Hive table "
                + "definition or if no partSpec is specified then order of all partition cols "
                + "should match. Dataframe schema: %s, Partitioned Cols in Hive: %s. Unable to "
                + "find partition column: %s at same position: %s (from last) in dataframe schema",
            Arrays.toString(dataFrameColumns),
            hivePartitionColumns,
            hivePartitionName,
            hivePartitionCount - hiveIndex - 1);
        Preconditions.checkArgument(hivePartitionName.equalsIgnoreCase((String) dataFrameColumn),
            mismatchMessage);
      }
      --hiveIndex;
      --dataFrameIndex;
    }
  }

  private void ensureDynamicPartitionColumnsAtEndWhenCreatingTable() {
    if (this.dynamicPartitionPresent) {
      ArrayList<String> partitionColumns = new ArrayList<>(this.partitions.keySet());
      Object[] dataFrameColumns = this.dataFrameSchema.fieldNames();
      int dataFrameColumnCount = dataFrameColumns.length;
      int partitionColumnCount = partitionColumns.size();
      String errorMessage = String.format(
          "Number of columns in dataframe should be > number of partition columns in partition "
              + "Spec. Dataframe cols: %s, Partition cols: %s",
          Arrays.toString(dataFrameColumns),
          partitionColumns);
      Preconditions.checkArgument(partitionColumnCount < dataFrameColumnCount, errorMessage);

      int dataFrameIndex = dataFrameColumnCount - 1;
      for (int i = partitionColumnCount - 1; i >= 0; --i) {
        String partitionColumn = partitionColumns.get(i);
        if (!isDynamicPartitionCol(partitionColumn)) {
          continue;
        }
        Object dataFrameColumn = dataFrameColumns[dataFrameIndex];
        String mismatchMessage = String.format(
            "When creating table, order of dynamic partition columns in df schema and "
                + "partitionSpec must match. Dataframe schema: %s, Dynamic partition columns: %s. "
                + "Partition column %s is not at the same position(from last) in DF schema.",
            Arrays.toString(dataFrameColumns),
            partitionColumns,
            partitionColumn);
        Preconditions.checkArgument(partitionColumn.equalsIgnoreCase((String) dataFrameColumn),
            mismatchMessage);
        --dataFrameIndex;
      }
    }
  }

  private String buildCreateTableQuery() {
    CreateTableBuilder createTableBuilder =
        new CreateTableBuilder(null, this.databaseName, this.tableName, this.storageFormat, this.hwcOptions);
    boolean partitionEncountered = false;
    for (StructField field : this.dataFrameSchema.fields()) {
      if (!isPartitionCol(field.name())) {
        if (partitionEncountered) {
          String errorMessage = String.format(
              "Non partitioned Column: %s is not allowed after partitioned column. Please ensure "
                  + "to place partitioned columns after non-partitioned ones.",
              field.name());
          throw new IllegalArgumentException(errorMessage);
        }
        createTableBuilder.column(field.name(), SchemaUtil.getHiveType(field.dataType(), field.metadata()));
      } else {
        partitionEncountered = true;
      }
    }

    if (this.partitions != null) {
      for (Map.Entry<String, String> partitionEntry : this.partitions.entrySet()) {
        StructField structField = this.dataFrameSchema.fields()[
            this.dataFrameSchema.fieldIndex(partitionEntry.getKey())];
        String typeString = SchemaUtil.getHiveType(structField.dataType(), structField.metadata());
        createTableBuilder.partition(structField.name(), typeString);
      }
    }
    return createTableBuilder.toString();
  }

  private boolean isPartitionPresent() {
    return this.partitions != null && !this.partitions.isEmpty();
  }

  /**
   * Indicates whether the partition specification contains dynamic partitions.
   *
   * @return {@code true} if dynamic partitions are present
   */
  public boolean isDynamicPartitionPresent() {
    return this.dynamicPartitionPresent;
  }

  private boolean isPartitionCol(String columnName) {
    return this.partitions != null && this.partitions.containsKey(columnName);
  }

  private boolean isStaticPartitionCol(String columnName) {
    return isPartitionCol(columnName) && this.partitions.get(columnName) != null;
  }

  private boolean isDynamicPartitionCol(String columnName) {
    return isPartitionCol(columnName) && this.partitions.get(columnName) == null;
  }

  private String insertOverwriteQuery() {
    if (this.describeTableOutput != null
        && this.describeTableOutput.considerDefaultCols()
        && this.overwriteData) {
      throw new IllegalArgumentException(
          "Default constraints cannot be supported during overwriting of table data. Consider "
              + "setting spark.datasource.hive.warehouse.consider.default.constraints=false.");
    }

    StringBuilder queryBuilder = new StringBuilder("INSERT ")
        .append(this.overwriteData ? "OVERWRITE" : "INTO")
        .append(" TABLE ")
        .append(this.databaseName)
        .append('.')
        .append(this.tableName);

    if (this.describeTableOutput != null
        && this.describeTableOutput.considerDefaultCols()
        && !this.describeTableOutput.getColumns().isEmpty()) {
      queryBuilder.append(" (")
          .append(this.describeTableOutput.getColumns().stream()
              .map(Column::getName)
              .collect(Collectors.joining(", ")))
          .append(") ");
    }

    if (StringUtils.isNotBlank(this.partitionSpec)) {
      queryBuilder.append(" PARTITION (").append(this.partitionSpec).append(") ");
    }

    queryBuilder.append(" SELECT ");
    String selectColumns = Arrays.stream(this.dataFrameSchema.fields())
        .map(StructField::name)
        .filter(columnName -> !isStaticPartitionCol(columnName))
        .map(columnName -> columnName.startsWith("`") ? columnName : "`" + columnName + "`")
        .collect(Collectors.joining(","));
    queryBuilder.append(selectColumns).append(" FROM ").append(getTempTableName());
    return queryBuilder.toString();
  }

  private String createTempTableQuery() {
    StringBuilder queryBuilder = new StringBuilder("CREATE TEMPORARY EXTERNAL TABLE ")
        .append(getTempTableName())
        .append('(');
    String columns = Arrays.stream(this.dataFrameSchema.fields())
        .map(structField ->
            (structField.name().startsWith("`") ? structField.name() : "`" + structField.name() + "`")
                + " "
                + SchemaUtil.getHiveType(structField.dataType(), structField.metadata()))
        .collect(Collectors.joining(","));
    queryBuilder.append(columns);
    queryBuilder.append(") STORED AS ").append(this.storageFormat)
        .append(" LOCATION ").append(HiveQlUtil.wrapWithSingleQuotes(this.sourceFilePath));
    return queryBuilder.toString();
  }

  private String getTempTableName() {
    return this.databaseName + "." + HiveQlUtil.getCleanSqlAlias(this.jobId);
  }

  private void parsePartitionSpec() {
    if (StringUtils.isNotBlank(this.partitionSpec)) {
      Tree partitionTree = getPartitionSpecNode();
      this.partitions = new LinkedHashMap<>(partitionTree.getChildCount());
      for (int i = 0; i < partitionTree.getChildCount(); ++i) {
        Tree partitionNode = partitionTree.getChild(i);
        if (partitionNode.getChildCount() == 2) {
          this.partitions.put(partitionNode.getChild(0).getText(), partitionNode.getChild(1).getText());
        } else {
          this.dynamicPartitionPresent = true;
          this.partitions.put(partitionNode.getChild(0).getText(), null);
        }
      }
    }
  }

  private Tree getPartitionSpecNode() {
    String parseQuery = String.format("INSERT INTO TABLE t1 PARTITION (%s) SELECT 1", this.partitionSpec);
    ASTNode rootNode;
    try {
      rootNode = ParseUtils.parse(parseQuery, null);
    } catch (ParseException parseException) {
      throw new IllegalArgumentException("Invalid partition spec: " + this.partitionSpec, parseException);
    }
    return rootNode.getChild(0).getChild(0).getChild(0).getChild(1);
  }
}
