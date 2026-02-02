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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.catalog.SessionConfigSupport;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * Mock connector for unit testing Spark DataSource V2 integration.
 */
public class MockHiveWarehouseConnector implements SessionConfigSupport {

  public static final int[] TEST_VALUES = {1, 2, 3, 4, 5};
  public static final Map<String, Object> WRITE_OUTPUT_BUFFER = new HashMap<>();
  public static final long COUNT_STAR_TEST_VALUE = 1024;

  private final StructType defaultTableSchema = StructType.fromDDL("a INT");

  /**
   * Returns the default schema for this mock connector.
   *
   * @param options options map
   * @return default schema
   */
  @Override
  public StructType inferSchema(CaseInsensitiveStringMap options) {
    return defaultTableSchema;
  }

  /**
   * Returns a mock table backed by the provided schema.
   *
   * @param schema table schema
   * @param partitions partition transforms
   * @param properties table properties
   * @return mock table instance
   */
  @Override
  public Table getTable(StructType schema, Transform[] partitions, Map<String, String> properties) {
    StructType resolvedSchema = schema != null ? schema : defaultTableSchema;
    return new MockTable(resolvedSchema);
  }

  /**
   * @return true to indicate metadata is supported
   */
  @Override
  public boolean supportsExternalMetadata() {
    return true;
  }

  /**
   * @return configuration key prefix
   */
  @Override
  public String keyPrefix() {
    return HiveWarehouseSession.CONF_PREFIX;
  }

  private static class MockTable implements Table, SupportsRead, SupportsWrite {
    private final StructType tableSchema;

    private MockTable(StructType tableSchema) {
      this.tableSchema = tableSchema;
    }

    @Override
    public String name() {
      return getClass().getName();
    }

    @Override
    public StructType schema() {
      return tableSchema;
    }

    @Override
    public Set<TableCapability> capabilities() {
      return new HashSet<>(Arrays.asList(TableCapability.BATCH_READ, TableCapability.BATCH_WRITE));
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
      return new MockScanBuilder(tableSchema);
    }

    @Override
    public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
      return new MockWriteSupport.MockWriteBuilder(info);
    }
  }

  private static class MockScanBuilder implements ScanBuilder {
    private final StructType tableSchema;

    private MockScanBuilder(StructType tableSchema) {
      this.tableSchema = tableSchema;
    }

    @Override
    public Scan build() {
      return new MockScan(tableSchema);
    }
  }

  private static class MockScan implements Scan, Batch {
    private final StructType tableSchema;

    private MockScan(StructType tableSchema) {
      this.tableSchema = tableSchema;
    }

    @Override
    public StructType readSchema() {
      return tableSchema;
    }

    @Override
    public Batch toBatch() {
      return this;
    }

    @Override
    public InputPartition[] planInputPartitions() {
      return new InputPartition[]{new MockInputPartition()};
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
      return new MockPartitionReaderFactory();
    }
  }

  private static class MockInputPartition implements InputPartition {
  }

  private static class MockPartitionReaderFactory implements PartitionReaderFactory {
    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
      return new MockPartitionReader();
    }
  }

  private static class MockPartitionReader implements PartitionReader<InternalRow> {
    private int currentIndex = 0;

    @Override
    public boolean next() {
      return currentIndex < TEST_VALUES.length;
    }

    @Override
    public InternalRow get() {
      int value = TEST_VALUES[currentIndex++];
      return new GenericInternalRow(new Object[]{value});
    }

    @Override
    public void close() {
    }
  }
}
