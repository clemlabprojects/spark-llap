/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hortonworks.spark.sql.hive.llap;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.catalog.SessionConfigSupport;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.unsafe.types.UTF8String;

public class SimpleMockConnector implements Table, SupportsRead, SessionConfigSupport {

    public static class SimpleMockDataReader {
        // Exposed for Python side.
        public static final int RESULT_SIZE = 10;
    }

    private StructType schema = (new StructType())
            .add("col1", "int")
            .add("col2", "string");

    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options) {
        return schema;
    }

    @Override
    public Table getTable(StructType schema, Transform[] partitions, Map<String, String> properties) {
        if (schema != null) {
            this.schema = schema;
        }
        return this;
    }

    @Override
    public String keyPrefix() {
        return HiveWarehouseSession.CONF_PREFIX;
    }

    @Override
    public String name() {
        return getClass().getName();
    }

    @Override
    public StructType schema() {
        return schema;
    }

    @Override
    public Set<TableCapability> capabilities() {
        return Collections.singleton(TableCapability.BATCH_READ);
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        return new SimpleScanBuilder(schema);
    }

    private static class SimpleScanBuilder implements ScanBuilder {
        private final StructType schema;

        private SimpleScanBuilder(StructType schema) {
            this.schema = schema;
        }

        @Override
        public Scan build() {
            return new SimpleScan(schema);
        }
    }

    private static class SimpleScan implements Scan, Batch {
        private final StructType schema;

        private SimpleScan(StructType schema) {
            this.schema = schema;
        }

        @Override
        public StructType readSchema() {
            return schema;
        }

        @Override
        public Batch toBatch() {
            return this;
        }

        @Override
        public InputPartition[] planInputPartitions() {
            return new InputPartition[] {new SimpleInputPartition()};
        }

        @Override
        public PartitionReaderFactory createReaderFactory() {
            return new SimplePartitionReaderFactory();
        }
    }

    private static class SimpleInputPartition implements InputPartition {
    }

    private static class SimplePartitionReaderFactory implements PartitionReaderFactory {
        @Override
        public PartitionReader<InternalRow> createReader(InputPartition partition) {
            return new SimplePartitionReader();
        }
    }

    private static class SimplePartitionReader implements PartitionReader<InternalRow> {
        private int index = 0;

        @Override
        public boolean next() {
            return index < SimpleMockDataReader.RESULT_SIZE;
        }

        @Override
        public InternalRow get() {
            int current = index++;
            Object[] values = new Object[] {current, UTF8String.fromString("Element " + current)};
            return new GenericInternalRow(values);
        }

        @Override
        public void close() {
        }
    }
}
