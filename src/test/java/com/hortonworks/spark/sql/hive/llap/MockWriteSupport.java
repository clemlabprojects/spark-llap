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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.metric.CustomMetric;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.SupportsTruncate;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;

/**
 * Mock write support used by tests to validate DataSource write paths.
 */
public class MockWriteSupport {

  /**
   * Mock write builder implementation.
   */
  public static class MockWriteBuilder implements WriteBuilder, SupportsTruncate {
    private final LogicalWriteInfo logicalWriteInfo;

    /**
     * Creates a builder for the provided logical write info.
     *
     * @param logicalWriteInfo logical write info
     */
    public MockWriteBuilder(LogicalWriteInfo logicalWriteInfo) {
      this.logicalWriteInfo = logicalWriteInfo;
    }

    @Override
    public WriteBuilder truncate() {
      return this;
    }

    @Override
    public Write build() {
      return new MockWrite(logicalWriteInfo);
    }

    @Override
    public BatchWrite buildForBatch() {
      return build().toBatch();
    }

    @Override
    public StreamingWrite buildForStreaming() {
      throw new UnsupportedOperationException("Streaming write is not supported in tests.");
    }
  }

  private static class MockWrite implements Write {
    @SuppressWarnings("unused")
    private final LogicalWriteInfo logicalWriteInfo;

    private MockWrite(LogicalWriteInfo logicalWriteInfo) {
      this.logicalWriteInfo = logicalWriteInfo;
    }

    @Override
    public String description() {
      return "MockWrite";
    }

    @Override
    public BatchWrite toBatch() {
      return new MockBatchWrite();
    }

    @Override
    public StreamingWrite toStreaming() {
      throw new UnsupportedOperationException("Streaming write is not supported in tests.");
    }

    @Override
    public CustomMetric[] supportedCustomMetrics() {
      return new CustomMetric[0];
    }
  }

  private static class MockBatchWrite implements BatchWrite {
    @Override
    public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo physicalWriteInfo) {
      return new MockDataWriterFactory();
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {
      List<InternalRow> writtenRows = new ArrayList<>();
      for (WriterCommitMessage message : messages) {
        if (message instanceof MockWriterCommitMessage) {
          writtenRows.addAll(((MockWriterCommitMessage) message).rows);
        }
      }
      MockHiveWarehouseConnector.WRITE_OUTPUT_BUFFER.put("TestWriteSupport", writtenRows);
    }

    @Override
    public void abort(WriterCommitMessage[] messages) {
      MockHiveWarehouseConnector.WRITE_OUTPUT_BUFFER.remove("TestWriteSupport");
    }
  }

  private static class MockDataWriterFactory implements DataWriterFactory {
    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
      return new MockDataWriter();
    }
  }

  private static class MockDataWriter implements DataWriter<InternalRow> {
    private final List<InternalRow> rowBuffer = new ArrayList<>();

    @Override
    public void write(InternalRow record) throws IOException {
      rowBuffer.add(record.copy());
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
      return new MockWriterCommitMessage(rowBuffer);
    }

    @Override
    public void abort() throws IOException {
      rowBuffer.clear();
    }

    @Override
    public void close() throws IOException {
    }
  }

  private static class MockWriterCommitMessage implements WriterCommitMessage {
    private final List<InternalRow> rows;

    private MockWriterCommitMessage(List<InternalRow> rows) {
      this.rows = rows;
    }
  }
}
