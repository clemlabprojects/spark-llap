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

import java.util.Map;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriteBuilder;

/**
 * Builds streaming write support for the HWC data source.
 */
class HiveStreamingDataSourceWriterBuilder implements WriteBuilder {
  private final LogicalWriteInfo logicalWriteInfo;

  /**
   * Creates a builder from Spark's logical write info.
   *
   * @param logicalWriteInfo Spark write metadata
   */
  public HiveStreamingDataSourceWriterBuilder(LogicalWriteInfo logicalWriteInfo) {
    this.logicalWriteInfo = logicalWriteInfo;
  }

  /**
   * Builds a {@link Write} implementation for streaming output.
   *
   * @return write implementation
   */
  @Override
  public Write build() {
    // Convert options to a regular map expected by the streaming writer.
    @SuppressWarnings("unchecked")
    Map<String, String> optionMap = (Map<String, String>) this.logicalWriteInfo.options();
    return new HiveStreamingWriter(
        this.logicalWriteInfo.queryId(),
        this.logicalWriteInfo.schema(),
        optionMap);
  }
}
