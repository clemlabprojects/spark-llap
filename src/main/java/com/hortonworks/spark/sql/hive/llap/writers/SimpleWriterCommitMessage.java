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

import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.connector.write.WriterCommitMessage;

/**
 * Simple commit message for HWC writers.
 */
public class SimpleWriterCommitMessage implements WriterCommitMessage {
  private final String message;
  private final Path writtenFilePath;
  private final long numRowsWritten;

  /**
   * Create a commit message without a row count.
   *
   * @param message commit message
   * @param writtenFilePath output file path
   */
  public SimpleWriterCommitMessage(String message, Path writtenFilePath) {
    this(message, writtenFilePath, 0L);
  }

  /**
   * Create a commit message.
   *
   * @param message commit message
   * @param writtenFilePath output file path
   * @param numRowsWritten number of rows written
   */
  public SimpleWriterCommitMessage(String message, Path writtenFilePath, long numRowsWritten) {
    this.message = message;
    this.writtenFilePath = writtenFilePath;
    this.numRowsWritten = numRowsWritten;
  }

  /**
   * @return commit message text
   */
  public String getMessage() {
    return message;
  }

  /**
   * @return output file path
   */
  public Path getWrittenFilePath() {
    return writtenFilePath;
  }

  /**
   * @return number of rows written
   */
  public long getNumRowsWritten() {
    return numRowsWritten;
  }
}
