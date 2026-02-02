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

package com.hortonworks.hwc.plan;

import java.util.OptionalLong;
import org.apache.spark.sql.connector.read.Statistics;

/**
 * Spark {@link Statistics} implementation for HWC query planning.
 */
public class HwcPlannerStatistics implements Statistics {
  private OptionalLong sizeInBytes;
  private OptionalLong numRows;

  /**
   * Creates an empty statistics container.
   */
  public HwcPlannerStatistics() {
  }

  /**
   * Creates a statistics container populated with estimated values.
   *
   * @param sizeInBytes estimated size in bytes
   * @param numRows estimated number of rows
   */
  public HwcPlannerStatistics(OptionalLong sizeInBytes, OptionalLong numRows) {
    this.sizeInBytes = sizeInBytes;
    this.numRows = numRows;
  }

  /**
   * Returns the current size estimate.
   *
   * @return size estimate in bytes, if available
   */
  public OptionalLong getSizeInBytes() {
    return sizeInBytes;
  }

  /**
   * Sets the current size estimate.
   *
   * @param sizeInBytes size estimate in bytes, or empty if unknown
   */
  public void setSizeInBytes(OptionalLong sizeInBytes) {
    this.sizeInBytes = sizeInBytes;
  }

  /**
   * Returns the current row-count estimate.
   *
   * @return row-count estimate, if available
   */
  public OptionalLong getNumRows() {
    return numRows;
  }

  /**
   * Sets the current row-count estimate.
   *
   * @param numRows row-count estimate, or empty if unknown
   */
  public void setNumRows(OptionalLong numRows) {
    this.numRows = numRows;
  }

  @Override
  public OptionalLong sizeInBytes() {
    // Spark planner uses this value for cost-based decisions.
    return sizeInBytes;
  }

  @Override
  public OptionalLong numRows() {
    // Row-count estimate used by the Spark optimizer.
    return numRows;
  }

  @Override
  public String toString() {
    return "HwcPlannerStatistics{sizeInBytes=" + sizeInBytes + ", numRows=" + numRows + '}';
  }
}
