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

package com.hortonworks.spark.sql.hive.llap.util;

import java.io.Serializable;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reorders Spark {@link InternalRow} fields to match Hive column ordering.
 */
public class SparkToHiveRecordMapper implements Serializable {
  private static final long serialVersionUID = -5213255591548912610L;
  private static final Logger LOG = LoggerFactory.getLogger(SparkToHiveRecordMapper.class);
  private StructType schemaInHiveColumnOrder;
  private int[] columnIndexMapping;
  private boolean isRecordReorderingNeeded = false;
  private StructType sparkDataFrameSchema;

  /**
   * Builds a mapper based on Spark schema and Hive column order.
   *
   * @param sparkDataFrameSchema Spark DataFrame schema
   * @param hiveColumns Hive column names in target order
   */
  public SparkToHiveRecordMapper(StructType sparkDataFrameSchema, String[] hiveColumns) {
    this.sparkDataFrameSchema = sparkDataFrameSchema;
    this.schemaInHiveColumnOrder = sparkDataFrameSchema;
    resolveBasedOnColumnNames(hiveColumns);
  }

  /**
   * Creates an empty mapper. Callers must initialize it before use.
   */
  public SparkToHiveRecordMapper() {
  }

  /**
   * Returns the schema reordered to Hive column order.
   *
   * @return schema aligned with Hive column order
   */
  public StructType getSchemaInHiveColumnsOrder() {
    return schemaInHiveColumnOrder;
  }

  /**
   * Reorders the provided row to match Hive column order.
   *
   * @param inputRow Spark row
   * @return reordered row (or the original row if no reordering is needed)
   */
  public InternalRow mapToHiveColumns(InternalRow inputRow) {
    if (isRecordReorderingNeeded) {
      int fieldCount = inputRow.numFields();
      Object[] reorderedValues = new Object[fieldCount];
      for (int i = 0; i < fieldCount; ++i) {
        reorderedValues[columnIndexMapping[i]] =
            inputRow.get(i, sparkDataFrameSchema.fields()[i].dataType());
      }
      inputRow = new GenericInternalRow(reorderedValues);
    }
    return inputRow;
  }

  private void resolveBasedOnColumnNames(String[] hiveColumns) {
    String[] sparkColumns = sparkDataFrameSchema.fieldNames();
    if (hiveColumns != null && hiveColumns.length == sparkColumns.length) {
      Map<String, Integer> indexByName = IntStream.range(0, hiveColumns.length).boxed()
          .collect(Collectors.toMap(index -> hiveColumns[index], Function.identity()));
      columnIndexMapping = new int[hiveColumns.length];
      for (int i = 0; i < sparkColumns.length; ++i) {
        Integer targetIndex = indexByName.get(sparkColumns[i]);
        if (targetIndex == null) {
          isRecordReorderingNeeded = false;
          break;
        }
        columnIndexMapping[i] = targetIndex;
        if (i != targetIndex) {
          isRecordReorderingNeeded = true;
        }
      }
      if (isRecordReorderingNeeded) {
        schemaInHiveColumnOrder = new StructType();
        for (String columnName : hiveColumns) {
          schemaInHiveColumnOrder =
              schemaInHiveColumnOrder.add(
                  sparkDataFrameSchema.fields()[sparkDataFrameSchema.fieldIndex(columnName)]);
        }
      }
    }
    LOG.info("Need to reorder/rearrange DF columns to match hive columns order: {}",
        isRecordReorderingNeeded);
  }
}
