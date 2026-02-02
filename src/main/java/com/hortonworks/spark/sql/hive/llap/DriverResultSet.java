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

import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

/**
 * Simple result container for driver-side JDBC queries.
 */
public class DriverResultSet {
  private final List<Row> rows;
  private final StructType schema;

  /**
   * Creates a driver result set for the given data and schema.
   *
   * @param rows query result rows
   * @param schema result schema
   */
  public DriverResultSet(List<Row> rows, StructType schema) {
    this.rows = rows;
    this.schema = schema;
  }

  /**
   * Converts the stored rows to a Spark DataFrame.
   *
   * @param sparkSession active Spark session
   * @return DataFrame built from the stored rows and schema
   */
  public Dataset<Row> asDataFrame(SparkSession sparkSession) {
    return sparkSession.createDataFrame(rows, schema);
  }

  /**
   * Returns the stored rows.
   *
   * @return result rows
   */
  public List<Row> getData() {
    return rows;
  }

  /**
   * Returns the stored rows.
   *
   * @return result rows
   */
  public List<Row> getRows() {
    return rows;
  }

  /**
   * Returns the schema of the result.
   *
   * @return result schema
   */
  public StructType getSchema() {
    return schema;
  }
}
