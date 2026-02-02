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

import com.hortonworks.spark.sql.hive.llap.FilterPushdown$;
import com.hortonworks.spark.sql.hive.llap.common.StatementType;
import com.hortonworks.spark.sql.hive.llap.util.HiveQlUtil;
import com.hortonworks.spark.sql.hive.llap.util.SchemaUtil;
import java.util.Arrays;
import java.util.Map;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;
import scala.collection.Seq;

/**
 * Batch implementation for JDBC-backed reads.
 */
class JdbcDataSourceBatch implements Batch {
  private final StructType baseSchema;
  private final StructType prunedSchema;
  private final Filter[] filterArray;
  private final Map<String, String> optionMap;

  /**
   * Create a JDBC batch reader configuration.
   *
   * @param baseSchema full table schema
   * @param prunedSchema schema after column pruning
   * @param optionMap data source options
   * @param filterArray pushed-down filters
   */
  public JdbcDataSourceBatch(StructType baseSchema,
                             StructType prunedSchema,
                             Map<String, String> optionMap,
                             Filter[] filterArray) {
    this.baseSchema = baseSchema;
    this.prunedSchema = prunedSchema;
    this.optionMap = optionMap;
    this.filterArray = filterArray;
  }

  /**
   * Build the JDBC query string with projection and filter pushdown.
   *
   * @param projectionColumns projected column names
   * @param filterArray pushed-down filters
   * @return SQL query string for JDBC execution
   */
  private String getQueryString(String[] projectionColumns, Filter[] filterArray) {
    String projectionExpression = "1";
    if (projectionColumns.length > 0) {
      projectionExpression = HiveQlUtil.projections(projectionColumns);
    }
    String baseQuery = getQueryType() == StatementType.FULL_TABLE_SCAN
      ? HiveQlUtil.selectStar(optionMap.get("table"))
      : optionMap.get("query");
    Seq<Filter> scalaFilterSeq = JavaConverters.asScalaBuffer(Arrays.asList(filterArray)).seq();
    String whereClause = FilterPushdown$.MODULE$.buildWhereClause(baseSchema, scalaFilterSeq);
    // Use a random alias to prevent conflicts with user-provided SQL.
    return HiveQlUtil.selectProjectAliasFilter(projectionExpression, baseQuery,
      HiveQlUtil.randomAlias(), whereClause);
  }

  private StatementType getQueryType() {
    return StatementType.fromOptions(optionMap);
  }

  @Override
  /**
   * {@inheritDoc}
   */
  public InputPartition[] planInputPartitions() {
    try {
      String queryString = getQueryString(SchemaUtil.columnNames(prunedSchema), filterArray);
      return new InputPartition[]{new JdbcInputPartition(queryString, optionMap, prunedSchema)};
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  @Override
  /**
   * {@inheritDoc}
   */
  public PartitionReaderFactory createReaderFactory() {
    try {
      return new JdbcInputPartitionReaderFactory();
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }
}
