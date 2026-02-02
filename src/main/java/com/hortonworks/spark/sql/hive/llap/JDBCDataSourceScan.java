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

import com.hortonworks.hwc.plan.HwcPlannerStatistics;
import com.hortonworks.spark.sql.hive.llap.HWConf;
import com.hortonworks.spark.sql.hive.llap.common.StatementType;
import com.hortonworks.spark.sql.hive.llap.util.JobUtil;
import com.hortonworks.spark.sql.hive.llap.util.QueryExecutionUtil;
import com.hortonworks.spark.sql.hive.llap.util.SchemaUtil;
import java.sql.Connection;
import java.util.Map;
import java.util.OptionalLong;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.Statistics;
import org.apache.spark.sql.connector.read.SupportsReportStatistics;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Scan implementation for the JDBC-based HWC data source.
 */
class JDBCDataSourceScan implements SupportsReportStatistics {
  private static final Logger LOG = LoggerFactory.getLogger(JDBCDataSourceScan.class);
  private final StructType baseSchema;
  private final StructType prunedSchema;
  private final Map<String, String> optionMap;
  private final Filter[] filterArray;

  /**
   * Create a scan for the JDBC data source.
   *
   * @param baseSchema full table schema
   * @param prunedSchema schema after column pruning
   * @param optionMap data source options
   * @param filterArray pushed-down filters
   */
  JDBCDataSourceScan(StructType baseSchema,
                     StructType prunedSchema,
                     Map<String, String> optionMap,
                     Filter[] filterArray) {
    this.baseSchema = baseSchema;
    this.prunedSchema = prunedSchema;
    this.optionMap = optionMap;
    this.filterArray = filterArray;
  }

  @Override
  /**
   * {@inheritDoc}
   */
  public StructType readSchema() {
    return prunedSchema;
  }

  @Override
  /**
   * {@inheritDoc}
   */
  public Batch toBatch() {
    return new JdbcDataSourceBatch(baseSchema, prunedSchema, optionMap, filterArray);
  }

  private StatementType getQueryType() {
    return StatementType.fromOptions(optionMap);
  }

  @Override
  /**
   * {@inheritDoc}
   */
  public Statistics estimateStatistics() {
    SQLConf sqlConf = SparkSession.getActiveSession().get().sqlContext().conf();
    HwcPlannerStatistics plannerStatistics =
      new HwcPlannerStatistics(OptionalLong.of(sqlConf.defaultSizeInBytes()), OptionalLong.empty());
    try (Connection connection = QueryExecutionUtil.getConnection(optionMap)) {
      if (StatementType.FULL_TABLE_SCAN.equals(getQueryType())) {
        String databaseName = HWConf.DEFAULT_DB.getFromOptionsMap(optionMap);
        SchemaUtil.TableRef tableRef =
          SchemaUtil.getDbTableNames(databaseName, optionMap.get("table"));
        LOG.debug("Found full table scan for table {}.{}, estimating stats", tableRef.databaseName,
          tableRef.tableName);
        plannerStatistics =
          JobUtil.getStatisticsForTable(tableRef.databaseName, tableRef.tableName, connection);
        LOG.debug("Estimated stats {} for table {}.{}",
          plannerStatistics, tableRef.databaseName, tableRef.tableName);
      }
    } catch (Exception e) {
      LOG.error("Unable to get table properties, setting to defaults " + plannerStatistics +
        " Error message - " + e.getMessage(), e);
    }
    return plannerStatistics;
  }
}
