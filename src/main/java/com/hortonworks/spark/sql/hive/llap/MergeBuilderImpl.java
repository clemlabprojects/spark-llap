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

import com.hortonworks.hwc.MergeBuilder;
import org.apache.commons.lang3.StringUtils;

/**
 * Implementation of the HWC {@link MergeBuilder} interface.
 */
public class MergeBuilderImpl implements MergeBuilder {
  private final HiveWarehouseSession hiveWarehouseSession;
  private final String databaseName;
  private String targetTableName;
  private String targetAlias;
  private String sourceTableOrExpression;
  private String sourceAlias;
  private String joinExpression;
  private String updatePredicate;
  private String[] updateAssignments;
  private boolean deleteRequired;
  private String deletePredicate;
  private String[] insertValues;
  private String insertPredicate;

  /**
   * Creates a MERGE builder for a given database.
   *
   * @param hiveWarehouseSession active HWC session
   * @param databaseName target database name
   */
  public MergeBuilderImpl(HiveWarehouseSession hiveWarehouseSession, String databaseName) {
    this.hiveWarehouseSession = hiveWarehouseSession;
    this.databaseName = databaseName;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MergeBuilder mergeInto(String targetTable, String alias) {
    this.targetTableName = targetTable;
    this.targetAlias = alias;
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MergeBuilder using(String sourceTableOrQuery, String alias) {
    this.sourceTableOrExpression = sourceTableOrQuery;
    this.sourceAlias = alias;
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MergeBuilder on(String joinExpression) {
    this.joinExpression = joinExpression;
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MergeBuilder whenMatchedThenUpdate(String condition, String... assignments) {
    this.updatePredicate = condition;
    this.updateAssignments = assignments;
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MergeBuilder whenMatchedThenDelete(String condition) {
    this.deleteRequired = true;
    this.deletePredicate = condition;
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public MergeBuilder whenNotMatchedInsert(String condition, String... assignments) {
    this.insertPredicate = condition;
    this.insertValues = assignments;
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void merge() {
    // Execute the MERGE statement as a single update.
    this.hiveWarehouseSession.executeUpdate(this.toString(), true);
  }

  @Override
  public String toString() {
    StringBuilder queryBuilder = new StringBuilder();
    queryBuilder.append(
        String.format(
            "MERGE INTO %s.%s AS %s USING %s AS %s ON %s",
            this.databaseName,
            this.targetTableName,
            this.targetAlias,
            this.sourceTableOrExpression,
            this.sourceAlias,
            this.joinExpression));

    if (this.updateAssignments != null && this.updateAssignments.length > 0) {
      queryBuilder.append(buildWhenMatchedPredicate(false, this.updatePredicate));
      queryBuilder.append(" THEN UPDATE SET ").append(String.join(",", this.updateAssignments));
    }

    if (this.deleteRequired) {
      queryBuilder.append(buildWhenMatchedPredicate(false, this.deletePredicate));
      queryBuilder.append(" THEN DELETE");
    }

    if (this.insertValues != null && this.insertValues.length > 0) {
      queryBuilder.append(buildWhenMatchedPredicate(true, this.insertPredicate));
      queryBuilder.append(" THEN INSERT VALUES (")
          .append(String.join(",", this.insertValues))
          .append(")");
    }

    return queryBuilder.toString();
  }

  private String buildWhenMatchedPredicate(boolean notMatchedClause, String condition) {
    String clause = notMatchedClause ? " WHEN NOT MATCHED " : " WHEN MATCHED ";
    if (StringUtils.isNotBlank(condition)) {
      clause = clause + "AND (" + condition + ") ";
    }
    return clause;
  }
}
