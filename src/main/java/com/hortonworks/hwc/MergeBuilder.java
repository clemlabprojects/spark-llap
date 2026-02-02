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

package com.hortonworks.hwc;

/**
 * Builder for assembling a Hive MERGE statement in a fluent way.
 */
public interface MergeBuilder {
  /**
   * Starts a MERGE statement by declaring the target table and its alias.
   *
   * @param targetTable the fully qualified target table name (for example, {@code db.table})
   * @param alias alias used to reference the target table in the merge statement
   * @return this builder for fluent chaining
   */
  MergeBuilder mergeInto(String targetTable, String alias);

  /**
   * Specifies the source table or subquery and its alias.
   *
   * @param sourceTableOrQuery table name or a subquery string
   * @param alias alias used to reference the source in the merge statement
   * @return this builder for fluent chaining
   */
  MergeBuilder using(String sourceTableOrQuery, String alias);

  /**
   * Adds the join predicate between source and target.
   *
   * @param joinExpression join predicate expression
   * @return this builder for fluent chaining
   */
  MergeBuilder on(String joinExpression);

  /**
   * Adds a WHEN MATCHED THEN UPDATE clause.
   *
   * @param condition optional predicate for the matched branch; pass {@code null} or empty for no
   *                  additional predicate
   * @param assignments column assignments in Hive syntax (for example, {@code "t.col = s.col"})
   * @return this builder for fluent chaining
   */
  MergeBuilder whenMatchedThenUpdate(String condition, String... assignments);

  /**
   * Adds a WHEN MATCHED THEN DELETE clause.
   *
   * @param condition optional predicate for the matched delete branch; pass {@code null} or empty
   *                  for no additional predicate
   * @return this builder for fluent chaining
   */
  MergeBuilder whenMatchedThenDelete(String condition);

  /**
   * Adds a WHEN NOT MATCHED THEN INSERT clause.
   *
   * @param condition optional predicate for the not-matched branch; pass {@code null} or empty for
   *                  no additional predicate
   * @param assignments column assignments in Hive syntax (for example, {@code "col = s.col"})
   * @return this builder for fluent chaining
   */
  MergeBuilder whenNotMatchedInsert(String condition, String... assignments);

  /**
   * Executes the assembled MERGE statement.
   */
  void merge();
}
