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

import com.hortonworks.spark.sql.hive.llap.DefaultJDBCWrapper;
import com.hortonworks.spark.sql.hive.llap.HWConf;
import java.sql.Connection;
import java.util.Locale;
import java.util.Map;
import scala.Option;

/**
 * Utility helpers for parsing queries and creating JDBC connections for HWC.
 */
public final class QueryExecutionUtil {
  public static final String HIVE_JDBC_URL_FOR_EXECUTOR = "hs2.jdbc.url.for.executor";

  /**
   * Chooses the execution method based on the SQL text and a smart-execution flag.
   *
   * @param enableSmartExecution whether to inspect the SQL before deciding
   * @param defaultMethod method to use when smart execution is disabled
   * @param sql SQL text to parse
   * @return chosen execution method
   */
  public static ExecutionMethod resolveExecutionMethod(boolean enableSmartExecution,
                                                       ExecutionMethod defaultMethod,
                                                       String sql) {
    if (!enableSmartExecution) {
      return defaultMethod;
    }
    return isDataFetchQuery(sql)
        ? ExecutionMethod.EXECUTE_JDBC_CLUSTER
        : ExecutionMethod.EXECUTE_HIVE_JDBC;
  }

  /**
   * Determines whether the SQL is a data-fetching query (SELECT with FROM).
   *
   * @param sql SQL text to analyze
   * @return {@code true} when the SQL is a SELECT/FROM query
   */
  public static boolean isDataFetchQuery(String sql) {
    String normalizedSql = stripLeadingComments(sql).trim().toLowerCase(Locale.ROOT);
    if (normalizedSql.isEmpty()) {
      return false;
    }

    if (startsWithKeyword(normalizedSql, "select")) {
      return containsKeyword(normalizedSql, "from");
    }

    if (startsWithKeyword(normalizedSql, "with")) {
      int selectIndex = indexOfKeyword(normalizedSql, "select", 0);
      if (selectIndex < 0) {
        return false;
      }
      return indexOfKeyword(normalizedSql, "from", selectIndex) >= 0;
    }

    return false;
  }

  private static String stripLeadingComments(String sql) {
    if (sql == null) {
      return "";
    }

    int index = 0;
    while (index < sql.length()) {
      while (index < sql.length() && Character.isWhitespace(sql.charAt(index))) {
        index++;
      }

      if (index + 1 < sql.length() && sql.charAt(index) == '-' && sql.charAt(index + 1) == '-') {
        index += 2;
        while (index < sql.length() && sql.charAt(index) != '\n' && sql.charAt(index) != '\r') {
          index++;
        }
        continue;
      }

      if (index + 1 < sql.length() && sql.charAt(index) == '/' && sql.charAt(index + 1) == '*') {
        int commentEnd = sql.indexOf("*/", index + 2);
        if (commentEnd < 0) {
          return "";
        }
        index = commentEnd + 2;
        continue;
      }

      break;
    }
    return sql.substring(index);
  }

  private static boolean startsWithKeyword(String sql, String keyword) {
    return indexOfKeyword(sql, keyword, 0) == 0;
  }

  private static boolean containsKeyword(String sql, String keyword) {
    return indexOfKeyword(sql, keyword, 0) >= 0;
  }

  private static int indexOfKeyword(String sql, String keyword, int fromIndex) {
    int index = Math.max(0, fromIndex);
    while (index < sql.length()) {
      int candidate = sql.indexOf(keyword, index);
      if (candidate < 0) {
        return -1;
      }

      boolean startBoundary = candidate == 0 || !Character.isLetterOrDigit(sql.charAt(candidate - 1));
      int endIndex = candidate + keyword.length();
      boolean endBoundary = endIndex >= sql.length() || !Character.isLetterOrDigit(sql.charAt(endIndex));
      if (startBoundary && endBoundary) {
        return candidate;
      }
      index = candidate + keyword.length();
    }
    return -1;
  }

  /**
   * Creates a JDBC connection based on HWC options.
   *
   * @param options HWC options map
   * @return JDBC connection
   */
  public static Connection getConnection(Map<String, String> options) {
    String jdbcUrl = HWConf.RESOLVED_HS2_URL.getFromOptionsMap(options);
    String userName = HWConf.USER.getFromOptionsMap(options);
    String userPassword = HWConf.PASSWORD.getFromOptionsMap(options);
    String dbcp2Config = HWConf.DBCP2_CONF.getFromOptionsMap(options);
    return DefaultJDBCWrapper.getConnector(Option.empty(), jdbcUrl, userName, userPassword, dbcp2Config);
  }

  /**
   * Creates a JDBC connection from explicit parameters.
   *
   * @param jdbcUrl HS2 JDBC URL
   * @param userName user name
   * @param userPassword user password
   * @param dbcp2Config DBCP2 configuration string
   * @return JDBC connection
   */
  public static Connection getConnection(String jdbcUrl,
                                         String userName,
                                         String userPassword,
                                         String dbcp2Config) {
    return DefaultJDBCWrapper.getConnector(Option.empty(), jdbcUrl, userName, userPassword, dbcp2Config);
  }

  /**
   * Supported execution paths for HWC statements.
   */
  public enum ExecutionMethod {
    EXECUTE_HIVE_JDBC,
    EXECUTE_JDBC_CLUSTER
  }
}
