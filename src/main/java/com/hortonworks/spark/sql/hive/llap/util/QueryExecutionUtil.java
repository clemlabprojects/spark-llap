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
import java.util.Map;
import org.antlr.runtime.tree.CommonTree;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
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
    ExecutionMethod resolvedMethod = defaultMethod;
    if (enableSmartExecution) {
      ASTNode parseTree;
      try {
        HiveConf hiveConf = new HiveConf();
        hiveConf.set("_hive.hdfs.session.path", "/tmp/some_dummy_path");
        hiveConf.set("_hive.local.session.path", "/tmp/some_dummy_path");
        Context parseContext = new Context((Configuration) hiveConf);
        parseTree = ParseUtils.parse(sql, parseContext);
      } catch (ParseException parseException) {
        throw new RuntimeException(parseException.getMessage(), parseException);
      }
      // Token type 1111 corresponds to select-query AST in Hive parser.
      resolvedMethod = 1111 == parseTree.getType()
          ? ExecutionMethod.EXECUTE_JDBC_CLUSTER
          : ExecutionMethod.EXECUTE_HIVE_JDBC;
    }
    return resolvedMethod;
  }

  /**
   * Determines whether the SQL is a data-fetching query (SELECT with FROM).
   *
   * @param sql SQL text to analyze
   * @return {@code true} when the SQL is a SELECT/FROM query
   */
  public static boolean isDataFetchQuery(String sql) {
    ASTNode parseTree;
    try {
      try {
        parseTree = ParseUtils.parse(sql, null);
      } catch (NullPointerException nullPointerException) {
        // Hive parser requires a session context for some environments.
        HiveConf hiveConf = new HiveConf();
        hiveConf.set("_hive.hdfs.session.path", "/tmp/some_dummy_path");
        hiveConf.set("_hive.local.session.path", "/tmp/some_dummy_path");
        Context parseContext = new Context((Configuration) hiveConf);
        parseTree = ParseUtils.parse(sql, parseContext);
      }
    } catch (ParseException parseException) {
      throw new RuntimeException(parseException.getMessage(), parseException);
    }
    if (parseTree.getChildCount() == 0) {
      return false;
    }
    return parseTree.getText().equalsIgnoreCase("TOK_QUERY")
        && parseTree.getChildren().stream()
            .map(node -> (ASTNode) node)
            .map(CommonTree::getToken)
            .anyMatch(token -> token.getText().equalsIgnoreCase("TOK_FROM"));
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
