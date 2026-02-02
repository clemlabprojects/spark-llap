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

import com.google.common.base.Preconditions;
import com.hortonworks.hwc.MergeBuilder;
import com.hortonworks.spark.sql.hive.llap.common.DriverResultSet;
import com.hortonworks.spark.sql.hive.llap.query.builder.CreateTableBuilder;
import com.hortonworks.spark.sql.hive.llap.query.builder.MergeBuilderImpl;
import com.hortonworks.spark.sql.hive.llap.util.FunctionWith4Args;
import com.hortonworks.spark.sql.hive.llap.util.HWCOptions;
import com.hortonworks.spark.sql.hive.llap.util.HiveQlUtil;
import com.hortonworks.spark.sql.hive.llap.util.QueryExecutionUtil;
import com.hortonworks.spark.sql.hive.llap.util.StreamingMetaCleaner;
import com.hortonworks.spark.sql.hive.llap.util.TriFunction;
import com.hortonworks.spark.sql.hive.llap.writers.HiveWarehouseDataWriterHelper;
import java.io.IOException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RuntimeConfig;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default Hive Warehouse Connector session implementation.
 */
public class HiveWarehouseSessionImpl implements com.hortonworks.hwc.HiveWarehouseSession {
  private static final Logger LOG = LoggerFactory.getLogger(HiveWarehouseSessionImpl.class);
  private static final String SELECT_STATEMENT_REGEX = "(\\s+)?select(\\s+).+";
  private static final Pattern SELECT_STATEMENT_PATTERN =
    Pattern.compile(SELECT_STATEMENT_REGEX, Pattern.CASE_INSENSITIVE);

  public static final String SESSION_CLOSED_MSG =
    "Session is closed!!! No more operations can be performed. Please create a new HiveWarehouseSession";
  public static final String HWC_SESSION_ID_KEY = "hwc_session_id";
  public static final String HWC_QUERY_MODE = "hwc_query_mode";

  static String HIVE_WAREHOUSE_CONNECTOR_INTERNAL = HiveWarehouseSession.HIVE_WAREHOUSE_CONNECTOR;

  protected HiveWarehouseSessionState sessionState;
  protected Supplier<Connection> connectionSupplier;
  protected TriFunction<Connection, String, String, DriverResultSet> statementExecutor;
  protected TriFunction<Connection, String, String, Boolean> updateExecutor;
  protected FunctionWith4Args<Connection, String, String, Boolean, Boolean> updateExecutorWithExceptionPropagation;
  private final AtomicReference<HwcSessionState> hwcSessionStateReference;

  /**
   * Creates a new session implementation.
   *
   * @param sessionState hive warehouse session state
   */
  public HiveWarehouseSessionImpl(HiveWarehouseSessionState sessionState) {
    this.sessionState = sessionState;
    this.connectionSupplier = () -> DefaultJDBCWrapper.getConnector(sessionState);
    this.statementExecutor = (connection, databaseName, sqlStatement) ->
      DefaultJDBCWrapper.executeStmt(connection, databaseName, sqlStatement, 0L);
    this.updateExecutor = (connection, databaseName, sqlStatement) ->
      DefaultJDBCWrapper.executeUpdate(connection, databaseName, sqlStatement);
    this.updateExecutorWithExceptionPropagation = DefaultJDBCWrapper::executeUpdate;
    this.hwcSessionStateReference = new AtomicReference<>(HwcSessionState.OPEN);
    LOG.info("Created a new HWC session: {}", sessionState.getSessionId());
  }

  private boolean usingDirectReaderMode() {
    return HWConf.HWCReadMode.resolveReadMode(HWConf.HWCReadMode.DIRECT_READER_V1)
      || HWConf.HWCReadMode.resolveReadMode(HWConf.HWCReadMode.DIRECT_READER_V2)
      || HWConf.getHwcExecutionMode().equalsIgnoreCase("spark");
  }

  /**
   * Ensures the session has not been closed.
   */
  private void ensureSessionOpen() {
    Preconditions.checkState(HwcSessionState.OPEN.equals(hwcSessionStateReference.get()), SESSION_CLOSED_MSG);
  }

  /**
   * Executes the SQL query using the configured execution mode.
   *
   * @param sqlStatement SQL statement to execute
   * @return query results
   */
  @Override
  public Dataset<Row> sql(String sqlStatement) {
    if (usingDirectReaderMode()) {
      return executeQueryInternal(sqlStatement, null);
    }
    if (HWConf.HWCReadMode.resolveReadMode(HWConf.HWCReadMode.JDBC_CLIENT)) {
      return executeSimpleJdbc(sqlStatement);
    }
    if (HWConf.HWCReadMode.resolveReadMode(HWConf.HWCReadMode.SECURE_ACCESS)
      && QueryExecutionUtil.isDataFetchQuery(sqlStatement)) {
      return executeQuerySecureAccess(sqlStatement);
    }
    return executeJdbcInternal(sqlStatement);
  }

  /**
   * Executes a query via JDBC cluster execution.
   *
   * @param sqlStatement SQL statement to execute
   * @return query results
   */
  @Override
  public Dataset<Row> executeQuery(String sqlStatement) {
    return executeSmart(QueryExecutionUtil.ExecutionMethod.EXECUTE_JDBC_CLUSTER, sqlStatement, null);
  }

  /**
   * Executes a query via JDBC cluster execution with optional Spark execution.
   *
   * @param sqlStatement SQL statement to execute
   * @param isSparkExecution whether Spark execution is requested (unused)
   * @return query results
   */
  @Override
  public Dataset<Row> executeQuery(String sqlStatement, boolean isSparkExecution) {
    return executeSmart(QueryExecutionUtil.ExecutionMethod.EXECUTE_JDBC_CLUSTER, sqlStatement, getCoresInSparkCluster());
  }

  /**
   * Executes a query with a specific split count.
   *
   * @param sqlStatement SQL statement to execute
   * @param splitCount number of splits to use
   * @return query results
   */
  @Override
  public Dataset<Row> executeQuery(String sqlStatement, int splitCount) {
    return executeSmart(QueryExecutionUtil.ExecutionMethod.EXECUTE_JDBC_CLUSTER, sqlStatement, splitCount);
  }

  private Dataset<Row> executeQueryInternal(String sqlStatement, Integer splitCount) {
    Preconditions.checkNotNull(sqlStatement, "The query cannot be null");
    if (QueryExecutionUtil.isDataFetchQuery(sqlStatement)) {
      ensureSessionOpen();
      if (HWConf.HWCReadMode.resolveReadMode(HWConf.HWCReadMode.SECURE_ACCESS)) {
        return executeQuerySecureAccess(sqlStatement);
      }
      if (usingDirectReaderMode()) {
        if (!HWConf.getSparkSqlExtension(sessionState)
          .contains("com.qubole.spark.hiveacid.HiveAcidAutoConvertExtension")) {
          LOG.info("For spark execution, set spark.sql.extensions to " +
            "com.qubole.spark.hiveacid.HiveAcidAutoConvertExtension");
          LOG.warn("com.qubole.spark.hiveacid.HiveAcidAutoConvertExtension will be replaced by " +
            "com.hortonworks.spark.sql.rule.Extensions. Please switch to " +
            "com.hortonworks.spark.sql.rule.Extensions.");
        }
        return session().sql(sqlStatement);
      }
      return executeJdbcInternal(sqlStatement);
    }
    return executeSimpleJdbc(sqlStatement);
  }

  private String setSplitPropertiesQuery(int splitCount) {
    return String.format("SET tez.grouping.split-count=%s", splitCount);
  }

  private int getCoresInSparkCluster() {
    return session().sparkContext().defaultParallelism();
  }

  /**
   * Executes a query using the Hive JDBC execution method.
   *
   * @param sqlStatement SQL statement to execute
   * @return query results
   */
  @Override
  public Dataset<Row> execute(String sqlStatement) {
    return executeSmart(QueryExecutionUtil.ExecutionMethod.EXECUTE_HIVE_JDBC, sqlStatement, null);
  }

  private Dataset<Row> executeSmart(QueryExecutionUtil.ExecutionMethod executionMethod,
                                    String sqlStatement,
                                    Integer splitCount) {
    QueryExecutionUtil.ExecutionMethod resolvedExecutionMethod =
      QueryExecutionUtil.resolveExecutionMethod(BooleanUtils.toBoolean(
        HWConf.SMART_EXECUTION.getString(sessionState)), executionMethod, sqlStatement);
    if (QueryExecutionUtil.ExecutionMethod.EXECUTE_HIVE_JDBC.equals(resolvedExecutionMethod)) {
      return executeSimpleJdbc(sqlStatement);
    }
    return executeQueryInternal(sqlStatement, splitCount);
  }

  /**
   * Returns the JDBC URL to be used by executors.
   *
   * @return JDBC URL for executor access
   */
  public String getJdbcUrlForExecutor() {
    RuntimeConfig runtimeConfig = session().conf();
    if ((runtimeConfig.contains(HWConf.HIVESERVER2_CREDENTIAL_ENABLED)
      && "true".equals(runtimeConfig.get(HWConf.HIVESERVER2_CREDENTIAL_ENABLED)))
      || runtimeConfig.contains(HWConf.HIVESERVER2_JDBC_URL_PRINCIPAL)) {
      String sparkMaster = runtimeConfig.get(HWConf.SPARK_MASTER, "local");
      LOG.debug("Getting jdbc connection url for kerberized cluster with spark.master = {}", sparkMaster);
      if ("yarn".equals(sparkMaster)) {
        StringBuilder urlBuilder = new StringBuilder()
          .append(String.format("%s;auth=delegationToken",
            runtimeConfig.get(HWConf.HIVESERVER2_JDBC_URL)));
        if (runtimeConfig.contains(HWConf.HIVESERVER2_JDBC_URL_CONF_LIST)
          && StringUtils.isNotBlank(runtimeConfig.get(HWConf.HIVESERVER2_JDBC_URL_CONF_LIST))) {
          urlBuilder.append("?").append(runtimeConfig.get(HWConf.HIVESERVER2_JDBC_URL_CONF_LIST));
        }
        return urlBuilder.toString();
      }
    }
    return HWConf.RESOLVED_HS2_URL.getString(sessionState);
  }

  private Dataset<Row> executeJdbcInternal(String sqlStatement) {
    Preconditions.checkNotNull(sqlStatement, "The query cannot be null");
    if (QueryExecutionUtil.isDataFetchQuery(sqlStatement)) {
      ensureSessionOpen();
      String queryMode = QueryExecutionUtil.ExecutionMethod.EXECUTE_HIVE_JDBC.name();
      DataFrameReader dataFrameReader = session().read()
        .format(HIVE_WAREHOUSE_CONNECTOR_INTERNAL)
        .option("query", sqlStatement)
        .option(HWC_SESSION_ID_KEY, sessionState.getSessionId())
        .option(HWC_QUERY_MODE, queryMode)
        .option("hs2.jdbc.url.for.executor", getJdbcUrlForExecutor());
      return dataFrameReader.load();
    }
    return executeSimpleJdbc(sqlStatement);
  }

  private Dataset<Row> executeQuerySecureAccess(String sqlStatement) {
    LOG.info("Execution mode: Secure Access FGAC");
    Preconditions.checkNotNull(sqlStatement, "The query cannot be null");
    ensureSessionOpen();
    return SecureAccessModeExecutor.getInstance(sessionState).execute(sessionState, sqlStatement);
  }

  /**
   * Executes an update statement.
   *
   * @param sqlStatement SQL update statement
   * @return true if execution succeeded
   */
  @Override
  public boolean executeUpdate(String sqlStatement) {
    return executeUpdate(sqlStatement, true);
  }

  /**
   * Executes an update statement and optionally propagates exceptions.
   *
   * @param sqlStatement SQL update statement
   * @param shouldPropagateException whether exceptions should be propagated
   * @return true if execution succeeded
   */
  @Override
  public boolean executeUpdate(String sqlStatement, boolean shouldPropagateException) {
    ensureSessionOpen();
    try (Connection connection = connectionSupplier.get()) {
      return updateExecutorWithExceptionPropagation.apply(connection,
        HWConf.DEFAULT_DB.getString(sessionState), sqlStatement, shouldPropagateException);
    } catch (SQLException exception) {
      throw new RuntimeException(exception.getMessage(), exception);
    }
  }

  private boolean executeUpdateInternal(String sqlStatement, Connection connection) {
    ensureSessionOpen();
    return updateExecutorWithExceptionPropagation.apply(connection,
      HWConf.DEFAULT_DB.getString(sessionState), sqlStatement, true);
  }

  /**
   * Returns a dataset for the given table name.
   *
   * @param tableName table name
   * @return dataset for the table
   */
  @Override
  public Dataset<Row> table(String tableName) {
    ensureSessionOpen();
    return sql("select * from " + tableName);
  }

  /**
   * @return active Spark session
   */
  @Override
  public SparkSession session() {
    ensureSessionOpen();
    return sessionState.getSession();
  }

  /**
   * @return session state
   */
  public HiveWarehouseSessionState sessionState() {
    return sessionState;
  }

  /**
   * @return Spark configuration from the current session
   */
  SparkConf conf() {
    return sessionState.getSession().sparkContext().getConf();
  }

  /**
   * Sets the default database for this session.
   *
   * @param databaseName database name to set
   */
  @Override
  public void setDatabase(String databaseName) {
    ensureSessionOpen();
    HWConf.DEFAULT_DB.setString(sessionState, databaseName);
  }

  private Dataset<Row> executeSimpleJdbc(String sqlStatement) {
    ensureSessionOpen();
    try (Connection connection = connectionSupplier.get()) {
      DriverResultSet resultSet = statementExecutor.apply(connection,
        HWConf.DEFAULT_DB.getString(sessionState), sqlStatement);
      return resultSet.asDataFrame(session());
    } catch (SQLException exception) {
      throw new RuntimeException(exception.getMessage(), exception);
    }
  }

  /**
   * @return dataset listing all databases
   */
  @Override
  public Dataset<Row> showDatabases() {
    ensureSessionOpen();
    return executeSimpleJdbc(HiveQlUtil.showDatabases());
  }

  /**
   * @return dataset listing tables in the current database
   */
  @Override
  public Dataset<Row> showTables() {
    ensureSessionOpen();
    return executeSimpleJdbc(HiveQlUtil.showTables(HWConf.DEFAULT_DB.getString(sessionState)));
  }

  /**
   * Describes a table.
   *
   * @param tableName table name
   * @return dataset describing the table
   */
  @Override
  public Dataset<Row> describeTable(String tableName) {
    ensureSessionOpen();
    return executeSimpleJdbc(HiveQlUtil.describeTable(HWConf.DEFAULT_DB.getString(sessionState), tableName));
  }

  /**
   * Drops a database.
   *
   * @param databaseName database name
   * @param ifExists whether to include IF EXISTS
   * @param cascade whether to cascade
   */
  @Override
  public void dropDatabase(String databaseName, boolean ifExists, boolean cascade) {
    executeUpdate(HiveQlUtil.dropDatabase(databaseName, ifExists, cascade));
  }

  /**
   * Drops a table.
   *
   * @param tableName table name
   * @param ifExists whether to include IF EXISTS
   * @param purge whether to purge data
   */
  @Override
  public void dropTable(String tableName, boolean ifExists, boolean purge) {
    ensureSessionOpen();
    try (Connection connection = connectionSupplier.get()) {
      executeUpdateInternal(HiveQlUtil.useDatabase(HWConf.DEFAULT_DB.getString(sessionState)), connection);
      String dropTableStatement = HiveQlUtil.dropTable(tableName, ifExists, purge);
      executeUpdateInternal(dropTableStatement, connection);
    } catch (SQLException exception) {
      throw new RuntimeException(exception.getMessage(), exception);
    }
  }

  /**
   * Creates a database if it does not exist.
   *
   * @param databaseName database name
   */
  public void createDatabase(String databaseName) {
    executeUpdate(HiveQlUtil.createDatabase(databaseName, false), true);
  }

  /**
   * Creates a database.
   *
   * @param databaseName database name
   * @param ifNotExists whether to include IF NOT EXISTS
   */
  @Override
  public void createDatabase(String databaseName, boolean ifNotExists) {
    executeUpdate(HiveQlUtil.createDatabase(databaseName, ifNotExists), true);
  }

  /**
   * Creates a table builder for the given table name.
   *
   * @param tableName table name
   * @return create table builder
   */
  @Override
  public CreateTableBuilder createTable(String tableName) {
    ensureSessionOpen();
    return new CreateTableBuilder(this, HWConf.DEFAULT_DB.getString(sessionState), tableName,
      HiveWarehouseDataWriterHelper.FileFormat.ORC, new HWCOptions(new HashMap<>()));
  }

  /**
   * @return merge builder for the current database
   */
  @Override
  public MergeBuilder mergeBuilder() {
    ensureSessionOpen();
    return new MergeBuilderImpl(this, HWConf.DEFAULT_DB.getString(sessionState));
  }

  /**
   * Cleans up streaming metadata.
   *
   * @param queryCheckpointDir checkpoint directory
   * @param databaseName database name
   * @param tableName table name
   */
  @Override
  public void cleanUpStreamingMeta(String queryCheckpointDir, String databaseName, String tableName) {
    ensureSessionOpen();
    try {
      new StreamingMetaCleaner(sessionState, queryCheckpointDir, databaseName, tableName).clean();
    } catch (IOException | SQLException exception) {
      throw new RuntimeException(exception.getMessage(), exception);
    }
  }

  /**
   * Commits an open Hive ACID transaction when running in direct reader mode.
   */
  @Override
  public void commitTxn() {
    if (!usingDirectReaderMode()) {
      return;
    }
    try {
      Class<?> transactionManagerClass =
        Class.forName("com.qubole.spark.hiveacid.transaction.HiveAcidTxnManagerObject");
      Method commitTxnMethod = transactionManagerClass.getMethod("commitTxn", SparkSession.class);
      commitTxnMethod.invoke(null, session());
    } catch (ClassNotFoundException exception) {
      LOG.debug("HiveAcidTxnManagerObject not on classpath; skipping commitTxn.");
    } catch (Exception exception) {
      throw new RuntimeException("Failed to commit hive-acid transaction", exception);
    }
  }

  /**
   * Closes the session and releases JDBC resources.
   */
  @Override
  public void close() {
    try {
      commitTxn();
    } catch (Exception exception) {
      LOG.info("Failed to end hive-acid txn. The txn may be committed already.", exception);
    }
    Preconditions.checkState(hwcSessionStateReference.compareAndSet(HwcSessionState.OPEN, HwcSessionState.CLOSED),
      SESSION_CLOSED_MSG);
    DefaultJDBCWrapper.closeHS2ConnectionPool();
  }

  /**
   * @return session identifier
   */
  public String getSessionId() {
    return sessionState.getSessionId();
  }

  /**
   * Shortcut for executeQuery.
   *
   * @param sqlStatement SQL statement to execute
   * @return query results
   */
  @Override
  public Dataset<Row> q(String sqlStatement) {
    return executeQuery(sqlStatement);
  }

  @Override
  protected void finalize() {
    close();
  }

  private enum HwcSessionState {
    OPEN,
    CLOSED
  }
}
