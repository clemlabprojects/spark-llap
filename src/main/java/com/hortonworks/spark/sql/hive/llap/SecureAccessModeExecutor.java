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
import com.hortonworks.spark.sql.hive.llap.HWConf;
import com.hortonworks.spark.sql.hive.llap.util.JobUtil;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes HWC queries in secure access mode by staging results into a protected location.
 */
public class SecureAccessModeExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(SecureAccessModeExecutor.class);
  private static final String CONF_STRING =
    "set hive.exec.orc.delta.streaming.optimizations.enabled=true;" +
    "set hive.stats.autogather=false;" +
    "set tez.runtime.pipelined.sorter.lazy-allocate.memory=true";
  private static final String SQL_CREATE_EXTERNAL_TABLE_AS =
    "CREATE TEMPORARY EXTERNAL TABLE %s STORED AS ORC LOCATION '%s' AS SELECT * FROM (%s) as q";
  private static final String SQL_DROP_TABLE = "DROP TABLE IF EXISTS %s";
  private static final String SQL_SINGLE_ROW_PROBE = "SELECT * FROM %s LIMIT 1";
  private static final String STAGING_DIR_ROOT_PERMISSION = "1703";
  private static final String TABLE_PERMISSION = "1700";

  private static volatile SecureAccessModeExecutor INSTANCE;
  private final FileSystem fileSystem;
  private final String applicationStagingDir;
  private final SparkSession sparkSession;
  private static final ThreadLocal<MessageDigest> SHA_DIGEST = ThreadLocal.withInitial(() -> {
    try {
      return MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  });

  /**
   * Get (or initialize) the singleton instance.
   *
   * @param state current HWC session state
   * @return singleton instance
   */
  public static SecureAccessModeExecutor getInstance(HiveWarehouseSessionState state) {
    init(state);
    return INSTANCE;
  }

  /**
   * Initialize the singleton instance if needed.
   *
   * @param state current HWC session state
   */
  public static void init(HiveWarehouseSessionState state) {
    if (INSTANCE != null) {
      return;
    }
    synchronized (SecureAccessModeExecutor.class) {
      if (INSTANCE != null) {
        return;
      }
      INSTANCE = new SecureAccessModeExecutor(state);
    }
  }

  /**
   * Create a secure access executor and its staging directory.
   *
   * @param state current HWC session state
   */
  private SecureAccessModeExecutor(HiveWarehouseSessionState state) {
    this.sparkSession = state.getSession();
    String stagingRoot = HWConf.LOAD_STAGING_DIR.getString(state);
    LOG.debug("Staging dir from conf: {}", stagingRoot);
    String date = Instant.ofEpochMilli(this.sparkSession.sparkContext().startTime())
      .atZone(ZoneId.of(HWConf.STAGING_OUTPUT_TZ.getString(state)))
      .toLocalDate()
      .format(DateTimeFormatter.ofPattern("yyyy_MM_dd"));
    String appId = this.sparkSession.sparkContext().applicationId();
    String user = this.sparkSession.sparkContext().sparkUser();
    this.applicationStagingDir = stagingRoot + "/" + date + "/" + user + "/" + appId;
    try {
      Path path = new Path(this.applicationStagingDir);
      Preconditions.checkNotNull(path.toUri().getScheme(),
        "Please specify the fully qualified path of staging Dir to be used for staging output " +
          "mode. Example: spark.datasource.hive.warehouse.load.staging.dir=s3a://<bucket>/staging/");
      this.fileSystem = path.getFileSystem(this.sparkSession.sparkContext().hadoopConfiguration());
      this.fileSystem.mkdirs(path, new FsPermission(STAGING_DIR_ROOT_PERMISSION));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Execute a query in secure access mode and return a Spark dataset for its results.
   *
   * @param state current HWC session state
   * @param query SQL query to execute
   * @return dataset containing query results
   */
  public Dataset<Row> execute(HiveWarehouseSessionState state, String query) {
    Preconditions.checkNotNull(query, "The query cannot be null");
    Preconditions.checkNotNull(state.getSession(),
      "Instantiate HiveWarehouseSession appropriately. Ex: " +
        "val hive = com.hortonworks.hwc.HiveWarehouseSession.session(spark).build");

    boolean disableCache = HWConf.isSecureAccessCacheDisabled(state);
    String tableName = "table_" + getQuerySha(query);
    String tableDir = getTableDir(state.getSessionId(), tableName);
    Path path = new Path(tableDir);
    try {
      if (disableCache && fileSystem.exists(path)) {
        LOG.info("Deleting cached folder {} for query {}", path, query);
        JobUtil.deleteDirectory(tableDir, fileSystem);
      }
      if (!fileSystem.exists(path)) {
        LOG.info("Create table data for query in staging space, use cache: {}, path: {}, query: {}",
          !disableCache, tableDir, query);
        createDirWithPermissions(path);
        persistQueryResult(state, tableName, tableDir, query);
      } else {
        LOG.info("Table already exists in staging space for query, use cache: {}, path: {}, query: {}",
          !disableCache, tableDir, query);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this.sparkSession.read().orc(tableDir + "/*");
  }

  /**
   * Build the staging directory for a session and table.
   *
   * @param sessionId HWC session id
   * @param tableName temporary table name
   * @return fully qualified staging path
   */
  private String getTableDir(String sessionId, String tableName) {
    return this.applicationStagingDir + "/hwc_session_" + sessionId + "/" + tableName;
  }

  /**
   * Delete the application-level staging directory.
   */
  private void deleteStagingDir() {
    LOG.info("Deleting staging dir: {}", this.applicationStagingDir);
    JobUtil.deleteDirectory(this.applicationStagingDir, this.fileSystem);
  }

  /**
   * Compute a stable hash for the query to build a deterministic table name.
   *
   * @param query SQL query
   * @return hex-encoded SHA-256 hash
   */
  private String getQuerySha(String query) {
    return String.format("%032x",
      new BigInteger(1, SHA_DIGEST.get().digest(query.getBytes(StandardCharsets.UTF_8))));
  }

  /**
   * Create a staging directory with secure permissions.
   *
   * @param path directory path to create
   * @throws IOException when creation fails
   */
  private void createDirWithPermissions(Path path) throws IOException {
    FsPermission fsPermission = new FsPermission(TABLE_PERMISSION);
    LOG.debug("Creating staging table: {}", path);
    this.fileSystem.mkdirs(path, fsPermission);
  }

  /**
   * Execute the query and persist results into a temporary external table.
   *
   * @param state current HWC session state
   * @param tableName temporary table name
   * @param tableDir staging directory for the table
   * @param query SQL query
   */
  private void persistQueryResult(HiveWarehouseSessionState state,
                                  String tableName,
                                  String tableDir,
                                  String query) {
    LOG.info("Persisting query results to temporary table ({}), sql: {}", tableName, query);
    String dropTable = String.format(SQL_DROP_TABLE, tableName);
    String createTable = String.format(SQL_CREATE_EXTERNAL_TABLE_AS, tableName, tableDir, query);
    try (Connection connection = DefaultJDBCWrapper.getJDBCConnOutsideThePool(state)) {
      DefaultJDBCWrapper.executeUpdate(connection, HWConf.DEFAULT_DB.getString(state), CONF_STRING, dropTable);
      int rowCount = DefaultJDBCWrapper.executeUpdate(
        connection,
        HWConf.DEFAULT_DB.getString(state),
        CONF_STRING,
        createTable);
      LOG.info("Rows selected for table ({}): {}", tableName, rowCount);
      if (rowCount == 0) {
        Dataset<Row> sampleDataFrame = DefaultJDBCWrapper.executeStmt(
          connection,
          HWConf.DEFAULT_DB.getString(state),
          SQL_SINGLE_ROW_PROBE.replace("%s", tableName),
          0L).asDataFrame(state.getSession());
        LOG.info("Double-check for empty table ({}), is empty: {}",
          tableName, sampleDataFrame.isEmpty());
        if (sampleDataFrame.isEmpty()) {
          String sql = String.format("SELECT * FROM %s", tableName);
          Dataset<Row> schemaProbeDataFrame = DefaultJDBCWrapper.executeStmt(
            connection,
            HWConf.DEFAULT_DB.getString(state),
            sql,
            0L).asDataFrame(state.getSession());
          // Persist an ORC schema file so Spark can infer schema from the directory.
          schemaProbeDataFrame.write().orc(tableDir + "/schema");
        }
      }
      LOG.debug("Temporary table creation was successful: {}", tableName);
    } catch (SQLException e) {
      LOG.warn("Deleting staging dir due to some errors while generating data: {}", tableDir);
      JobUtil.deleteDirectory(tableDir, fileSystem);
      throw new RuntimeException(e);
    }
  }

  /**
   * Close and cleanup the singleton executor.
   */
  public static void close() {
    if (INSTANCE == null) {
      return;
    }
    INSTANCE.deleteStagingDir();
  }
}
