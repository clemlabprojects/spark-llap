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
import com.hortonworks.spark.sql.hive.llap.HiveWarehouseSessionState;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.spark.sql.execution.streaming.StreamMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

/**
 * Cleans streaming metadata and exactly-once table properties for HWC streaming queries.
 */
public class StreamingMetaCleaner {
  private static final Logger LOG = LoggerFactory.getLogger(StreamingMetaCleaner.class);
  private static final String EXACTLY_ONCE_COMMIT_KEY_FORMAT = "_meta_SPARK_%s";
  private final HiveWarehouseSessionState sessionState;
  private final String queryCheckpointDir;
  private final String dbName;
  private final String tableName;

  /**
   * Creates a cleaner for the given streaming checkpoint and table.
   *
   * @param sessionState active HWC session state
   * @param queryCheckpointDir checkpoint directory of the streaming query
   * @param dbName Hive database name
   * @param tableName Hive table name
   */
  public StreamingMetaCleaner(HiveWarehouseSessionState sessionState,
                              String queryCheckpointDir,
                              String dbName,
                              String tableName) {
    this.sessionState = sessionState;
    this.queryCheckpointDir = queryCheckpointDir;
    this.dbName = dbName;
    this.tableName = tableName;
  }

  /**
   * Removes the exactly-once table property and deletes the checkpoint directory.
   *
   * @throws IOException when checkpoint metadata cannot be read or deleted
   * @throws SQLException when JDBC operations fail
   */
  public void clean() throws IOException, SQLException {
    Path checkpointPath = new Path(queryCheckpointDir);
    Configuration hadoopConfiguration = sessionState.getSession().sparkContext().hadoopConfiguration();
    Option<StreamMetadata> streamMetadata = getStreamMetadata(hadoopConfiguration, checkpointPath);
    if (streamMetadata.isEmpty()) {
      LOG.error("Unable to determine queryId, either queryCheckpointDir[{}] does not contain " +
        "metadata file or it is corrupted!!!", queryCheckpointDir);
      return;
    }
    String tablePropertyKey = getExactlyOncePropertyKey(streamMetadata.get().id());
    try (Connection connection = DefaultJDBCWrapper.getConnector(sessionState)) {
      // Remove the exactly-once property before deleting checkpoint data.
      DefaultJDBCWrapper.unsetTableProperties(connection, dbName, tableName, false, tablePropertyKey);
      LOG.info("SUCCESSFUL: Unset tblproperty: {} for database:{}, table:{}",
        tablePropertyKey, dbName, tableName);
    } catch (Exception exception) {
      if (isHivePropNotFound(exception)) {
        LOG.info("Table property:{} not found for table:{}, proceeding to delete checkpoint directory.",
          tablePropertyKey, tableName);
      } else if (exception instanceof SQLException) {
        throw (SQLException) exception;
      } else {
        throw new RuntimeException(exception);
      }
    }
    // Remove the checkpoint directory after successful property cleanup.
    checkpointPath.getFileSystem(hadoopConfiguration).delete(checkpointPath, true);
    LOG.info("SUCCESSFUL: Deleted specified queryCheckpointDir: {}", queryCheckpointDir);
  }

  private Option<StreamMetadata> getStreamMetadata(Configuration hadoopConfiguration,
                                                   Path checkpointPath) throws IOException {
    FileSystem fileSystem = checkpointPath.getFileSystem(hadoopConfiguration);
    Path qualifiedCheckpointPath =
        checkpointPath.makeQualified(fileSystem.getUri(), fileSystem.getWorkingDirectory());
    Path metadataPath = new Path(qualifiedCheckpointPath, "metadata");
    return StreamMetadata.read(metadataPath, hadoopConfiguration);
  }

  private String getExactlyOncePropertyKey(String queryId) {
    return String.format(EXACTLY_ONCE_COMMIT_KEY_FORMAT, queryId);
  }

  private boolean isHivePropNotFound(Exception exception) {
    String className = exception.getClass().getName();
    if ("org.apache.hive.service.cli.HiveSQLException".equals(className)
      || "shadehiveservice.org.apache.hive.service.cli.HiveSQLException".equals(className)) {
      try {
        java.lang.reflect.Method method = exception.getClass().getMethod("getErrorCode");
        Object errorCodeObject = method.invoke(exception);
        int errorCode = (Integer) errorCodeObject;
        return ErrorMsg.ALTER_TBL_UNSET_NON_EXIST_PROPERTY.getErrorCode() == errorCode;
      } catch (Exception ignoredException) {
        return false;
      }
    }
    return false;
  }
}
