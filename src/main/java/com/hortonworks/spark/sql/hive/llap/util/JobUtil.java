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

import com.google.common.base.Preconditions;
import com.hortonworks.hwc.plan.HwcPlannerStatistics;
import com.hortonworks.spark.sql.hive.llap.DefaultJDBCWrapper;
import com.hortonworks.spark.sql.hive.llap.Column;
import com.hortonworks.spark.sql.hive.llap.DescribeTableOutput;
import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Enumeration;
import java.util.OptionalLong;
import java.util.TimeZone;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.Tuple2;
import scala.Tuple4;

/**
 * Spark and Hadoop utility helpers used by HWC.
 */
public class JobUtil {
  public static final String SPARK_SQL_EXECUTION_ID = "spark.sql.execution.id";
  private static final Logger LOG = LoggerFactory.getLogger(JobUtil.class);
  public static final TimeZone UTC_TIME_ZONE = TimeZone.getTimeZone("UTC");

  /**
   * Returns the active Spark session or the default session if active is not present.
   *
   * @return active Spark session
   */
  public static SparkSession getSparkSession() {
    Option<SparkSession> activeSessionOption = SparkSession.getActiveSession();
    if (activeSessionOption.isEmpty()) {
      activeSessionOption = SparkSession.getDefaultSession();
    }
    return activeSessionOption.get();
  }

  /**
   * Copies Spark SQL configuration entries into the provided Hadoop configuration.
   *
   * @param hadoopConfiguration Hadoop configuration to populate
   */
  public static void populateSparkConfs(Configuration hadoopConfiguration) {
    SQLConf sqlConf = getSparkSession().sessionState().conf();
    scala.collection.Iterator<Tuple4<String, String, String, String>> definedConfIterator =
      sqlConf.getAllDefinedConfs().iterator();
    while (definedConfIterator.hasNext()) {
      Tuple4<String, String, String, String> definedConf = definedConfIterator.next();
      hadoopConfiguration.set(definedConf._1(), definedConf._2());
    }
    scala.collection.Iterator<Tuple2<String, String>> confIterator = sqlConf.getAllConfs().iterator();
    while (confIterator.hasNext()) {
      Tuple2<String, String> confEntry = confIterator.next();
      hadoopConfiguration.set(confEntry._1(), confEntry._2());
    }
  }

  /**
   * Converts a timestamp between time zones at millisecond precision.
   *
   * @param timestampMillis timestamp in milliseconds
   * @param sourceTimeZone source time zone
   * @param targetTimeZone target time zone
   * @return converted timestamp in milliseconds
   */
  public static long convertMillis(long timestampMillis, TimeZone sourceTimeZone, TimeZone targetTimeZone) {
    long sourceOffsetMillis = sourceTimeZone.getOffset(timestampMillis);
    long targetOffsetMillis = targetTimeZone.getOffset(timestampMillis);
    long localMillis = timestampMillis + sourceOffsetMillis - targetOffsetMillis;
    long correctedOffset = sourceOffsetMillis - targetTimeZone.getOffset(localMillis);
    return timestampMillis + correctedOffset;
  }

  /**
   * Converts a timestamp between time zones at microsecond precision.
   *
   * @param timestampMicros timestamp in microseconds
   * @param sourceTimeZone source time zone
   * @param targetTimeZone target time zone
   * @return converted timestamp in microseconds
   */
  public static long convertMicros(long timestampMicros, TimeZone sourceTimeZone, TimeZone targetTimeZone) {
    long timestampMillis = timestampMicros / 1000L;
    long subMillis = timestampMicros % 1000L;
    return convertMillis(timestampMillis, sourceTimeZone, targetTimeZone) * 1000L + subMillis;
  }

  /**
   * Gets the Spark session time zone on the driver.
   *
   * @return driver time zone
   */
  public static TimeZone getSparkSessionTimezoneAtDriver() {
    return TimeZone.getTimeZone(getSparkSession().conf().get(SQLConf.SESSION_LOCAL_TIMEZONE().key()));
  }

  /**
   * Gets the Spark session time zone on an executor.
   *
   * @param hadoopConfiguration Hadoop configuration
   * @return executor time zone
   */
  public static TimeZone getSparkSessionTimezoneAtExecutor(Configuration hadoopConfiguration) {
    return TimeZone.getTimeZone(hadoopConfiguration.get(SQLConf.SESSION_LOCAL_TIMEZONE().key()));
  }

  /**
   * Serializes a JobConf into a byte array.
   *
   * @param jobConf job configuration
   * @return serialized bytes
   * @throws IOException when serialization fails
   */
  public static byte[] serializeJobConf(JobConf jobConf) throws IOException {
    ByteArrayOutputStream byteStream = new ByteArrayOutputStream(200000);
    try (DataOutputStream dataOutputStream = new DataOutputStream(byteStream)) {
      jobConf.write((DataOutput) dataOutputStream);
    }
    return byteStream.toByteArray();
  }

  /**
   * @return SQL execution id at the driver
   */
  public static String getSqlExecutionIdAtDriver() {
    return getSparkSession().sparkContext().getLocalProperty(SPARK_SQL_EXECUTION_ID);
  }

  /**
   * @return SQL execution id at the executor
   */
  public static String getSqlExecutionIdAtExecutor() {
    return TaskContext.get().getLocalProperty(SPARK_SQL_EXECUTION_ID);
  }

  /**
   * Replaces the Spark Hive JDBC driver registration with the Hive driver.
   */
  public static void replaceSparkHiveDriver() {
    try {
      Enumeration<Driver> registeredDrivers = DriverManager.getDrivers();
      while (registeredDrivers.hasMoreElements()) {
        Driver registeredDriver = registeredDrivers.nextElement();
        String driverClassName = registeredDriver.getClass().getName();
        LOG.debug("Found a registered JDBC driver {}", driverClassName);
        if (driverClassName.endsWith("HiveDriver")) {
          LOG.debug("Deregistering {}", driverClassName);
          DriverManager.deregisterDriver(registeredDriver);
        } else {
          LOG.debug("Not deregistering the {}", driverClassName);
        }
      }
      DriverManager.registerDriver((Driver) Class.forName("org.apache.hive.jdbc.HiveDriver").newInstance());
    } catch (Exception exception) {
      throw new RuntimeException(exception.getMessage(), exception);
    }
  }

  /**
   * Retrieves basic statistics for a table.
   *
   * @param databaseName database name
   * @param tableName table name
   * @param connection JDBC connection
   * @return planner statistics
   */
  public static HwcPlannerStatistics getStatisticsForTable(String databaseName, String tableName, Connection connection) {
    DescribeTableOutput describeTableOutput = DefaultJDBCWrapper.describeTable(connection, databaseName, tableName);
    String totalSizeString = null;
    String rowCountString = null;
    for (Column column : describeTableOutput.getDetailedTableInfoColumns()) {
      if (column.getDataType() != null) {
        if ("numRows".equals(column.getDataType().trim())) {
          rowCountString = column.getComment();
        } else if ("totalSize".equals(column.getDataType().trim())) {
          totalSizeString = column.getComment();
        }
      }
      if (totalSizeString != null && rowCountString != null) {
        break;
      }
    }
    Preconditions.checkNotNull(rowCountString, "numRows found null for table - " + tableName);
    Preconditions.checkNotNull(totalSizeString, "totalSize found null for table - " + tableName);
    return new HwcPlannerStatistics(OptionalLong.of(Long.parseLong(totalSizeString)),
      OptionalLong.of(Long.parseLong(rowCountString)));
  }

  /**
   * Builds the staging directory prefix for HWC operations.
   *
   * @param stagingRootPath staging root path
   * @return staging path prefix
   */
  public static String getStagingDirPrefix(String stagingRootPath) {
    Preconditions.checkNotNull(stagingRootPath, "Staging output location is null, Enter valid value");
    SparkContext sparkContext = getSparkSession().sparkContext();
    String applicationStartDate = getApplicationStartDate(getSparkSession(), "UTC")
      .format(DateTimeFormatter.ofPattern("yyyy_MM_dd"));
    return stagingRootPath + '/' + applicationStartDate + '/' + sparkContext.sparkUser() + '/' + sparkContext.applicationId();
  }

  /**
   * Deletes a directory if it exists.
   *
   * @param directoryPath directory path
   * @param fileSystem filesystem to use
   */
  public static void deleteDirectory(String directoryPath, FileSystem fileSystem) {
    try {
      LOG.debug("Attempting to delete dir: {}", directoryPath);
      Path hdfsPath = new Path(directoryPath);
      if (fileSystem.exists(hdfsPath)) {
        LOG.debug("Deleting dir: {}", directoryPath);
        fileSystem.delete(hdfsPath, true);
      }
    } catch (IOException exception) {
      throw new RuntimeException(exception);
    }
  }

  /**
   * Returns the Spark application start time in milliseconds.
   *
   * @param sparkSession Spark session
   * @return application start time
   */
  public static Long getApplicatioStartTime(SparkSession sparkSession) {
    return sparkSession.sparkContext().startTime();
  }

  /**
   * Returns the Spark application start date in the provided time zone.
   *
   * @param sparkSession Spark session
   * @param timeZoneId time zone id
   * @return application start date
   */
  public static LocalDate getApplicationStartDate(SparkSession sparkSession, String timeZoneId) {
    Long startTime = getApplicatioStartTime(sparkSession);
    return Instant.ofEpochMilli(startTime).atZone(ZoneId.of(timeZoneId)).toLocalDate();
  }
}
