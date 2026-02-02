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

import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.RuntimeConfig;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.String.format;

/**
 * Provides Hive Warehouse Connector configuration keys and helpers.
 *
 * <p>See {@link org.apache.spark.sql.connector.catalog.SessionConfigSupport} for Spark session
 * config support.
 */
public enum HWConf {

  // ENUM(shortKey, qualifiedKey, default)
  USER("user.name", warehouseKey("user.name"), ""),
  PASSWORD("password", warehouseKey("password"), ""),
  RESOLVED_HS2_URL("hs2.url.resolved", warehouseKey("hs2.url.resolved"), ""),
  DBCP2_CONF("dbcp2.conf", warehouseKey("dbcp2.conf"), null),
  DEFAULT_DB("default.db", warehouseKey("default.db"), "default"),
  MAX_EXEC_RESULTS("exec.results.max", warehouseKey("exec.results.max"), 0),
  LOAD_STAGING_DIR("load.staging.dir", warehouseKey("load.staging.dir"), "/tmp"),
  ARROW_ALLOCATOR_MAX("arrow.allocator.max", warehouseKey("arrow.allocator.max"), Long.MAX_VALUE),
  COUNT_TASKS("count.tasks", warehouseKey("count.tasks"), 100),
  SMART_EXECUTION("smartExecution", warehouseKey("smartExecution"), "false"),
  STAGING_OUTPUT_TZ("staging.output.dir.date.tz", "staging.output.dir.date.tz", "UTC"),
  WRITE_PATH_STRICT_COLUMN_NAMES_MAPPING("write.path.strictColumnNamesMapping",
    warehouseKey("write.path.strictColumnNamesMapping"), "true"),
  READ_JDBC_MODE("read.jdbc.mode", warehouseKey("read.jdbc.mode"), "client"),
  DEFAULT_WRITE_FORMAT("default.write.format", warehouseKey("default.write.format"), "orc"),
  BATCH_DATAREADER_COLUMNS_LIMIT("batch.datareader.columns.limit",
    warehouseKey("batch.datareader.columns.limit"), 1000),
  ENFORCE_DEFAULT_CONSTRAINTS("enforce.default.constraints",
    warehouseKey("enforce.default.constraints"), "true");

  HWConf(String simpleKeyName, String qualifiedKeyName, Object defaultValueObject) {
    this.simpleKeyName = simpleKeyName;
    this.qualifiedKeyName = qualifiedKeyName;
    this.defaultValueObject = defaultValueObject;
  }

  /**
   * Builds a fully qualified HWC config key for the given suffix.
   *
   * @param keySuffix config key suffix
   * @return the fully qualified HWC config key
   */
  static String warehouseKey(String keySuffix) {
    return HiveWarehouseSession.CONF_PREFIX + "." + keySuffix;
  }

  private static final Logger LOG = LoggerFactory.getLogger(HWConf.class);

  public static final String HIVESERVER2_CREDENTIAL_ENABLED =
    "spark.security.credentials.hiveserver2.enabled";
  public static final String HIVESERVER2_JDBC_URL_PRINCIPAL =
    "spark.sql.hive.hiveserver2.jdbc.url.principal";
  public static final String HIVESERVER2_JDBC_URL =
    "spark.sql.hive.hiveserver2.jdbc.url";
  public static final String HIVESERVER2_JDBC_URL_CONF_LIST =
    "spark.sql.hive.conf.list";
  public static final String SPARK_SUBMIT_DEPLOYMODE =
    "spark.submit.deployMode";
  public static final String SPARK_MASTER =
    "spark.master";
  public static final String PARTITION_OPTION_KEY =
    "partition";
  public static final String HWC_EXECUTION_MODE =
    "spark.sql.hive.hwc.execution.mode";
  public static final String HWC_READ_MODE =
    "spark.datasource.hive.warehouse.read.mode";
  public static final String SECURE_ACCESS_DISABLE_CACHING =
    "spark.hadoop.secure.access.cache.disable";
  public static final String HWC_EXECUTION_MODE_SPARK = "spark";

  public static final String INVALID_BATCH_DATAREADER_COLUMNS_LIMIT_CONFIG_ERR_MSG =
    format("The config %s is invalid. It should be a valid integer > 0.",
      BATCH_DATAREADER_COLUMNS_LIMIT.getQualifiedKey());

  /**
   * Sets a string value in the session state and Spark SQL conf.
   *
   * @param state hive warehouse session state
   * @param value string value to set
   */
  public void setString(HiveWarehouseSessionState state, String value) {
    state.getProps().put(qualifiedKeyName, value);
    state.getSession().sessionState().conf().setConfString(qualifiedKeyName, value);
  }

  /**
   * Sets an integer value in the session state and Spark SQL conf.
   *
   * @param state hive warehouse session state
   * @param value integer value to set
   */
  public void setInt(HiveWarehouseSessionState state, Integer value) {
    String stringValue = Integer.toString(value);
    state.getProps().put(qualifiedKeyName, stringValue);
    state.getSession().sessionState().conf().setConfString(qualifiedKeyName, stringValue);
  }

  // This is called from executors so it can't depend explicitly on session state.
  /**
   * Reads the value from the options map using the simple key name.
   *
   * @param options reader or writer options
   * @return the configured value or the default value if present
   */
  public String getFromOptionsMap(Map<String, String> options) {
    String configuredValue = options.get(simpleKeyName);
    if (configuredValue == null) {
      configuredValue = options.get(simpleKeyName.toLowerCase());
    }
    return Optional.ofNullable(configuredValue)
      .orElse(defaultValueObject == null ? null : defaultValueObject.toString());
  }

  /**
   * @return fully qualified configuration key
   */
  public String getQualifiedKey() {
    return qualifiedKeyName;
  }

  /**
   * @return short, unqualified configuration key
   */
  public String getSimpleKey() {
    return simpleKeyName;
  }

  /**
   * Reads a string value from the session state, falling back to Spark SQL conf.
   *
   * @param state hive warehouse session state
   * @return configured string value
   */
  String getString(HiveWarehouseSessionState state) {
    return Optional.ofNullable(state.getProps().get(qualifiedKeyName))
      .orElse(state.getSession().sessionState().conf()
        .getConfString(qualifiedKeyName, (String) defaultValueObject));
  }

  /**
   * Reads an integer value from the session state, falling back to Spark SQL conf.
   *
   * @param state hive warehouse session state
   * @return configured integer value
   */
  Integer getInt(HiveWarehouseSessionState state) {
    return Integer.parseInt(Optional.ofNullable(state.getProps().get(qualifiedKeyName))
      .orElse(state.getSession().sessionState().conf()
        .getConfString(qualifiedKeyName, defaultValueObject.toString())));
  }

  /**
   * @return the default value for this configuration
   */
  Object getDefaultValue() {
    return defaultValueObject;
  }

  /**
   * Return connection URL (with replaced proxy user name if exists).
   *
   * @param state hive warehouse session state
   * @return connection URL with user substitution applied
   */
  public static String getConnectionUrl(HiveWarehouseSessionState state) {
    String userString = USER.getString(state);
    if (userString == null) {
      userString = "";
    }
    String urlString = getConnectionUrlFromConf(state);
    String resolvedUrl = urlString.replace("${user}", userString);
    LOG.info("Using HS2 URL: {}", resolvedUrl);
    return resolvedUrl;
  }

  /**
   * For the given HiveServer2 JDBC URLs, attach the postfix strings if needed.
   *
   * For kerberized clusters,
   *
   * 1. YARN cluster mode: ";auth=delegationToken"
   * 2. YARN client mode: ";principal=hive/_HOST@EXAMPLE.COM"
   *
   * Non-kerberied clusters,
   * 3. Use the given URLs.
   *
   * @param state hive warehouse session state
   * @return JDBC URL with any required security parameters appended
   */
  public static String getConnectionUrlFromConf(HiveWarehouseSessionState state) {
    SparkSession sparkSession = state.getSession();
    RuntimeConfig runtimeConfig = sparkSession.conf();
    StringBuilder urlBuilder = new StringBuilder();

    if ((runtimeConfig.contains(HIVESERVER2_CREDENTIAL_ENABLED)
        && "true".equals(runtimeConfig.get(HIVESERVER2_CREDENTIAL_ENABLED)))
      || runtimeConfig.contains(HIVESERVER2_JDBC_URL_PRINCIPAL)) {
      String deployMode = runtimeConfig.get(SPARK_SUBMIT_DEPLOYMODE, "client");
      LOG.debug("Getting jdbc connection url for kerberized cluster with spark.submit.deployMode = {}",
        deployMode);
      if ("cluster".equals(deployMode)) {
        urlBuilder.append(format("%s;auth=delegationToken", runtimeConfig.get(HIVESERVER2_JDBC_URL)));
      } else {
        String principal = runtimeConfig.get(HIVESERVER2_JDBC_URL_PRINCIPAL, "");
        if (StringUtils.isBlank(principal)) {
          urlBuilder.append(format("%s;auth=delegationToken", runtimeConfig.get(HIVESERVER2_JDBC_URL)));
        } else {
          urlBuilder.append(format("%s;principal=%s",
            runtimeConfig.get(HIVESERVER2_JDBC_URL),
            principal));
        }
      }
    } else {
      LOG.debug("Getting jdbc connection url for non-kerberized cluster");
      urlBuilder.append(runtimeConfig.get(HIVESERVER2_JDBC_URL));
    }

    if (runtimeConfig.contains(HIVESERVER2_JDBC_URL_CONF_LIST)
        && StringUtils.isNotBlank(runtimeConfig.get(HIVESERVER2_JDBC_URL_CONF_LIST))) {
      urlBuilder.append("?").append(runtimeConfig.get(HIVESERVER2_JDBC_URL_CONF_LIST));
    }
    return urlBuilder.toString();
  }

  /**
   * Reads the configured HWC execution mode from the active Spark session.
   *
   * @return lower-cased execution mode, or empty string if unset
   */
  public static String getHwcExecutionMode() {
    SparkSession sparkSession = SparkSession.active();
    if (sparkSession.sessionState().conf().contains(HWC_EXECUTION_MODE)) {
      LOG.info("HWC_EXECUTION_MODE{}",
        sparkSession.sessionState().conf().getConfString(HWC_EXECUTION_MODE).toLowerCase());
      return sparkSession.sessionState().conf().getConfString(HWC_EXECUTION_MODE).toLowerCase();
    }
    return "";
  }

  /**
   * Checks whether secure access caching is disabled for the current session.
   *
   * @param state hive warehouse session state
   * @return true if caching is disabled
   */
  public static boolean isSecureAccessCacheDisabled(HiveWarehouseSessionState state) {
    SQLConf conf = state.getSQLConf();
    return conf.contains(SECURE_ACCESS_DISABLE_CACHING)
      && Boolean.parseBoolean(conf.getConfString(SECURE_ACCESS_DISABLE_CACHING.toLowerCase()));
  }

  /**
   * Returns configured Spark SQL extensions if present.
   *
   * @param state hive warehouse session state
   * @return extensions configuration string or empty string when unset
   */
  public static String getSparkSqlExtension(HiveWarehouseSessionState state) {
    if (state.getSession().conf().contains("spark.sql.extensions")) {
      return state.getSession().conf().get("spark.sql.extensions");
    }
    return "";
  }

  private final String simpleKeyName;
  private final String qualifiedKeyName;
  private final Object defaultValueObject;

  public enum HWCReadMode {
    DIRECT_READER_V1,
    DIRECT_READER_V2,
    JDBC_CLUSTER,
    JDBC_CLIENT,
    SECURE_ACCESS;

    private static HWCReadMode getReadMode(String readModeName) {
      switch (readModeName) {
        case "direct_reader_v1":
        case "spark":
          return DIRECT_READER_V1;
        case "direct_reader_v2":
          return DIRECT_READER_V2;
        case "jdbc_client":
          return JDBC_CLIENT;
        case "secure_access":
          return SECURE_ACCESS;
        default:
          return JDBC_CLUSTER;
      }
    }

    public static boolean resolveReadMode(HWCReadMode readMode) {
      SparkSession sparkSession = SparkSession.active();
      String jdbcReadMode = sparkSession.sessionState().conf()
        .getConfString("spark.datasource.hive.warehouse.read.jdbc.mode", "cluster")
        .toLowerCase();

      if (sparkSession.sessionState().conf().contains(HWConf.HWC_READ_MODE)) {
        String readModeValue = sparkSession.sessionState().conf()
          .getConfString(HWConf.HWC_READ_MODE).toLowerCase();
        return HWCReadMode.getReadMode(readModeValue).equals(readMode);
      }

      if (sparkSession.sessionState().conf().contains(HWConf.HWC_EXECUTION_MODE)) {
        LOG.info("'spark.sql.hive.hwc.execution.mode' will be replaced by " +
          "'spark.datasource.hive.warehouse.read.mode'. Please switch to " +
          "'spark.datasource.hive.warehouse.read.mode' to specify the read mode.");
        String executionModeValue = sparkSession.sessionState().conf()
          .getConfString(HWConf.HWC_EXECUTION_MODE).toLowerCase();
        if (sparkSession.sessionState().conf().contains("spark.sql.extensions")) {
          String extensionsConfig = sparkSession.sessionState().conf()
            .getConfString("spark.sql.extensions");
          if (executionModeValue.equalsIgnoreCase(HWConf.HWC_EXECUTION_MODE_SPARK)) {
            if (extensionsConfig.equalsIgnoreCase("com.qubole.spark.hiveacid.HiveAcidAutoConvertExtension")) {
              return HWCReadMode.getReadMode(executionModeValue).equals(readMode);
            }
            if (jdbcReadMode.equalsIgnoreCase("client")) {
              return HWCReadMode.getReadMode("jdbc_client").equals(readMode);
            }
          }
        }
        return HWCReadMode.getReadMode("jdbc_cluster").equals(readMode);
      }

      if (jdbcReadMode.equalsIgnoreCase("client")) {
        return HWCReadMode.getReadMode("jdbc_client").equals(readMode);
      }
      return HWCReadMode.getReadMode("jdbc_cluster").equals(readMode);
    }
  }
}
