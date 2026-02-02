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

package com.hortonworks.spark.sql.hive.llap.common;

import com.hortonworks.spark.sql.hive.llap.HiveWarehouseSession;
import com.hortonworks.spark.sql.hive.llap.HiveWarehouseSessionState;
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
 * Spark/HWC configuration keys and helpers for resolving runtime behavior.
 *
 * <p>See: {@link org.apache.spark.sql.connector.catalog.SessionConfigSupport}</p>
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

  private HWConf(String simpleConfigKey, String qualifiedConfigKey, Object defaultConfigValue) {
    this.simpleConfigKey = simpleConfigKey;
    this.qualifiedConfigKey = qualifiedConfigKey;
    this.defaultConfigValue = defaultConfigValue;
  }

  /**
   * Build a fully qualified HWC config key from a suffix.
   *
   * @param keySuffix suffix to append to the HWC config prefix
   * @return fully qualified HWC config key
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
   * Set a string config for the current session and cache it in the session state.
   *
   * @param state current HWC session state
   * @param value string value to set
   */
  public void setString(HiveWarehouseSessionState state, String value) {
    state.getProps().put(qualifiedConfigKey, value);
    state.getSession().sessionState().conf().setConfString(qualifiedConfigKey, value);
  }

  /**
   * Set an integer config for the current session and cache it in the session state.
   *
   * @param state current HWC session state
   * @param value integer value to set
   */
  public void setInt(HiveWarehouseSessionState state, Integer value) {
    state.getProps().put(qualifiedConfigKey, Integer.toString(value));
    state.getSession().sessionState().conf().setConfString(qualifiedConfigKey, Integer.toString(value));
  }

  /**
   * Resolve the config value from a Spark data source options map.
   *
   * <p>This is called from executors, so it cannot depend on session state.</p>
   *
   * @param optionMap Spark data source options
   * @return configured value or default value
   */
  public String getFromOptionsMap(Map<String, String> optionMap) {
    String optionValue = optionMap.get(simpleConfigKey);
    if (optionValue == null) {
      optionValue = optionMap.get(simpleConfigKey.toLowerCase());
    }
    return Optional.ofNullable(optionValue)
      .orElse(defaultConfigValue == null ? null : defaultConfigValue.toString());
  }

  /**
   * @return fully qualified config key
   */
  public String getQualifiedKey() {
    return qualifiedConfigKey;
  }

  /**
   * @return short/simple config key
   */
  public String getSimpleKey() {
    return simpleConfigKey;
  }

  /**
   * Resolve the string config value for this key from the session state.
   *
   * @param state current HWC session state
   * @return configured value or default value
   */
  String getString(HiveWarehouseSessionState state) {
    return Optional.ofNullable(state.getProps().get(qualifiedConfigKey))
      .orElse(state.getSession().sessionState().conf()
        .getConfString(qualifiedConfigKey, (String) defaultConfigValue));
  }

  /**
   * Resolve the integer config value for this key from the session state.
   *
   * @param state current HWC session state
   * @return configured integer value or default value
   */
  Integer getInt(HiveWarehouseSessionState state) {
    return Integer.parseInt(Optional.ofNullable(state.getProps().get(qualifiedConfigKey))
      .orElse(state.getSession().sessionState().conf()
        .getConfString(qualifiedConfigKey, defaultConfigValue.toString())));
  }

  /**
   * @return default config value for this key
   */
  Object getDefaultValue() {
    return defaultConfigValue;
  }

  /**
   * Return connection URL (with replaced proxy user name if exists).
   *
   * @param state current HWC session state
   * @return resolved JDBC URL with user substitution if needed
   */
  public static String getConnectionUrl(HiveWarehouseSessionState state) {
    String userString = USER.getString(state);
    if (userString == null) {
      userString = "";
    }
    String urlString = getConnectionUrlFromConf(state);
    String returnValue = urlString.replace("${user}", userString);
    LOG.info("Using HS2 URL: {}", returnValue);
    return returnValue;
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
   * @param state current HWC session state
   * @return JDBC URL with required security parameters appended
   */
  public static String getConnectionUrlFromConf(HiveWarehouseSessionState state) {
    SparkSession sparkSession = state.getSession();
    RuntimeConfig conf = sparkSession.conf();
    StringBuilder builder = new StringBuilder();

    boolean delegationTokenEnabled = conf.contains(HIVESERVER2_CREDENTIAL_ENABLED)
      && "true".equals(conf.get(HIVESERVER2_CREDENTIAL_ENABLED));
    boolean hasKerberosPrincipal = conf.contains(HIVESERVER2_JDBC_URL_PRINCIPAL);
    if (delegationTokenEnabled || hasKerberosPrincipal) {
      String deployMode = conf.get(SPARK_SUBMIT_DEPLOYMODE, "client");
      LOG.debug("Getting jdbc connection url for kerberized cluster with spark.submit.deployMode = {}",
        deployMode);
      if ("cluster".equals(deployMode)) {
        builder.append(format("%s;auth=delegationToken", conf.get(HIVESERVER2_JDBC_URL)));
      } else {
        builder.append(format("%s;principal=%s",
          conf.get(HIVESERVER2_JDBC_URL),
          conf.get(HIVESERVER2_JDBC_URL_PRINCIPAL)));
      }
    } else {
      LOG.debug("Getting jdbc connection url for non-kerberized cluster");
      builder.append(conf.get(HIVESERVER2_JDBC_URL));
    }

    if (conf.contains(HIVESERVER2_JDBC_URL_CONF_LIST)
        && StringUtils.isNotBlank(conf.get(HIVESERVER2_JDBC_URL_CONF_LIST))) {
      builder.append("?").append(conf.get(HIVESERVER2_JDBC_URL_CONF_LIST));
    }
    return builder.toString();
  }

  /**
   * Resolve the configured HWC execution mode if present.
   *
   * @return lower-cased execution mode or empty string if not configured
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
   * Check whether secure access cache is disabled.
   *
   * @param state current HWC session state
   * @return true when secure access cache is disabled
   */
  public static boolean isSecureAccessCacheDisabled(HiveWarehouseSessionState state) {
    SQLConf conf = state.getSQLConf();
    return conf.contains(SECURE_ACCESS_DISABLE_CACHING)
      && Boolean.parseBoolean(conf.getConfString(SECURE_ACCESS_DISABLE_CACHING.toLowerCase()));
  }

  /**
   * Read the configured Spark SQL extensions string.
   *
   * @param state current HWC session state
   * @return value of spark.sql.extensions or empty string if not configured
   */
  public static String getSparkSqlExtension(HiveWarehouseSessionState state) {
    if (state.getSession().conf().contains("spark.sql.extensions")) {
      return state.getSession().conf().get("spark.sql.extensions");
    }
    return "";
  }

  private final String simpleConfigKey;
  private final String qualifiedConfigKey;
  private final Object defaultConfigValue;

  /**
   * Supported read modes for HWC direct and JDBC paths.
   */
  public enum HWCReadMode {
    DIRECT_READER_V1,
    DIRECT_READER_V2,
    JDBC_CLUSTER,
    JDBC_CLIENT,
    SECURE_ACCESS;

    private static HWCReadMode getReadMode(String modeName) {
      switch (modeName) {
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

    /**
     * Resolve the effective read mode based on Spark/HWC configs.
     *
     * @param readMode expected read mode
     * @return true if the expected read mode matches the resolved mode
     */
    public static boolean resolveReadMode(HWCReadMode readMode) {
      SparkSession sparkSession = SparkSession.active();
      String jdbcMode = sparkSession.sessionState().conf()
        .getConfString("spark.datasource.hive.warehouse.read.jdbc.mode", "cluster")
        .toLowerCase();

      if (sparkSession.sessionState().conf().contains(HWConf.HWC_READ_MODE)) {
        String configuredReadMode = sparkSession.sessionState().conf()
          .getConfString(HWConf.HWC_READ_MODE).toLowerCase();
        return HWCReadMode.getReadMode(configuredReadMode).equals(readMode);
      }

      if (sparkSession.sessionState().conf().contains(HWConf.HWC_EXECUTION_MODE)) {
        LOG.info("'spark.sql.hive.hwc.execution.mode' will be replaced by " +
          "'spark.datasource.hive.warehouse.read.mode'. Please switch to " +
          "'spark.datasource.hive.warehouse.read.mode' to specify the read mode.");
        String configuredExecutionMode = sparkSession.sessionState().conf()
          .getConfString(HWConf.HWC_EXECUTION_MODE).toLowerCase();
        if (sparkSession.sessionState().conf().contains("spark.sql.extensions")) {
          String sparkSqlExtensions = sparkSession.sessionState().conf()
            .getConfString("spark.sql.extensions");
          if (configuredExecutionMode.equalsIgnoreCase(HWConf.HWC_EXECUTION_MODE_SPARK)) {
            if (sparkSqlExtensions.equalsIgnoreCase(
              "com.qubole.spark.hiveacid.HiveAcidAutoConvertExtension")) {
              return HWCReadMode.getReadMode(configuredExecutionMode).equals(readMode);
            }
            if (jdbcMode.equalsIgnoreCase("client")) {
              return HWCReadMode.getReadMode("jdbc_client").equals(readMode);
            }
          }
        }
        return HWCReadMode.getReadMode("jdbc_cluster").equals(readMode);
      }

      if (jdbcMode.equalsIgnoreCase("client")) {
        return HWCReadMode.getReadMode("jdbc_client").equals(readMode);
      }
      return HWCReadMode.getReadMode("jdbc_cluster").equals(readMode);
    }
  }
}
