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

import com.hortonworks.spark.sql.hive.llap.util.HWCOptions;
import com.hortonworks.spark.sql.hive.llap.util.JobUtil;
import com.hortonworks.spark.sql.hive.llap.util.QueryExecutionUtil;
import com.hortonworks.spark.sql.hive.llap.util.SchemaUtil;
import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Spark catalog implementation for the Hive Warehouse Connector.
 */
public class HWCCatalog implements TableCatalog {
  private static final Logger LOG = LoggerFactory.getLogger(HWCCatalog.class);
  public static final String HWC_CATALOG_NAME = "hwc";
  protected static final String HWC_CATALOG_CONF = "spark.sql.catalog.hwc";
  private static final String HWC_CATALOG_CLASS_NAME =
      "com.hortonworks.spark.sql.hive.llap.HWCCatalog";

  /**
   * Registers the HWC catalog in the active Spark session.
   */
  public static void setupHWCCatalog() {
    SparkSession.active().conf().set(HWC_CATALOG_CONF, HWC_CATALOG_CLASS_NAME);
  }

  @Override
  public Table createTable(Identifier identifier,
                           StructType schema,
                           Transform[] partitions,
                           Map<String, String> properties)
      throws TableAlreadyExistsException, NoSuchNamespaceException {
    HWCIdentifier hwcIdentifier = resolveIdentifier(identifier, properties);
    Map<String, String> options = hwcIdentifier.getOptions();
    String databaseName = hwcIdentifier.namespace()[0];
    String tableName = hwcIdentifier.name();

    try (Connection connection = getConnection(options)) {
      if (!DefaultJDBCWrapper.databaseExists(connection, databaseName)) {
        LOG.info("Database does not exist: {}", databaseName);
        throw new NoSuchNamespaceException(new String[] {databaseName});
      }
      boolean tableExists = DefaultJDBCWrapper.tableExists(connection, databaseName, tableName);
      if (tableExists) {
        LOG.info("Table: {}.{} already exists, so should not be created again", databaseName, tableName);
        throw new TableAlreadyExistsException(databaseName, tableName);
      }

      DataWriteQueryBuilder queryBuilder =
          new DataWriteQueryBuilder(databaseName, tableName, null, null, schema);
      queryBuilder.withCreateTableQuery(true)
          .withStorageFormat(HiveWarehouseDataWriterHelper.FileFormat.getFormat(
              options.getOrDefault("fileformat", "orc")))
          .withValidateAgainstHiveColumns(false)
          .withLoadData(false)
          .withOptions(new HWCOptions(options));

      if (options.containsKey("partition")) {
        queryBuilder.withPartitionSpec(options.get("partition"));
      }

      String createTableQuery = queryBuilder.build().get(0);
      LOG.info("Created table query: {}", createTableQuery);
      DefaultJDBCWrapper.executeUpdate(connection, databaseName, createTableQuery, true);
    } catch (NoSuchNamespaceException | TableAlreadyExistsException exception) {
      throw exception;
    } catch (Exception exception) {
      throw new RuntimeException(exception.getMessage(), exception);
    }
    return new HiveWarehouseConnector(options);
  }

  @Override
  public boolean tableExists(Identifier identifier) {
    HWCIdentifier hwcIdentifier = resolveIdentifier(identifier, null);
    String databaseName = hwcIdentifier.namespace()[0];
    String tableName = hwcIdentifier.name();
    try (Connection connection = getConnection(hwcIdentifier.getOptions())) {
      return DefaultJDBCWrapper.tableExists(connection, databaseName, tableName);
    } catch (Exception exception) {
      throw new RuntimeException(exception.getMessage(), exception);
    }
  }

  @Override
  public Table loadTable(Identifier identifier) throws NoSuchTableException {
    HWCIdentifier hwcIdentifier = resolveIdentifier(identifier, null);
    String databaseName = hwcIdentifier.namespace()[0];
    String tableName = hwcIdentifier.getOptions().getOrDefault("table", null);
    StructType schema = null;
    if (tableName != null) {
      String fullyQualifiedTableName =
          SchemaUtil.getDbTableNames(databaseName, tableName).getFullyQualifiedName();
      schema = resolveSchemaFromHiveAcidMetadata(fullyQualifiedTableName);
    }
    return new HiveWarehouseConnector(hwcIdentifier.getOptions(), schema);
  }

  @Override
  public Identifier[] listTables(String[] namespace) throws NoSuchNamespaceException {
    throw new UnsupportedOperationException(
        "Use hive.showTables to list all tables present in a database");
  }

  @Override
  public void renameTable(Identifier oldIdentifier, Identifier newIdentifier)
      throws NoSuchTableException, TableAlreadyExistsException {
    throw new UnsupportedOperationException("Use hive.executeUpdate to rename a table");
  }

  @Override
  public String[] defaultNamespace() {
    return new String[] {"default"};
  }

  @Override
  public void initialize(String name, CaseInsensitiveStringMap options) {
    // No-op. HWC catalog is configured via Spark session properties.
  }

  @Override
  public String name() {
    return HWC_CATALOG_NAME;
  }

  @Override
  public boolean dropTable(Identifier identifier) {
    HWCIdentifier hwcIdentifier = resolveIdentifier(identifier, null);
    String databaseName = hwcIdentifier.namespace()[0];
    String tableName = hwcIdentifier.name();
    try (Connection connection = getConnection(hwcIdentifier.getOptions())) {
      LOG.info("Dropping table: {}.{}", databaseName, tableName);
      boolean dropped = DefaultJDBCWrapper.dropTable(connection, databaseName, tableName, true);
      if (dropped) {
        HiveWarehouseConnector.removeHWCId(identifier, hwcIdentifier);
      }
      return dropped;
    } catch (Exception exception) {
      throw new RuntimeException(exception.getMessage(), exception);
    }
  }

  @Override
  public Table alterTable(Identifier identifier, TableChange... changes) throws NoSuchTableException {
    throw new UnsupportedOperationException("Use hive.executeUpdate to alter a table");
  }

  private Connection getConnection(Map<String, String> options) {
    // Ensure the Spark Hive driver is used for catalog operations.
    JobUtil.replaceSparkHiveDriver();
    return QueryExecutionUtil.getConnection(options);
  }

  /**
   * Resolves the HWC identifier used by catalog callbacks.
   *
   * <p>Spark does not always pass back the original {@link HWCIdentifier} instance that was
   * created through {@link HiveWarehouseConnector#extractIdentifier(CaseInsensitiveStringMap)}.
   * On some write paths Spark reconstructs a plain {@link Identifier}, which means the internal
   * identifier cache cannot be used directly. In that case, rebuild the minimum HWC options map
   * from the callback identifier and the active session's HWC connection settings so subsequent
   * catalog operations still have database, table, and JDBC context.
   *
   * @param identifier Spark catalog identifier received by the callback
   * @param properties optional table properties supplied by Spark during table creation
   * @return an {@link HWCIdentifier} suitable for HWC catalog operations
   */
  private HWCIdentifier resolveIdentifier(Identifier identifier, Map<String, String> properties) {
    HWCIdentifier hwcIdentifier = HiveWarehouseConnector.convertToHWCIdentifier(identifier);
    if (hwcIdentifier != null) {
      return hwcIdentifier;
    }

    Map<String, String> options = new HashMap<>();
    if (properties != null) {
      options.putAll(properties);
    }

    String[] namespace = identifier.namespace();
    if (namespace != null && namespace.length > 0) {
      options.putIfAbsent("database", namespace[0]);
    }
    options.putIfAbsent("table", identifier.name());

    SparkSession sparkSession = SparkSession.active();
    copySessionOptionIfPresent(sparkSession, options, HWConf.DEFAULT_DB);
    copySessionOptionIfPresent(sparkSession, options, HWConf.RESOLVED_HS2_URL);
    copySessionOptionIfPresent(sparkSession, options, HWConf.USER);
    copySessionOptionIfPresent(sparkSession, options, HWConf.PASSWORD);
    copySessionOptionIfPresent(sparkSession, options, HWConf.DBCP2_CONF);

    hwcIdentifier = new HWCIdentifier(options);
    LOG.debug("Reconstructed HWC identifier for catalog callback: {} -> {}", identifier, options);
    return hwcIdentifier;
  }

  /**
   * Copies an HWC session-scoped configuration value into the connector options map when present.
   *
   * <p>The DataSource V2 catalog callbacks run with an {@link Identifier} and optional properties,
   * but they do not automatically carry the session's HWC JDBC settings. This helper bridges that
   * gap by reusing the active session configuration only when the option was not already supplied
   * by Spark.
   *
   * @param sparkSession active Spark session
   * @param options mutable HWC options map being reconstructed
   * @param confKey HWC configuration key to copy
   */
  private void copySessionOptionIfPresent(SparkSession sparkSession,
                                          Map<String, String> options,
                                          HWConf confKey) {
    String qualifiedKey = confKey.getQualifiedKey();
    if (sparkSession.conf().contains(qualifiedKey)) {
      options.putIfAbsent(confKey.getSimpleKey(), sparkSession.conf().get(qualifiedKey));
    }
  }

  private StructType resolveSchemaFromHiveAcidMetadata(String fullyQualifiedTableName) {
    try {
      Class<?> metadataClass = Class.forName("com.qubole.spark.hiveacid.hive.HiveAcidMetadata");
      Object metadata = metadataClass
          .getMethod("fromSparkSession", SparkSession.class, String.class)
          .invoke(null, SparkSession.active(), fullyQualifiedTableName);
      return (StructType) metadataClass.getMethod("tableSchema").invoke(metadata);
    } catch (ClassNotFoundException exception) {
      LOG.debug("HiveAcidMetadata not found on classpath; returning null schema.");
      return null;
    } catch (Exception exception) {
      throw new RuntimeException("Failed to resolve schema from HiveAcidMetadata", exception);
    }
  }
}
