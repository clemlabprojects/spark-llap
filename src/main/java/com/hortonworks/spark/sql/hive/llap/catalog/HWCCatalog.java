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

package com.hortonworks.spark.sql.hive.llap.catalog;

import com.hortonworks.spark.sql.hive.llap.DefaultJDBCWrapper;
import com.hortonworks.spark.sql.hive.llap.HiveWarehouseConnector;
import com.hortonworks.spark.sql.hive.llap.query.builder.DataWriteQueryBuilder;
import com.hortonworks.spark.sql.hive.llap.util.HWCOptions;
import com.hortonworks.spark.sql.hive.llap.util.JobUtil;
import com.hortonworks.spark.sql.hive.llap.util.QueryExecutionUtil;
import com.hortonworks.spark.sql.hive.llap.util.SchemaUtil;
import com.hortonworks.spark.sql.hive.llap.writers.HiveWarehouseDataWriterHelper;
import java.sql.Connection;
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
      "com.hortonworks.spark.sql.hive.llap.catalog.HWCCatalog";

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
    HWCIdentifier hwcIdentifier = HiveWarehouseConnector.convertToHWCIdentifier(identifier);
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
    HWCIdentifier hwcIdentifier = HiveWarehouseConnector.convertToHWCIdentifier(identifier);
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
    HWCIdentifier hwcIdentifier = HiveWarehouseConnector.convertToHWCIdentifier(identifier);
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
    HWCIdentifier hwcIdentifier = HiveWarehouseConnector.convertToHWCIdentifier(identifier);
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
