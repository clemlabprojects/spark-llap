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

import com.hortonworks.spark.sql.hive.llap.catalog.HWCCatalog;
import com.hortonworks.spark.sql.hive.llap.catalog.HWCIdentifier;
import com.hortonworks.spark.sql.hive.llap.common.StatementType;
import com.hortonworks.spark.sql.hive.llap.util.HiveQlUtil;
import com.hortonworks.spark.sql.hive.llap.util.JobUtil;
import com.hortonworks.spark.sql.hive.llap.util.QueryExecutionUtil;
import com.hortonworks.spark.sql.hive.llap.util.SchemaUtil;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.SessionConfigSupport;
import org.apache.spark.sql.connector.catalog.SupportsCatalogOptions;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Spark DataSource V2 table implementation for the Hive Warehouse Connector.
 */
public class HiveWarehouseConnector implements Table,
  SessionConfigSupport,
  SupportsRead,
  SupportsWrite,
  SupportsCatalogOptions {

  // Tracks identifiers created through the catalog to allow later conversion.
  private static final Map<IdentifierLookupKey, HWCIdentifier> identifierToHwcIdentifierMap =
    Collections.synchronizedMap(new HashMap<>());
  private static final Logger LOG = LoggerFactory.getLogger(HiveWarehouseConnector.class);

  protected Map<String, String> optionsByName;
  protected StructType tableSchema;

  /**
   * Creates a connector with no options or schema configured.
   */
  public HiveWarehouseConnector() {
    this(null, null);
  }

  /**
   * Creates a connector with options but no schema configured.
   *
   * @param optionsByName connector options
   */
  public HiveWarehouseConnector(Map<String, String> optionsByName) {
    this(optionsByName, null);
  }

  /**
   * Creates a connector with options and schema configured.
   *
   * @param optionsByName connector options
   * @param tableSchema resolved table schema
   */
  public HiveWarehouseConnector(Map<String, String> optionsByName, StructType tableSchema) {
    this.optionsByName = optionsByName;
    this.tableSchema = tableSchema;
  }

  /**
   * Infers the table schema using the provided options.
   *
   * @param optionsMap DataSource options
   * @return inferred schema
   */
  @Override
  public StructType inferSchema(CaseInsensitiveStringMap optionsMap) {
    this.optionsByName = optionsMap.asCaseSensitiveMap();
    try {
      this.tableSchema = getTableSchema();
      return this.tableSchema;
    } catch (Exception exception) {
      throw new RuntimeException(exception.getMessage(), exception);
    }
  }

  /**
   * Returns this instance configured with the provided schema and properties.
   *
   * @param schema inferred schema
   * @param partitions partition transforms (unused)
   * @param properties DataSource properties
   * @return this connector instance
   */
  @Override
  public Table getTable(StructType schema, Transform[] partitions, Map<String, String> properties) {
    this.tableSchema = schema;
    this.optionsByName = properties;
    return this;
  }

  /**
   * @return connector name for Spark
   */
  @Override
  public String name() {
    return getClass().toString();
  }

  /**
   * Returns the schema, resolving it from HS2 if needed.
   *
   * @return table schema
   */
  @Override
  public StructType schema() {
    try {
      if (tableSchema == null) {
        tableSchema = getTableSchema();
      }
      return tableSchema;
    } catch (Exception exception) {
      throw new RuntimeException(exception.getMessage(), exception);
    }
  }

  /**
   * @return supported table capabilities for the connector
   */
  @Override
  public Set<TableCapability> capabilities() {
    Set<TableCapability> capabilities = new HashSet<>();
    capabilities.add(TableCapability.BATCH_READ);
    capabilities.add(TableCapability.BATCH_WRITE);
    capabilities.add(TableCapability.TRUNCATE);
    return capabilities;
  }

  /**
   * Creates a scan builder for a JDBC-backed read.
   *
   * @param options read options
   * @return scan builder
   */
  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    return new JdbcDataSourceReader(tableSchema, options.asCaseSensitiveMap());
  }

  /**
   * @return the required key prefix for session config support
   */
  @Override
  public String keyPrefix() {
    return "hive.warehouse";
  }

  /**
   * Resolves the statement type from the configured options.
   *
   * @return statement type derived from options
   */
  private StatementType resolveQueryType() {
    return StatementType.fromOptions(optionsByName);
  }

  /**
   * Resolves the table schema using HS2, based on the configured options.
   *
   * @return resolved schema
   * @throws Exception when schema resolution fails
   */
  private StructType getTableSchema() throws Exception {
    JobUtil.replaceSparkHiveDriver();
    StatementType queryStatementType = resolveQueryType();
    String queryText;
    if (queryStatementType == StatementType.FULL_TABLE_SCAN) {
      String databaseName = optionsByName.getOrDefault("database", "default");
      SchemaUtil.TableRef tableRef = SchemaUtil.getDbTableNames(databaseName, optionsByName.get("table"));
      queryText = HiveQlUtil.selectStar(tableRef.databaseName, tableRef.tableName);
    } else {
      queryText = optionsByName.get("query");
    }

    try (Connection connection = QueryExecutionUtil.getConnection(optionsByName)) {
      JobUtil.replaceSparkHiveDriver();
      return DefaultJDBCWrapper.resolveQuery(
        connection,
        HWConf.DEFAULT_DB.getFromOptionsMap(optionsByName),
        queryText
      );
    } catch (SQLException exception) {
      LOG.error("Failed to connect to HS2", exception);
      throw new RuntimeException(exception.getMessage(), exception);
    }
  }

  /**
   * Updates connector options after construction.
   *
   * @param optionsByName connector options
   */
  public void setOptions(Map<String, String> optionsByName) {
    this.optionsByName = optionsByName;
  }

  /**
   * Creates a write builder for the connector.
   *
   * @param info logical write info
   * @return write builder
   */
  @Override
  public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
    return new HiveWarehouseWriterBuilder(info);
  }

  /**
   * Extracts the catalog name and initializes the HWC catalog if needed.
   *
   * @param options DataSource options
   * @return catalog name for Spark to use
   */
  @Override
  public String extractCatalog(CaseInsensitiveStringMap options) {
    HWCCatalog.setupHWCCatalog();
    return "hwc";
  }

  /**
   * Creates an identifier from options and caches it for later conversion.
   *
   * @param options DataSource options
   * @return identifier instance
   */
  @Override
  public Identifier extractIdentifier(CaseInsensitiveStringMap options) {
    HWCIdentifier identifier = new HWCIdentifier(options.asCaseSensitiveMap());
    addHwcIdentifier(identifier);
    return identifier;
  }

  private static void addHwcIdentifier(HWCIdentifier identifier) {
    IdentifierLookupKey lookupKey = new IdentifierLookupKey(identifier);
    synchronized (identifierToHwcIdentifierMap) {
      identifierToHwcIdentifierMap.put(lookupKey, identifier);
    }
  }

  /**
   * Converts a Spark identifier to an HWC identifier, if possible.
   *
   * @param identifier Spark catalog identifier
   * @return HWC identifier or null if not found
   */
  public static HWCIdentifier convertToHWCIdentifier(Identifier identifier) {
    IdentifierLookupKey lookupKey = new IdentifierLookupKey(identifier);
    HWCIdentifier hwcIdentifier = identifierToHwcIdentifierMap.get(lookupKey);
    if (hwcIdentifier == null && identifier instanceof HWCIdentifier) {
      hwcIdentifier = (HWCIdentifier) identifier;
      addHwcIdentifier(hwcIdentifier);
    }
    return hwcIdentifier;
  }

  /**
   * Removes an identifier mapping if it matches the provided HWC identifier.
   *
   * @param identifier Spark identifier
   * @param identifierToRemove HWC identifier to remove
   */
  public static void removeHWCId(Identifier identifier, HWCIdentifier identifierToRemove) {
    IdentifierLookupKey lookupKey = new IdentifierLookupKey(identifier);
    synchronized (identifierToHwcIdentifierMap) {
      if (Objects.equals(identifierToHwcIdentifierMap.get(lookupKey), identifierToRemove)) {
        identifierToHwcIdentifierMap.remove(lookupKey);
      }
    }
  }

  /**
   * Identifier wrapper that ensures consistent lookup behavior.
   */
  public static class IdentifierLookupKey {
    private final Identifier catalogIdentifier;

    /**
     * Creates a lookup key.
     *
     * @param catalogIdentifier Spark identifier
     */
    public IdentifierLookupKey(Identifier catalogIdentifier) {
      this.catalogIdentifier = catalogIdentifier;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof IdentifierLookupKey)) {
        return false;
      }
      IdentifierLookupKey other = (IdentifierLookupKey) obj;
      return Objects.equals(catalogIdentifier.toString(), other.catalogIdentifier.toString());
    }

    @Override
    public int hashCode() {
      return catalogIdentifier.toString().hashCode();
    }
  }
}
