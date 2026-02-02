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

import com.google.common.collect.Lists;
import com.hortonworks.spark.sql.hive.llap.util.SchemaUtil;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import static com.hortonworks.spark.sql.hive.llap.HiveWarehouseBuilderTest.*;
import static com.hortonworks.spark.sql.hive.llap.TestSecureHS2Url.TEST_HS2_URL;
import static org.apache.spark.sql.types.DataTypes.*;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link SchemaUtil}.
 */
public class SchemaUtilTest extends SessionTestBase {

  /**
   * Verifies that a CREATE TABLE statement can be generated and executed for a Spark schema.
   */
  @Test
  public void testBuildHiveCreateTableQueryFromSparkDFSchema() {
    HiveWarehouseSessionState warehouseSessionState =
        HiveWarehouseBuilder
            .session(sparkSession)
            .userPassword(TEST_USER, TEST_PASSWORD)
            .hs2url(TEST_HS2_URL)
            .dbcp2Conf(TEST_DBCP2_CONF)
            .maxExecResults(TEST_EXEC_RESULTS_MAX)
            .defaultDB(TEST_DEFAULT_DB)
            .sessionStateForTest();
    HiveWarehouseSession warehouseSession = new MockHiveWarehouseSessionImpl(warehouseSessionState);

    // Route the internal connector to the mock implementation for tests.
    HiveWarehouseSessionImpl.HIVE_WAREHOUSE_CONNECTOR_INTERNAL =
      "com.hortonworks.spark.sql.hive.llap.MockHiveWarehouseConnector";

    StructType tableSchema = buildTestSchema();
    String createTableQuery = SchemaUtil.buildHiveCreateTableQueryFromSparkDFSchema(tableSchema, "testDB", "testTable");
    System.out.println("create table query:" + createTableQuery);
    assertTrue(warehouseSession.executeUpdate(createTableQuery));
  }

  private StructType buildTestSchema() {
    return (new StructType())
        .add("c1", ByteType)
        .add("c2", ShortType)
        .add("c3", IntegerType)
        .add("c4", LongType)
        .add("c5", FloatType)
        .add("c6", DoubleType)
        .add("c7", createDecimalType())
        .add("c8", StringType)
        .add("c9", StringType,
            true, new MetadataBuilder().putString("HIVE_TYPE_STRING", "char(20)").build())
        .add("c10", StringType,
            true, new MetadataBuilder().putString("HIVE_TYPE_STRING", "varchar(20)").build())
        .add("c11", BinaryType)
        .add("c12", BooleanType)
        .add("c13", TimestampType)
        .add("c14", DateType)
        .add("c15", createArrayType(StringType))
        .add("c16", createStructType(Lists.newArrayList(
            createStructField("f1", IntegerType, true),
            createStructField("f2", StringType, true))))
        .add("c17", createStructType(Lists.newArrayList(
            createStructField("f1",
                createStructType(Lists.newArrayList(
                    createStructField("f2", IntegerType, true))),
                true))));
  }
}
