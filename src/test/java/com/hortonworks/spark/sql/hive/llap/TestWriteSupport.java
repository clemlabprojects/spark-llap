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

import com.hortonworks.spark.sql.hive.llap.common.DriverResultSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.catalyst.InternalRow;
import org.junit.Test;

import static com.hortonworks.spark.sql.hive.llap.TestSecureHS2Url.TEST_HS2_URL;
import static org.junit.Assert.assertEquals;

/**
 * Tests write support for the mock connector.
 */
public class TestWriteSupport extends SessionTestBase {

  /**
   * Verifies that writes are captured in the mock output buffer.
   */
  @Test
  public void testWriteSupport() {
    HiveWarehouseSession warehouseSession = HiveWarehouseBuilder
        .session(sparkSession)
        .hs2url(TEST_HS2_URL)
        .build();
    HiveWarehouseSessionImpl sessionImpl = (HiveWarehouseSessionImpl) warehouseSession;
    sessionImpl.HIVE_WAREHOUSE_CONNECTOR_INTERNAL = "com.hortonworks.spark.sql.hive.llap.MockHiveWarehouseConnector";
    DriverResultSet inputResultSet = MockHiveWarehouseSessionImpl.testFixture();
    Dataset dataFrame = inputResultSet.asDataFrame(sparkSession);
    dataFrame.write().format(sessionImpl.HIVE_WAREHOUSE_CONNECTOR_INTERNAL).mode("append").save();
    List<InternalRow> writtenRows =
      (List<InternalRow>) MockHiveWarehouseConnector.WRITE_OUTPUT_BUFFER.get("TestWriteSupport");
    Map<Integer, String> unorderedRowMap = new HashMap<>();
    for (int i = 0; i < writtenRows.size(); i++) {
      InternalRow currentRow = writtenRows.get(i);
      unorderedRowMap.put(currentRow.getInt(0), currentRow.getString(1));
    }
    assertEquals(unorderedRowMap.get(1), "ID 1");
    assertEquals(unorderedRowMap.get(2), "ID 2");
  }
}
