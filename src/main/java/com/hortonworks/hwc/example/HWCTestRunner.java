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

package com.hortonworks.hwc.example;

import com.hortonworks.hwc.HiveWarehouseSession;
import com.hortonworks.spark.sql.hive.llap.HiveWarehouseSessionImpl;
import org.apache.spark.sql.SparkSession;

/**
 * Minimal example runner that validates the Hive Warehouse Connector can connect to Hive
 * and list databases.
 */
public class HWCTestRunner {
  /**
   * Creates a Spark session, builds a Hive Warehouse session, and prints databases.
   *
   * @param args command line arguments (unused)
   */
  public static void main(String[] args) {
    SparkSession sparkSession = SparkSession.builder()
        .appName(HWCTestRunner.class.getName())
        .getOrCreate();

    // Build the Hive Warehouse session used for Hive metadata operations.
    HiveWarehouseSessionImpl hiveWarehouseSession = HiveWarehouseSession.session(sparkSession).build();

    // List databases to validate connectivity.
    System.out.println("========Showing databases========");
    hiveWarehouseSession.showDatabases().show(false);

    // Stop the Spark session to release resources.
    sparkSession.stop();
  }
}
