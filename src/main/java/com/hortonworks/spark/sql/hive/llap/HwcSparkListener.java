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

import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Spark listener that cleans up HWC resources when the application ends.
 */
public class HwcSparkListener extends SparkListener {
  private static final Logger LOG = LoggerFactory.getLogger(HwcSparkListener.class);

  /**
   * Closes open transactions, JDBC pools, and staging directories on application end.
   *
   * @param applicationEnd Spark application end event
   */
  @Override
  public void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {
    super.onApplicationEnd(applicationEnd);
    LOG.info("Spark onApplicationEnd event triggered, closing all resources");
    try {
      // Close all Hive ACID transactions if the optional manager is present.
      Class<?> transactionManagerClass =
          Class.forName("com.qubole.spark.hiveacid.transaction.HiveAcidTxnManagerObject");
      transactionManagerClass.getMethod("endAllTxn", long.class).invoke(null, -1L);
    } catch (Exception exception) {
      LOG.error("Unable to close all transactions managed by txnManager.", exception);
    }
    try {
      // Close pooled JDBC connections to HS2.
      DefaultJDBCWrapper.closeHS2ConnectionPool();
    } catch (Exception exception) {
      LOG.error("Unable to close Jdbc connections", exception);
    }
    try {
      // Clean secure-access staging output if present.
      SecureAccessModeExecutor.close();
    } catch (Exception exception) {
      throw new RuntimeException("Unable to delete staging output dir", exception);
    }
  }
}
