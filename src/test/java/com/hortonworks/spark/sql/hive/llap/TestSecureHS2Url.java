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

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests secure HS2 URL resolution for different modes.
 */
public class TestSecureHS2Url extends SessionTestBase {

  static final String TEST_HS2_URL = "jdbc:hive2://example.com:10084";
  static final String TEST_PRINCIPAL = "testUser/_HOST@EXAMPLE.com";

  static final String KERBERIZED_CLUSTER_MODE_URL = TEST_HS2_URL + ";auth=delegationToken";
  static final String KERBERIZED_CLIENT_MODE_URL =
      TEST_HS2_URL +
          ";principal=" +
          TEST_PRINCIPAL;

  /**
   * Verifies cluster mode URL when credentials are enabled.
   */
  @Test
  public void kerberizedClusterMode() {
    HiveWarehouseSessionState sessionState = HiveWarehouseBuilder
        .session(sparkSession)
        .hs2url(TEST_HS2_URL)
        .credentialsEnabled()
        .build()
        .sessionState;

    String resolvedHs2Url = HWConf.RESOLVED_HS2_URL.getString(sessionState);
    assertEquals(resolvedHs2Url, KERBERIZED_CLUSTER_MODE_URL);
  }

  /**
   * Verifies client mode URL when principal is set.
   */
  @Test
  public void kerberizedClientMode() {
    sparkSession.conf().set("spark.security.credentials.hiveserver2.enabled", "false");
    HiveWarehouseSessionState sessionState = HiveWarehouseBuilder
        .session(sparkSession)
        .hs2url(TEST_HS2_URL)
        .principal(TEST_PRINCIPAL)
        .build()
        .sessionState;

    String resolvedHs2Url = HWConf.RESOLVED_HS2_URL.getString(sessionState);
    assertEquals(resolvedHs2Url, KERBERIZED_CLIENT_MODE_URL);
  }

  /**
   * Verifies URL resolution for non-kerberized configurations.
   */
  @Test
  public void nonKerberized() {
    sparkSession.conf().set("spark.security.credentials.hiveserver2.enabled", "false");
    HiveWarehouseSessionState sessionState = HiveWarehouseBuilder
        .session(sparkSession)
        .hs2url(TEST_HS2_URL)
        .build()
        .sessionState;

    String resolvedHs2Url = HWConf.RESOLVED_HS2_URL.getString(sessionState);
    assertEquals(resolvedHs2Url, TEST_HS2_URL);
  }
}
