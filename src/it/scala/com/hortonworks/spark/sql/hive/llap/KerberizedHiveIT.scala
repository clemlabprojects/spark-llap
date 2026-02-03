/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hortonworks.spark.sql.hive.llap

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll

class KerberizedHiveIT extends AnyFunSuite with BeforeAndAfterAll {
  private var cluster: LocalYarnHiveMiniCluster = _
  private val kerbEnabled = sys.props.get("it.kerberos").contains("true") &&
    !sys.props.get("skipIT").exists(_.toBoolean)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    if (kerbEnabled) {
      cluster = new LocalYarnHiveMiniCluster(enableYarn = false, enableKerberos = true)
      cluster.start()
    }
  }

  override protected def afterAll(): Unit = {
    if (cluster != null) {
      cluster.stop()
      cluster = null
    }
    super.afterAll()
  }

  if (kerbEnabled) {
    test("kerberos hiveserver2 jdbc roundtrip") {
      val conn = cluster.openJdbc()
      try {
        val stmt = conn.createStatement()
        val rs = stmt.executeQuery("select 1")
        assert(rs.next())
        assert(rs.getInt(1) == 1)
      } finally {
        conn.close()
      }
    }
  } else {
    ignore("kerberos hiveserver2 jdbc roundtrip") { }
  }
}
