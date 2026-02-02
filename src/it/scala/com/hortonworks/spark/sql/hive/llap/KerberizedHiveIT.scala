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
