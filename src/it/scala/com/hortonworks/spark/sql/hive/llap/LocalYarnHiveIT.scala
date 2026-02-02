package com.hortonworks.spark.sql.hive.llap

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll

class LocalYarnHiveIT extends AnyFunSuite with BeforeAndAfterAll {
  private var cluster: LocalYarnHiveMiniCluster = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    if (sys.props.get("skipIT").exists(_.toBoolean)) {
      cancel("skipIT=true")
    }
    cluster = new LocalYarnHiveMiniCluster(enableYarn = true, enableKerberos = false)
    cluster.start()
  }

  override protected def afterAll(): Unit = {
    if (cluster != null) {
      cluster.stop()
      cluster = null
    }
    super.afterAll()
  }

  test("hiveserver2 jdbc roundtrip") {
    val conn = cluster.openJdbc()
    try {
      val stmt = conn.createStatement()
      stmt.execute("CREATE DATABASE IF NOT EXISTS it_db")
      stmt.execute("SHOW DATABASES")
    } finally {
      conn.close()
    }
  }

  test("hwc executeQuery against local hs2") {
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("hwc-it")
      .config("spark.sql.hive.hiveserver2.jdbc.url", cluster.jdbcUrl)
      .getOrCreate()

    val hive = com.hortonworks.hwc.HiveWarehouseSession.session(spark)
      .hs2url(cluster.jdbcUrl)
      .build()

    try {
      val rows = hive.executeQuery("select 1 as v").collect()
      assert(rows.length == 1)
      assert(rows(0).getInt(0) == 1)
    } finally {
      hive.close()
      spark.stop()
    }
  }
}
