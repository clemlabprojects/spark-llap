package com.hortonworks.spark.sql.hive.llap

import java.io.File
import java.net.ServerSocket
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path => NioPath}
import java.security.PrivilegedExceptionAction
import java.sql.{Connection, DriverManager}
import java.util.Properties

import scala.io.Source
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.hadoop.minikdc.MiniKdc
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.server.MiniYARNCluster
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hive.service.server.HiveServer2

class LocalYarnHiveMiniCluster(enableYarn: Boolean, enableKerberos: Boolean) {
  private var baseDir: NioPath = _
  private var hdfsCluster: MiniDFSCluster = _
  private var yarnCluster: MiniYARNCluster = _
  private var hiveServer2: HiveServer2 = _
  private var hiveConf: HiveConf = _
  private var hs2Port: Int = _
  private var kdc: MiniKdc = _
  private var keytab: File = _
  private var servicePrincipal: String = _
  private var clientPrincipal: String = _

  def start(): Unit = {
    baseDir = Files.createTempDirectory("hwc-it-")

    System.setProperty("datanucleus.schema.autoCreateAll", "true")
    System.setProperty("datanucleus.schema.autoCreateTables", "true")
    System.setProperty("datanucleus.schema.autoCreateColumns", "true")
    System.setProperty("datanucleus.schema.autoCreateSchema", "true")
    System.setProperty("datanucleus.schema.validateTables", "false")
    System.setProperty("datanucleus.schema.validateColumns", "false")
    System.setProperty("datanucleus.schema.validateConstraints", "false")

    val hadoopConf = new Configuration()
    hadoopConf.set("hadoop.tmp.dir", baseDir.resolve("hadoop-tmp").toString)

    if (enableKerberos) {
      hadoopConf.set("hadoop.security.authentication", "kerberos")
      UserGroupInformation.setConfiguration(hadoopConf)
      initKdc()
    } else {
      hadoopConf.set("hadoop.security.authentication", "simple")
      UserGroupInformation.setConfiguration(hadoopConf)
    }

    var warehousePath: String = baseDir.resolve("warehouse").toString

    if (enableYarn) {
      hadoopConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.resolve("hdfs").toString)
      hdfsCluster = new MiniDFSCluster.Builder(hadoopConf).numDataNodes(1).build()
      hdfsCluster.waitClusterUp()
      val fs = hdfsCluster.getFileSystem
      warehousePath = new Path("/warehouse").toString
      fs.mkdirs(new Path(warehousePath))
      hadoopConf.set("fs.defaultFS", fs.getUri.toString)

      val yarnConf = new YarnConfiguration(hadoopConf)
      yarnConf.set("yarn.nodemanager.aux-services", "mapreduce_shuffle")
      yarnConf.set("yarn.nodemanager.aux-services.mapreduce_shuffle.class", "org.apache.hadoop.mapred.ShuffleHandler")
      val nmLocalDir = baseDir.resolve("yarn-nm-local")
      val nmLogDir = baseDir.resolve("yarn-nm-log")
      Files.createDirectories(nmLocalDir)
      Files.createDirectories(nmLogDir)
      yarnConf.set("yarn.nodemanager.local-dirs", nmLocalDir.toString)
      yarnConf.set("yarn.nodemanager.log-dirs", nmLogDir.toString)
      yarnConf.set("yarn.nodemanager.disk-health-checker.max-disk-utilization-per-disk-percentage", "99.0")
      yarnConf.set("yarn.nodemanager.disk-health-checker.min-free-space-per-disk-mb", "0")
      yarnConf.set("yarn.nodemanager.disk-health-checker.min-healthy-disks", "0")
      yarnCluster = new MiniYARNCluster("hwc-it", 1, 1, 1)
      yarnCluster.init(yarnConf)
      yarnCluster.start()
    }

    hs2Port = freePort()
    hiveConf = new HiveConf(hadoopConf, classOf[HiveConf])
    hiveConf.set("hive.server2.thrift.port", hs2Port.toString)
    hiveConf.set("hive.server2.thrift.bind.host", "localhost")
    hiveConf.set("hive.server2.transport.mode", "binary")
    hiveConf.set("hive.server2.enable.doAs", "false")
    hiveConf.set("hive.notification.event.poll.interval", "0s")
    hiveConf.set("hive.notification.event.consumers", "")
    hiveConf.set("hive.scheduled.queries.executor.enabled", "false")
    hiveConf.set("hive.metastore.uris", "")
    hiveConf.set("hive.metastore.warehouse.dir", warehousePath)
    hiveConf.set("hive.exec.scratchdir", baseDir.resolve("scratch").toString)
    val metastoreUrl = s"jdbc:derby:;databaseName=${baseDir.resolve("metastore_db")};create=true"
    System.setProperty("javax.jdo.option.ConnectionURL", metastoreUrl)
    System.setProperty("javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver")
    System.setProperty("javax.jdo.option.ConnectionUserName", "APP")
    hiveConf.set("javax.jdo.option.ConnectionURL", metastoreUrl)
    hiveConf.set("javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver")
    hiveConf.set("datanucleus.schema.autoCreateAll", "true")
    hiveConf.set("datanucleus.schema.autoCreateTables", "true")
    hiveConf.set("datanucleus.schema.autoCreateColumns", "true")
    hiveConf.set("datanucleus.schema.autoCreateSchema", "true")
    hiveConf.set("hive.metastore.schema.verification", "false")
    hiveConf.set("hive.metastore.schema.verification.record.version", "false")

    if (enableKerberos) {
      hiveConf.set("hive.server2.authentication", "KERBEROS")
      hiveConf.set("hive.server2.authentication.kerberos.principal", servicePrincipal)
      hiveConf.set("hive.server2.authentication.kerberos.keytab", keytab.getAbsolutePath)
      hiveConf.set("hive.metastore.sasl.enabled", "false")
    }

    initMetastoreSchema(metastoreUrl)

    hiveServer2 = new HiveServer2()
    hiveServer2.init(hiveConf)
    hiveServer2.start()

    waitForPort(hs2Port)
  }

  def stop(): Unit = {
    if (hiveServer2 != null) {
      try hiveServer2.stop() catch { case NonFatal(_) => }
    }
    if (yarnCluster != null) {
      try yarnCluster.stop() catch { case NonFatal(_) => }
    }
    if (hdfsCluster != null) {
      try hdfsCluster.shutdown(true) catch { case NonFatal(_) => }
    }
    if (kdc != null) {
      try kdc.stop() catch { case NonFatal(_) => }
    }
  }

  def jdbcUrl: String = {
    if (enableKerberos) {
      s"jdbc:hive2://localhost:$hs2Port/default;principal=$servicePrincipal"
    } else {
      s"jdbc:hive2://localhost:$hs2Port/default"
    }
  }

  def openJdbc(): Connection = {
    Class.forName("org.apache.hive.jdbc.HiveDriver")
    if (!enableKerberos) {
      DriverManager.getConnection(jdbcUrl, new Properties())
    } else {
      val ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(clientPrincipal, keytab.getAbsolutePath)
      ugi.doAs(new PrivilegedExceptionAction[Connection]() {
        override def run(): Connection = DriverManager.getConnection(jdbcUrl, new Properties())
      })
    }
  }

  private def initKdc(): Unit = {
    val kdcDir = baseDir.resolve("kdc").toFile
    val conf = MiniKdc.createConf()
    kdc = new MiniKdc(conf, kdcDir)
    kdc.start()

    servicePrincipal = s"hive/localhost@${kdc.getRealm}"
    clientPrincipal = s"testuser@${kdc.getRealm}"
    keytab = new File(kdcDir, "hive.keytab")
    kdc.createPrincipal(keytab, servicePrincipal, clientPrincipal)

    System.setProperty("java.security.krb5.conf", kdc.getKrb5conf.getAbsolutePath)
    System.setProperty("javax.security.auth.useSubjectCredsOnly", "false")
  }

  private def freePort(): Int = {
    val socket = new ServerSocket(0)
    try socket.getLocalPort finally socket.close()
  }

  private def initMetastoreSchema(jdbcUrl: String): Unit = {
    Class.forName("org.apache.derby.jdbc.EmbeddedDriver")
    val conn = DriverManager.getConnection(jdbcUrl)
    try {
      val meta = conn.getMetaData
      val tables = meta.getTables(null, "APP", "CTLGS", null)
      val exists = try tables.next() finally tables.close()
      if (!exists) {
        val stream = Option(getClass.getClassLoader.getResourceAsStream(
          "metastore/hive-schema-4.0.0.derby.sql"
        )).getOrElse {
          throw new IllegalStateException("Missing metastore schema resource")
        }
        val source = Source.fromInputStream(stream, StandardCharsets.UTF_8.name())
        try {
          val sql = source.getLines()
            .filterNot(line => line.trim.startsWith("--"))
            .mkString("\n")
          sql.split(";").map(_.trim).filter(_.nonEmpty).foreach { stmt =>
            val statement = conn.createStatement()
            try statement.execute(stmt) finally statement.close()
          }
        } finally {
          source.close()
          stream.close()
        }
      }
    } finally {
      conn.close()
    }
  }

  private def waitForPort(port: Int): Unit = {
    val deadline = System.currentTimeMillis() + 60000
    var connected = false
    while (!connected && System.currentTimeMillis() < deadline) {
      try {
        val socket = new java.net.Socket("localhost", port)
        socket.close()
        connected = true
      } catch {
        case NonFatal(_) => Thread.sleep(200)
      }
    }
    if (!connected) {
      throw new IllegalStateException(s"HiveServer2 did not start on port $port")
    }
  }
}
