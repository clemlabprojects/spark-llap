package com.hortonworks.spark.sql.rule

import java.util.Locale

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.NonFatal

import com.hortonworks.hwc.HiveWarehouseSession
import com.hortonworks.spark.sql.hive.llap.{HiveWarehouseConnector, HiveWarehouseSessionImpl, HWConf}
import com.hortonworks.spark.sql.hive.llap.catalog.HWCCatalog
import com.hortonworks.spark.sql.hive.llap.common.HwcSparkListener
import com.hortonworks.spark.sql.hive.llap.util.QueryExecutionUtil
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.{SparkSession, SQLContext}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, HiveTableRelation}
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoStatement, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.{Identifier, Table}
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.util.{CaseInsensitiveStringMap, QueryExecutionListener}

class HWCSwitchRule extends Rule[LogicalPlan] {
  private val ACCESSTYPE_NONE: Byte = 0
  private val ACCESSTYPE_READWRITE: Byte = 8
  private val logger: Logger = LogManager.getLogger(getClass)

  private def logInfo(msg: String): Unit = logger.info(msg)

  private def getAccessType(tableMeta: CatalogTable): Int = {
    try {
      val accessInfo = tableMeta.getClass.getMethod("accessInfo").invoke(tableMeta)
      val accessType = accessInfo.getClass.getMethod("accessType").invoke(accessInfo)
      accessType.asInstanceOf[java.lang.Number].intValue()
    } catch {
      case _: NoSuchMethodException =>
        logInfo("Unknown access type; Assuming READWRITE")
        ACCESSTYPE_READWRITE
      case NonFatal(_) =>
        logInfo("Unknown access type; Assuming READWRITE")
        ACCESSTYPE_READWRITE
    }
  }

  private def getSessionOpts(keyPrefix: String, spark: SparkSession): Map[String, String] = {
    val pattern = ("^spark\\.datasource\\." + keyPrefix + "\\.(.+)").r
    SQLConf.get.getAllConfs.flatMap {
      case (key, value) =>
        pattern.findFirstMatchIn(key).map(m => m.group(1) -> value)
    }
  }

  private def isConvertible(relation: HiveTableRelation): Boolean = {
    relation.tableMeta.properties.getOrElse("transactional", "false").toBoolean
  }

  private def isWriteEnabled(): Boolean = {
    val spark = SparkSession.getActiveSession.get
    val enabled = spark.sessionState.conf.getConfString(
      "tests.enable.write.via.direct.reader.mode", "false").equalsIgnoreCase("true")
    if (enabled) {
      logger.info("Write through spark-acid is enabled for testing only and is not recommended for production.")
    } else {
      logger.info("Write or Insert into table, through spark-acid is not supported please use HWC")
    }
    enabled
  }

  private def convert(relation: HiveTableRelation): LogicalPlan = {
    val spark = SparkSession.getActiveSession.get
    val identifier = TableIdentifier(relation.tableMeta.identifier.table,
      relation.tableMeta.identifier.database)
    val options = relation.tableMeta.properties ++ relation.tableMeta.storage.properties +
      ("table" -> identifier.unquotedString)

    try {
      val dsClass = Class.forName("com.qubole.spark.hiveacid.datasource.HiveAcidDataSource")
      val ds = dsClass.getConstructor().newInstance()
      val method = dsClass.getMethods.find(m => m.getName == "createRelation" && m.getParameterTypes.length == 2)
      method match {
        case Some(m) =>
          val baseRelation = m.invoke(ds, spark.sqlContext.asInstanceOf[SQLContext], options)
            .asInstanceOf[BaseRelation]
          LogicalRelation(baseRelation, isStreaming = false)
        case None =>
          relation
      }
    } catch {
      case _: ClassNotFoundException =>
        logger.debug("HiveAcidDataSource not found on classpath; skipping conversion.")
        relation
      case NonFatal(e) =>
        logger.warn("Failed to convert to HiveAcidDataSource; leaving plan unchanged.", e)
        relation
    }
  }

  private def convertV2(relation: HiveTableRelation): LogicalPlan = {
    val serde = relation.tableMeta.storage.serde.getOrElse("").toLowerCase(Locale.ROOT)
    if (serde != "org.apache.hadoop.hive.ql.io.orc.orcserde") {
      return convert(relation)
    }
    val dbName = relation.tableMeta.identifier.database.getOrElse("default")
    val tableName = relation.tableMeta.identifier.table
    val tableOpts = Map("database" -> dbName, "table" -> tableName)
    try {
      val dsClass = Class.forName("com.qubole.spark.hiveacid.datasource.HiveAcidDataSourceV2")
      val ctor = dsClass.getConstructor(classOf[String], classOf[String])
      val ds = ctor.newInstance(dbName, tableName).asInstanceOf[Table]
      DataSourceV2Relation.create(ds, None, None, new CaseInsensitiveStringMap(tableOpts.asJava))
    } catch {
      case _: ClassNotFoundException =>
        logger.debug("HiveAcidDataSourceV2 not found on classpath; falling back to v1 conversion.")
        convert(relation)
      case NonFatal(e) =>
        logger.warn("Failed to convert to HiveAcidDataSourceV2; leaving plan unchanged.", e)
        relation
    }
  }

  private def registerListener(): Unit = {
    val spark = SparkSession.active
    if (spark.sparkContext.getLocalProperty("hwc.listeners.registered") == null) {
      logger.info("Registering Listeners")
      spark.sparkContext.setLocalProperty("hwc.listeners.registered", "true")
      spark.sparkContext.addSparkListener(new HwcSparkListener())
      try {
        val cls = Class.forName("com.qubole.spark.hiveacid.transaction.SparkAcidQueryListener")
        val ctor = cls.getConstructor(classOf[SparkSession])
        val listener = ctor.newInstance(spark).asInstanceOf[QueryExecutionListener]
        spark.listenerManager.register(listener)
      } catch {
        case _: ClassNotFoundException =>
          logger.debug("SparkAcidQueryListener not found on classpath; skipping registration.")
        case NonFatal(e) =>
          logger.warn("Failed to register SparkAcidQueryListener", e)
      }
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    registerListener()
    plan.resolveOperators {
      case relation: HiveTableRelation =>
        val tableMeta = relation.tableMeta
        val dbName = tableMeta.identifier.database.getOrElse("default")
        val tableName = tableMeta.identifier.table

        if (getAccessType(tableMeta) == ACCESSTYPE_NONE &&
          HWConf.HWCReadMode.resolveReadMode(HWConf.HWCReadMode.JDBC_CLIENT)) {
          logInfo(s"using hwc extension for jdbc-client")
          val jdbcMode = HWConf.HWCReadMode.JDBC_CLIENT.name()
          logger.warn(s"JDBC mode is $jdbcMode; Not switching to HWC connector for table `${dbName}`.`${tableName}`")
          relation
        } else if (getAccessType(tableMeta) == ACCESSTYPE_NONE &&
          HWConf.HWCReadMode.resolveReadMode(HWConf.HWCReadMode.JDBC_CLUSTER)) {
          logInfo("using hwc extension for jdbc-cluster mode")
          val spark = SparkSession.getActiveSession.get
          val ds = new HiveWarehouseConnector()
          val keyPrefix = ds.keyPrefix()
          val sessionOpts = getSessionOpts(keyPrefix, spark)

          val hwcSession = HWCSwitchRule.getOrCreateSession(spark)
          val sessionId = hwcSession.getSessionId()
          logInfo(s"Hive Warehouse Connector session ID is $sessionId")

          val jdbcUrlForExecutor = hwcSession.getJdbcUrlForExecutor()
          val tableOpts = Map(
            "table" -> s"`${dbName}`.`${tableName}`",
            "hwc_session_id" -> sessionId,
            "hwc_query_mode" -> QueryExecutionUtil.ExecutionMethod.EXECUTE_HIVE_JDBC.name(),
            "hs2.jdbc.url.for.executor" -> jdbcUrlForExecutor)

          val options = Map("hwc_session_id" -> sessionId) ++ tableOpts ++ sessionOpts
          ds.setOptions(options.asJava)
          DataSourceV2Relation.create(
            ds,
            Some(new HWCCatalog()),
            Some(Identifier.of(Array(dbName), tableName)),
            new CaseInsensitiveStringMap(options.asJava))
        } else if (DDLUtils.isHiveTable(tableMeta) && isConvertible(relation) &&
          HWConf.HWCReadMode.resolveReadMode(HWConf.HWCReadMode.DIRECT_READER_V1)) {
          logInfo("using DIRECT_READER_V1 extension for reading")
          convert(relation)
        } else if (DDLUtils.isHiveTable(tableMeta) && isConvertible(relation) &&
          HWConf.HWCReadMode.resolveReadMode(HWConf.HWCReadMode.DIRECT_READER_V2)) {
          logInfo("using DIRECT_READER_V2 extension for reading")
          convertV2(relation)
        } else {
          relation
        }

      case insert: InsertIntoStatement =>
        insert.table match {
          case hiveRelation: HiveTableRelation
            if insert.query.resolved && DDLUtils.isHiveTable(hiveRelation.tableMeta) &&
              isConvertible(hiveRelation) && isWriteEnabled() &&
              HWConf.HWCReadMode.resolveReadMode(HWConf.HWCReadMode.DIRECT_READER_V1) =>
            logInfo("using spark acid extension for writing")
            insert.copy(table = convert(hiveRelation))
          case _ =>
            insert
        }
    }
  }
}

object HWCSwitchRule {
  private val hwcSessions = new mutable.HashMap[SparkSession, HiveWarehouseSessionImpl]()

  def getOrCreateSession(spark: SparkSession): HiveWarehouseSessionImpl = hwcSessions.synchronized {
    hwcSessions.getOrElseUpdate(spark, HiveWarehouseSession.session(spark).build())
  }
}
