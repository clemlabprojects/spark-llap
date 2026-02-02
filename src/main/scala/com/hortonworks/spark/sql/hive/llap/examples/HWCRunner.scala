package com.hortonworks.spark.sql.hive.llap.examples

import java.util.HashMap

import com.google.common.base.Preconditions
import com.hortonworks.hwc.HiveWarehouseSession
import com.hortonworks.spark.sql.hive.llap.HiveWarehouseSessionImpl
import com.hortonworks.spark.sql.hive.llap.CreateTableBuilder
import com.hortonworks.spark.sql.hive.llap.util.{HWCOptions, SchemaUtil}
import com.hortonworks.spark.sql.hive.llap.HiveWarehouseDataWriterHelper
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StructType}

object HWCRunner extends Logging {
  val hwc_examples_test_db: String = "hwc_examples_test_db"
  val hive_table_batch_test: String = "hive_table_batch_test"
  val spark: SparkSession = SparkSession.builder().appName("HWCRunner").getOrCreate()
  val hwc: HiveWarehouseSessionImpl = HiveWarehouseSession.session(spark).build()

  def main(args: Array[String]): Unit = {
    setUpDB()
    testWithTryCatch(() => testBatchReadAndWrite(), "testBatchReadAndWrite")
    testWithTryCatch(() => testStreamingWriteAndRead(), "testStreamingWriteAndRead")
    hwc.close()
    spark.stop()
  }

  def testWithTryCatch(testMethod: () => Unit, testName: String): Unit = {
    logInfo(s"==========STARTING test: $testName")
    try {
      testMethod()
    } catch {
      case e: Throwable =>
        logError(s"Error while running test: $testName", e)
    }
    logInfo(s"==========FINISHED test: $testName")
  }

  private def setUpDB(): Unit = {
    hwc.dropDatabase(hwc_examples_test_db, true, true)
    hwc.createDatabase(hwc_examples_test_db, false)
    logInfo(s"Created database..$hwc_examples_test_db")
    hwc.setDatabase(hwc_examples_test_db)
    spark.sql(s"use $hwc_examples_test_db")
  }

  def buildHiveCreateTableQueryFromSparkDFSchema(
      schema: StructType,
      database: String,
      table: String): String = {
    val builder = new CreateTableBuilder(
      null,
      database,
      table,
      HiveWarehouseDataWriterHelper.FileFormat.ORC,
      new HWCOptions(new HashMap[String, String]()))
    schema.fields.foreach { field =>
      builder.column(field.name, SchemaUtil.getHiveType(field.dataType, field.metadata))
    }
    builder.prop("transactional", "true")
    builder.toString
  }

  private def testBatchReadAndWrite(): Unit = {
    val numRows = 1000
    val numCols = 100
    val df = getDataFrame(numRows, numCols)
    logInfo(s"Generated dataframe ${df.schema}, writing to hive table now")
    val createTableQuery = buildHiveCreateTableQueryFromSparkDFSchema(
      df.schema,
      hwc_examples_test_db,
      hive_table_batch_test)
    hwc.executeUpdate(createTableQuery)
    df.write
      .format("com.hortonworks.spark.sql.hive.llap.HiveWarehouseConnector")
      .mode("overwrite")
      .option("table", hive_table_batch_test)
      .save()
    logInfo("Successfully written to hive table, reading the same now...")
    val hiveDf = hwc.sql(s"select * from $hive_table_batch_test")
    hiveDf.cache()
    logInfo("=======df read from hive table=========")
    hiveDf.show(false)
    Preconditions.checkState(hiveDf.count() == numRows)
  }

  private def getDataFrame(numRows: Int, numCols: Int): Dataset[Row] = {
    val schema = (1 to numCols).foldLeft(new StructType()) { (s, i) =>
      s.add(s"c$i", DoubleType, nullable = true)
    }
    val rows = spark.sparkContext.parallelize(1 to numRows).map { _ =>
      Row.fromSeq((1 to numCols).map(_.toDouble))
    }
    spark.createDataFrame(rows, schema)
  }

  private def testStreamingWriteAndRead(): Unit = {
    val rateDF = spark.readStream.format("rate").option("rowsPerSecond", 1L).load()
    val hive_table_streaming_test = "spark_rate_source"
    hwc.executeUpdate(
      s"create table $hwc_examples_test_db.$hive_table_streaming_test " +
        " (`timestamp` string, `value` bigint)" +
        " STORED AS ORC TBLPROPERTIES ('transactional'='true')",
      true)

    val minStreamingDurationSecs = 10
    val durationSecs = spark.conf.getOption("hwc.test.streaming.duration.secs")
      .flatMap(s => scala.util.Try(s.toInt).toOption)
      .filter(_ > minStreamingDurationSecs)
      .getOrElse(minStreamingDurationSecs)

    logInfo(s"Starting streaming query with: streamingQueryDuration = $durationSecs secs")
    logInfo(s"Using metastore uris ${spark.conf.get("spark.hadoop.hive.metastore.uris")}")

    val baseWriter = rateDF.writeStream
      .format("com.hortonworks.spark.sql.hive.llap.streaming.HiveStreamingDataSource")
      .outputMode("append")
      .option("metastoreUri", spark.conf.get("spark.hadoop.hive.metastore.uris"))
      .option("table", hive_table_streaming_test)
      .option("database", hwc_examples_test_db)

    val writer = spark.conf.getOption("spark.hadoop.hive.metastore.kerberos.principal") match {
      case Some(principal) => baseWriter.option("metastoreKrbPrincipal", principal)
      case None => baseWriter
    }

    val streamingQuery = writer.start()
    Thread.sleep(durationSecs * 1000L)
    logInfo("=====Stopping streaming query...")
    streamingQuery.stop()

    val streamingSink = hwc.sql(s"select * from $hwc_examples_test_db.$hive_table_streaming_test")
    logInfo("=====Records in streaming table=======")
    streamingSink.show(false)
  }
}
