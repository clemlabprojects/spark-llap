package com.hortonworks.spark.sql.hive.llap.streaming.examples

import java.io.{File, FileInputStream}
import java.util.Properties

import scala.collection.JavaConverters._

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object HiveStreamingExample {

  def main(args: Array[String]): Unit = {
    if (args.length != 4 && args.length != 6) {
      // scalastyle:off println
      System.err.println(s"Usage: HiveStreamingExample <bootstrap-servers> <properties-file> <topic-name>")
      System.err.println(s"Usage: HiveStreamingExample <bootstrap-servers> <properties-file> <topic-name> <database> <table>")
      System.err.println(
        "Kafka consumer properties such as security.protocol, ssl.truststore.location can be set in properties-file. " +
          "Value 'none' can be supplied if there are no properties to set.")
      // scalastyle:on println
      System.exit(1)
    }

    val bootstrapServers = args(0)
    val propertiesFileName = args(1)
    val topicName = args(2)
    val metastoreUri = args(3)

    val sparkConf = new SparkConf()
      .set("spark.sql.streaming.checkpointLocation", "./checkpoint")
    val sparkSession = SparkSession.builder()
      .appName("HiveStreamingExample")
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()

    val options = scala.collection.mutable.HashMap.empty[String, String]
    val skipValues = Set("none", "null", "empty", "undefined")
    if (!skipValues.contains(propertiesFileName.toLowerCase.trim)) {
      val props = new Properties()
      val fileStream = new FileInputStream(new File(propertiesFileName))
      try {
        props.load(fileStream)
      } finally {
        fileStream.close()
      }
      props.asScala.foreach { case (k, v) =>
        options.update(s"kafka.$k", v)
      }
    }

    val df = sparkSession.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("startingOffsets", "earliest")
      .option("subscribe", topicName)
      .options(options.toMap)
      .load()

    import sparkSession.implicits._

    val dataStream = df
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
      .map(_._2)

    val (dbName, tableName) = if (args.length == 6) {
      (args(4), args(5))
    } else {
      (sparkConf.get("spark.datasource.hive.warehouse.dbname"),
        sparkConf.get("spark.datasource.hive.warehouse.tablename"))
    }

    val writer = dataStream.map { s =>
      val x = s.split(",")
      Schema(x(0).toInt, x(1).toInt, x(2).toInt, x(3).toInt, x(4).toInt, x(5).toInt,
        x(6).toInt, x(7).toInt, x(8).toInt, x(9).toInt, x(10).toInt, x(11).toInt,
        x(12).toInt, x(13).toInt, x(14).toInt, x(15).toInt, x(16).toInt, x(17).toInt,
        x(18).toInt, x(19).toFloat, x(20).toFloat, x(21).toFloat, x(22).toFloat,
        x(23).toFloat, x(24).toFloat, x(25).toFloat, x(26).toFloat, x(27).toFloat,
        x(28).toFloat, x(29).toFloat, x(30).toFloat, x(31).toFloat, x(32).toFloat,
        x(33).toFloat, x(34))
    }.writeStream
      .format("com.hortonworks.spark.sql.hive.llap.streaming.HiveStreamingDataSource")
      .option("metastoreUri", metastoreUri)
      .option("database", dbName)
      .option("table", tableName)

    val query = writer.start()
    query.awaitTermination()

    query.stop()
    sparkSession.stop()
  }
}
