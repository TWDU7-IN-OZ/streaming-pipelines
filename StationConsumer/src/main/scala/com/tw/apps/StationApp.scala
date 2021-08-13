package com.tw.apps

import StationDataTransformation._
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.spark.sql.SparkSession

object StationApp {

  def main(args: Array[String]): Unit = {

    val zookeeperConnectionString = if (args.isEmpty) "zookeeper:2181" else args(0)

    val retryPolicy = new ExponentialBackoffRetry(1000, 3)

    val zkClient = CuratorFrameworkFactory.newClient(zookeeperConnectionString, retryPolicy)

    zkClient.start()

    val kafkaBrokers = new String(zkClient.getData.forPath("/tw/stationDataNYCv2/kafkaBrokers"))

    val nycStationTopic = new String(zkClient.getData.watched.forPath("/tw/stationDataNYCv2/topic"))
    val sfStationTopic = new String(zkClient.getData.watched.forPath("/tw/stationDataSF/topic"))
    val franceStationTopic = new String(zkClient.getData.watched.forPath("/tw/stationDataFrance/topic"))

    val checkpointLocation = new String(
      zkClient.getData.watched.forPath("/tw/output/checkpointLocation"))

    val outputLocation = new String(
      zkClient.getData.watched.forPath("/tw/output/dataLocation"))

    implicit val spark: SparkSession = SparkSession.builder
      .appName("StationConsumer")
      .getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    import spark.implicits._

    val nycStationDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBrokers)
      .option("subscribe", nycStationTopic)
      .option("startingOffsets", "latest")
      .option("failOnDataLoss", value = false)
      .load()
      .selectExpr("CAST(value AS STRING) as raw_payload")
      .transform(jsonToStationDataDF(_, spark))

    val sfStationDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBrokers)
      .option("subscribe", sfStationTopic)
      .option("startingOffsets", "latest")
      .option("failOnDataLoss", value = false)
      .load()
      .selectExpr("CAST(value AS STRING) as raw_payload")
      .transform(jsonToStationDataDF(_, spark))

    val franceStationDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBrokers)
      .option("subscribe", franceStationTopic)
      .option("startingOffsets", "latest")
      .option("failOnDataLoss", false)
      .load()
      .selectExpr("CAST(value AS STRING) as raw_payload")
      .transform(franceStationStatusJson2DF(_, spark))

    val rawStationData = nycStationDF.union(sfStationDF).union(franceStationDF).as[StationData]

    validateAndReduce(rawStationData)
      .toDF()
      .transform(formatDate)
      .writeStream
      .format("overwriteCSV")
      .outputMode("update")
      .option("header", value = true)
      .option("truncate", value = false)
      .option("checkpointLocation", checkpointLocation)
      .option("path", outputLocation)
      .start()
      .awaitTermination()
  }
}
