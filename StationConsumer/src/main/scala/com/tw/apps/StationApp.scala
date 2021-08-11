package com.tw.apps

import com.tw.apps.StationDataTransformation._
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.spark.sql.SparkSession

object StationApp {

  def main(args: Array[String]): Unit = {

    val zookeeperConnectionString = if (args.isEmpty) "zookeeper:2181" else args(0)

    val retryPolicy = new ExponentialBackoffRetry(1000, 3)

    val zkClient = CuratorFrameworkFactory.newClient(zookeeperConnectionString, retryPolicy)

    zkClient.start()

    val stationKafkaBrokers = new String(zkClient.getData.forPath("/tw/stationStatus/kafkaBrokers"))

    val nycStationTopic = new String(zkClient.getData.watched.forPath("/tw/stationDataNYC/topic"))
    val sfStationTopic = new String(zkClient.getData.watched.forPath("/tw/stationDataSF/topic"))

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
      .option("kafka.bootstrap.servers", stationKafkaBrokers)
      .option("subscribe", nycStationTopic)
      .option("startingOffsets", "latest")
      .option("failOnDataLoss", value = false)
      .load()
      .selectExpr("CAST(value AS STRING) as raw_payload")
      .transform(nycStationStatusJson2DF(_, spark))

    val sfStationDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", stationKafkaBrokers)
      .option("subscribe", sfStationTopic)
      .option("startingOffsets", "latest")
      .option("failOnDataLoss", value = false)
      .load()
      .selectExpr("CAST(value AS STRING) as raw_payload")
      .transform(sfStationStatusJson2DF(_, spark))

    val rawStationData = nycStationDF.union(sfStationDF).as[StationData]

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
