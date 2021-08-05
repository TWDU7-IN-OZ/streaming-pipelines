package com.tw.apps

import com.tw.apps.StationValidation.getInvalidRows
import org.apache.spark.sql.SparkSession
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object StationValidatorApp {
  def main(args: Array[String]): Unit = {

    val retryPolicy = new ExponentialBackoffRetry(1000, 3)
    if (args.length != 2) {
      val message = "Two arguments are required: \"zookeeper server\" and \"application folder in zookeeper\"!"
      throw new IllegalArgumentException(message)
    }
    val zookeeperConnectionString = args(0)

    val zookeeperFolder = args(1)

    val zkClient = CuratorFrameworkFactory.newClient(zookeeperConnectionString, retryPolicy)

    zkClient.start

    val checkpointLocation = new String(
      zkClient.getData.watched.forPath(s"$zookeeperFolder/checkpointLocation"))

    val dataLocation = new String(
      zkClient.getData.watched.forPath(s"$zookeeperFolder/dataLocation"))

    val invalidDataLocation = new String(
      zkClient.getData.watched.forPath(s"$zookeeperFolder/invalidDataLocation"))

    val invalidDataCheckpointLocation = new String(
      zkClient.getData.watched.forPath(s"$zookeeperFolder/invalidDataCheckpointLocation"))


    val spark = SparkSession.builder
      .appName("StationValidatorApp")
      .getOrCreate()


    val stationSchema = new StructType()
      .add("bikes_available", "integer")
      .add("docks_available", "integer")
      .add("is_renting", "boolean")
      .add("is_returning", "boolean")
      .add("last_updated", "long")
      .add("station_id", "string")
      .add("name", "string")
      .add("latitude", "double")
      .add("longitude", "double")
    val stationDF = spark
      .readStream
      .schema(stationSchema)
      .option("checkpointLocation", checkpointLocation)
      .csv(dataLocation)

    val invalidRows = getInvalidRows(stationDF, spark)

    invalidRows
      .writeStream
      .format("csv")
      .option("invalidDataCheckpointLocation", invalidDataCheckpointLocation)
      .option("path", invalidDataLocation)
      .start()
      .awaitTermination()

  }
}
