package com.tw.apps

import org.apache.spark.sql.SparkSession
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.spark.sql.functions._

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






    val dataLocation = new String(
      zkClient.getData.watched.forPath(s"$zookeeperFolder/dataLocation"))

    val spark = SparkSession.builder
      .appName("StationValidatorApp")
      .getOrCreate()



    val stationDF = spark.read.csv(dataLocation)
    print(stationDF.first())

  }
}
