package com.tw.apps

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

object StationValidation {
  def getInValidData(stationDF: DataFrame)(implicit sparkSession: SparkSession): DataFrame = {
     stationDF.filter(col("docks_available") < 0 || col("bikes_available")<0 || col("latitude").isNull || col("longitude").isNull)
  }

  def getValidData(stationDF:Dataset[StationData] )(implicit sparkSession: SparkSession): Dataset[StationData] = {
    stationDF.filter(col("docks_available") >= 0 && col("bikes_available")>=0 && col("latitude").isNotNull && col("longitude").isNotNull)
  }

}
