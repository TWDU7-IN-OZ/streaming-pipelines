package com.tw.apps

import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object StationValidation {
  def getInvalidRows(stationDF: DataFrame, spark: SparkSession): DataFrame = {
    stationDF.filter(station => !validateDock(station))
  }

  def validateDock(station: Row): Boolean = {
    station.getAs[Int]("docks_available") >= 0
  }
}
