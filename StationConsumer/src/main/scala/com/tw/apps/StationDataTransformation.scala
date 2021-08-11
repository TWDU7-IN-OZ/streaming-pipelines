package com.tw.apps

import java.time.Instant
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{udf, _}
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}

import scala.util.parsing.json.JSON

object StationDataTransformation {

  val sfToStationStatus: String => Seq[StationData] = raw_payload => {
    val json = JSON.parseFull(raw_payload)
    val payload = json.get.asInstanceOf[Map[String, Any]]("payload")
    extractSFStationStatus(payload)
  }

  private def extractSFStationStatus(payload: Any) = {

    val network: Any = payload.asInstanceOf[Map[String, Any]]("network")

    val stations: Any = network.asInstanceOf[Map[String, Any]]("stations")

    stations.asInstanceOf[Seq[Map[String, Any]]]
      .map(x => {
        StationData(
          x("free_bikes").asInstanceOf[Double].toInt,
          x("empty_slots").asInstanceOf[Double].toInt,
          x("extra").asInstanceOf[Map[String, Any]]("renting").asInstanceOf[Double] == 1,
          x("extra").asInstanceOf[Map[String, Any]]("returning").asInstanceOf[Double] == 1,
          Instant.from(DateTimeFormatter.ISO_INSTANT.parse(x("timestamp").asInstanceOf[String])).getEpochSecond,
          x("id").asInstanceOf[String],
          x("name").asInstanceOf[String],
          x("latitude").asInstanceOf[Double],
          x("longitude").asInstanceOf[Double]
        )
      })
  }

  def sfStationStatusJson2DF(jsonDF: DataFrame, spark: SparkSession): DataFrame = {
    val toStatusFn: UserDefinedFunction = udf(sfToStationStatus)

    import spark.implicits._

    jsonDF.select(explode(toStatusFn(jsonDF("raw_payload"))) as "status")
      .select($"status.*")
  }

  def nycStationStatusJson2DF(jsonDF: DataFrame, spark: SparkSession): DataFrame = {
    import spark.implicits._

    jsonDF.select(from_json($"raw_payload", ScalaReflection.schemaFor[StationData].dataType) as "status")
      .select($"status.*")
  }


  def formatDate(value: DataFrame): DataFrame = {
    value.withColumnRenamed("last_updated", "last_updated_epoch")
      .withColumn("last_updated", from_unixtime(col("last_updated_epoch"), "yyyy-MM-dd'T'hh:mm:ss"))
  }

  def validateAndReduce(rawStationData: Dataset[StationData])(implicit spark: SparkSession): Dataset[ValidatedStationData] = {
    import spark.implicits._
    rawStationData
      .withColumn("is_valid", validateFields).as[ValidatedStationData]
      .groupByKey(r => (r.station_id, r.is_valid))
      .reduceGroups((r1, r2) => if (r1.last_updated > r2.last_updated) r1 else r2)
      .map(_._2)
  }

  private def validateFields: Column = {

    when(isPositiveInteger("docks_available") &&
      isPositiveInteger("bikes_available") &&
      isNonNull("latitude") && isNonNull("longitude"), lit(true))
      .otherwise(lit(false))
  }

  private def isPositiveInteger(columnName: String): Column = {
    col(columnName) >= 0
  }

  private def isNonNull(colName: String): Column = {
    col(colName).isNotNull
  }
}
