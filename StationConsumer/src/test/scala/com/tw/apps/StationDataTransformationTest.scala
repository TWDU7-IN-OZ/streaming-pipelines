package com.tw.apps

import com.tw.apps.StationDataTransformation.{formatDate, nycStationStatusJson2DF, sfStationStatusJson2DF}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest._

class StationDataTransformationTest extends FeatureSpec with Matchers with GivenWhenThen {

  feature("Apply station status transformations to data frame") {
    implicit val spark: SparkSession = SparkSession.builder.appName("Test App").master("local").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    import spark.implicits._

    scenario("Transform nyc station data frame") {

      val testStationData =
        """{
          "station_id":"83",
          "bikes_available":19,
          "docks_available":41,
          "is_renting":true,
          "is_returning":true,
          "last_updated":1536242527,
          "name":"Atlantic Ave & Fort Greene Pl",
          "latitude":40.68382604,
          "longitude":-73.97632328
          }"""

      val schema = ScalaReflection.schemaFor[StationData].dataType //.asInstanceOf[StructType]

      Given("Sample data for station_status")
      val testDF1 = Seq(testStationData).toDF("raw_payload")


      When("Transformations are applied")
      val resultDF1 = testDF1.transform(nycStationStatusJson2DF(_, spark))

      Then("Useful columns are extracted")
      resultDF1.schema.fields(0).name should be("bikes_available")
      resultDF1.schema.fields(0).dataType.typeName should be("integer")
      resultDF1.schema.fields(1).name should be("docks_available")
      resultDF1.schema.fields(1).dataType.typeName should be("integer")
      resultDF1.schema.fields(2).name should be("is_renting")
      resultDF1.schema.fields(2).dataType.typeName should be("boolean")
      resultDF1.schema.fields(3).name should be("is_returning")
      resultDF1.schema.fields(3).dataType.typeName should be("boolean")
      resultDF1.schema.fields(4).name should be("last_updated")
      resultDF1.schema.fields(4).dataType.typeName should be("long")
      resultDF1.schema.fields(5).name should be("station_id")
      resultDF1.schema.fields(5).dataType.typeName should be("string")
      resultDF1.schema.fields(6).name should be("name")
      resultDF1.schema.fields(6).dataType.typeName should be("string")
      resultDF1.schema.fields(7).name should be("latitude")
      resultDF1.schema.fields(7).dataType.typeName should be("double")
      resultDF1.schema.fields(8).name should be("longitude")
      resultDF1.schema.fields(8).dataType.typeName should be("double")

      val row1 = resultDF1.head()
      row1.get(0) should be(19)
      row1.get(1) should be(41)
      row1.get(2) shouldBe true
      row1.get(3) shouldBe true
      row1.get(4) should be(1536242527)
      row1.get(5) should be("83")
      row1.get(6) should be("Atlantic Ave & Fort Greene Pl")
      row1.get(7) should be(40.68382604)
      row1.get(8) should be(-73.97632328)
    }

    scenario("Transform sf station data frame") {

      val testStationData =
        """{
          "payload":
          {
          "network":{
          "stations":[{
	        "empty_slots": 4,
	        "extra": {
	        	"address": null,
	        	"last_updated": 1602187490,
	        	"renting": 1,
	        	"returning": 1,
	        	"uid": "340"
	        },
          	"free_bikes": 11,
          	"id": "d0e8f4f1834b7b33a3faf8882f567ab8",

          	"longitude": -122.270582,
          	"name": "Harmon St at Adeline St",
          	"timestamp": "2020-10-08T21:02:33.968000Z"
          }]
          }
          }
          }"""

      val schema = ScalaReflection.schemaFor[StationData].dataType //.asInstanceOf[StructType]

      Given("Sample data for station_status")
      val testDF1 = Seq(testStationData).toDF("raw_payload")
      testDF1.show(false)


      When("Transformations are applied")
      val resultDF1 = testDF1.transform(sfStationStatusJson2DF(_, spark))
      resultDF1.show(false)

      Then("Useful columns are extracted")
      resultDF1.schema.fields(0).name should be("bikes_available")
      resultDF1.schema.fields(0).dataType.typeName should be("integer")
      resultDF1.schema.fields(1).name should be("docks_available")
      resultDF1.schema.fields(1).dataType.typeName should be("integer")
      resultDF1.schema.fields(2).name should be("is_renting")
      resultDF1.schema.fields(2).dataType.typeName should be("boolean")
      resultDF1.schema.fields(3).name should be("is_returning")
      resultDF1.schema.fields(3).dataType.typeName should be("boolean")
      resultDF1.schema.fields(4).name should be("last_updated")
      resultDF1.schema.fields(4).dataType.typeName should be("long")
      resultDF1.schema.fields(5).name should be("station_id")
      resultDF1.schema.fields(5).dataType.typeName should be("string")
      resultDF1.schema.fields(6).name should be("name")
      resultDF1.schema.fields(6).dataType.typeName should be("string")
      resultDF1.schema.fields(7).name should be("latitude")
      resultDF1.schema.fields(7).dataType.typeName should be("double")
      resultDF1.schema.fields(8).name should be("longitude")
      resultDF1.schema.fields(8).dataType.typeName should be("double")

      val row1 = resultDF1.head()
      row1.get(0) should be(11)
      row1.get(1) should be(4)
      row1.get(2) shouldBe true
      row1.get(3) shouldBe true
      row1.get(4) should be(1602190953)
      row1.get(5) should be("d0e8f4f1834b7b33a3faf8882f567ab8")
      row1.get(6) should be("Harmon St at Adeline St")
      row1.get(7) should be null
      row1.get(8) should be(-122.270582)
    }

    scenario("Transform last updated") {

      val testStationMart =
        """[
          {"last_updated":1536242527}
          ]"""

      Given("Sample data for last_updated")
      val testDF1 = spark.read.json(Seq(testStationMart).toDS())
      testDF1.show(false)


      When("Transformations are applied")
      val resultDF1 = testDF1.transform(formatDate)

      Then("Useful columns are extracted")
      resultDF1.schema.fields(0).name should be("last_updated_epoch")
      resultDF1.schema.fields(1).name should be("last_updated")

      val row1 = resultDF1.head()

      row1.get(0) should be(1536242527)
      row1.get(1) should be("2018-09-06T02:02:07")
    }

    scenario("reduce valid data with is_valid true") {
      val testStationData =
        """{
          "station_id":"83",
          "bikes_available":19,
          "docks_available":41,
          "is_renting":true,
          "is_returning":true,
          "last_updated":1536242527,
          "name":"Atlantic Ave & Fort Greene Pl",
          "latitude":40.68382604,
          "longitude":-73.97632328
          }
        """
      val testStationData1 =
        """{
          |"station_id":"83",
          |          "bikes_available":10,
          |          "docks_available":25,
          |          "is_renting":true,
          |          "is_returning":true,
          |          "last_updated":1536242528,
          |          "name":"Atlantic Ave & Fort Greene Pl",
          |          "latitude":40.68382604,
          |          "longitude":-73.97632328
          |}
        """.stripMargin

      val testDF1: Dataset[StationData] = spark.read.json(Seq(testStationData, testStationData1).toDS())
        .withColumn("bikes_available", col("bikes_available").cast(IntegerType))
        .withColumn("docks_available", col("docks_available").cast(IntegerType))
        .as[StationData]

      val expectedData = buildExpectedDataSet(spark, testStationData1, isValid = true).as[ValidatedStationData]

      val validatedData: Dataset[ValidatedStationData] = StationDataTransformation.validateAndReduce(testDF1)
      validatedData.collect().length should be(1)

      expectedData.head() shouldBe validatedData.head()
    }

    scenario("reduce invalid data with is_valid false") {

      val validtestStationData1 =
        """{
          |"station_id":"83",
          |"bikes_available":10,
          |"docks_available":25,
          |"is_renting":true,
          |"is_returning":true,
          |"last_updated":1536242527,
          |"name":"Atlantic Ave & Fort Greene Pl",
          |"latitude":40.68382604,
          |"longitude":-73.97632328
          |}
        """.stripMargin

      val invalidtestStationData =
        """{
          "station_id":"83",
          "bikes_available":-19,
          "docks_available":41,
          "is_renting":true,
          "is_returning":true,
          "last_updated":1536242528,
          "name":"Atlantic Ave & Fort Greene Pl",
          "latitude":40.68382604,
          "longitude":-73.97632328
          }
        """
      val invalidtestStationData2 =
        """{
          |"station_id":"83",
          |"bikes_available":10,
          |"docks_available":-25,
          |"is_renting":true,
          |"is_returning":true,
          |"last_updated":1536242530,
          |"name":"Atlantic Ave & Fort Greene Pl",
          |"longitude":-73.97632328
          |}
        """.stripMargin

      val testDF1: Dataset[StationData] = spark.read.json(Seq(validtestStationData1, invalidtestStationData,invalidtestStationData2).toDS())
        .withColumn("bikes_available", col("bikes_available").cast(IntegerType))
        .withColumn("docks_available", col("docks_available").cast(IntegerType))
        .as[StationData]


      val expectedData = buildExpectedDataSet(spark, validtestStationData1, isValid = true)
        .unionByName(buildExpectedDataSet(spark, invalidtestStationData2, isValid = false).withColumn("latitude", lit(null)))
        .as[ValidatedStationData]


      val validatedData: Dataset[ValidatedStationData] = StationDataTransformation.validateAndReduce(testDF1)
      expectedData.collect() should contain theSameElementsAs validatedData.collect()
    }
  }

  private def buildExpectedDataSet(spark: SparkSession, dataSet: String, isValid: Boolean) = {
    import spark.implicits._
    spark.read.json(Seq(dataSet).toDS())
      .withColumn("bikes_available", col("bikes_available").cast(IntegerType))
      .withColumn("docks_available", col("docks_available").cast(IntegerType))
      .withColumn("is_valid", lit(isValid))
  }
}
