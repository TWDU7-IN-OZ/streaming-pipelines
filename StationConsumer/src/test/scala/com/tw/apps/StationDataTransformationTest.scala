package com.tw.apps

import StationDataTransformation.{formatDate, nycStationStatusJson2DF, jsonToStationDataDF, franceStationStatusJson2DF}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions.{from_json, lit}
import org.scalatest._

class StationDataTransformationTest extends FeatureSpec with Matchers with GivenWhenThen {

  feature("Apply station status transformations to data frame") {
    val spark = SparkSession.builder.appName("Test App").master("local").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC");
    import spark.implicits._

    scenario("Transform san fransisco station data frame") {

      val testStationData =
        """
          |{
          |    "metadata": {
          |        "producer_id": "producer_station-san_francisco",
          |        "size": 133012,
          |        "message_id": "a72f0ff5-bc0f-41d4-9b75-695ccedee91d",
          |        "ingestion_time": 1628678812171
          |    },
          |    "payload": {
          |        "network": {
          |            "company": [
          |                "Motivate International, Inc."
          |            ],
          |            "gbfs_href": "https://gbfs.fordgobike.com/gbfs/gbfs.json",
          |            "href": "/v2/networks/ford-gobike",
          |            "id": "ford-gobike",
          |            "location": {
          |                "city": "San Francisco Bay Area, CA",
          |                "country": "US",
          |                "latitude": 37.7141454,
          |                "longitude": -122.25
          |            },
          |            "name": "Bay Wheels",
          |            "stations": [
          |                {
          |                    "empty_slots": 4,
          |                    "extra": {
          |                        "address": null,
          |                        "last_updated": 1602187490,
          |                        "renting": 1,
          |                        "returning": 1,
          |                        "uid": "340"
          |                    },
          |                    "free_bikes": 11,
          |                    "id": "d0e8f4f1834b7b33a3faf8882f567ab8",
          |                    "latitude": 37.849735,
          |                    "longitude": -122.270582,
          |                    "name": "Harmon St at Adeline St",
          |                    "timestamp": "2020-10-08T21:02:33.968000Z"
          |                }
          |            ]
          |        }
          |    }
          |}
          """.stripMargin


      Given("Sample data for station_status")
      val testDF1 = Seq(testStationData).toDF("raw_payload")


      When("Transformations are applied")
      val resultDF1 = testDF1.transform(jsonToStationDataDF(_, spark))

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
      row1.get(7) should be(37.849735)
      row1.get(8) should be(-122.270582)
    }

    scenario("Transform nyc station data frame") {

      val testStationData =
        """
          |{
          |    "metadata": {
          |        "producer_id": "producer_station-nyc",
          |        "size": 133012,
          |        "message_id": "a72f0ff5-bc0f-41d4-9b75-695ccedee91d",
          |        "ingestion_time": 1628678812171
          |    },
          |    "payload":{
          |   "network":{
          |      "company":[
          |         "NYC Bike Share, LLC",
          |         "Motivate International, Inc.",
          |         "PBSC Urban Solutions"
          |      ],
          |      "gbfs_href":"https://gbfs.citibikenyc.com/gbfs/gbfs.json",
          |      "href":"/v2/networks/citi-bike-nyc",
          |      "id":"citi-bike-nyc",
          |      "location":{
          |         "city":"New York, NY",
          |         "country":"US",
          |         "latitude":40.7143528,
          |         "longitude":-74.00597309999999
          |      },
          |      "name":"Citi Bike",
          |      "stations":[
          |         {
          |            "empty_slots":21,
          |            "extra":{
          |               "address":null,
          |               "last_updated":1628682235,
          |               "renting":1,
          |               "returning":1,
          |               "uid":"3328"
          |            },
          |            "free_bikes":8,
          |            "id":"46a983722ee1f51813a6a3eb6534a6e4",
          |            "latitude":40.795,
          |            "longitude":-73.9645,
          |            "name":"W 100 St & Manhattan Ave",
          |            "timestamp":"2021-08-11T11:52:25.357000Z"
          |         }]
          |}
          |}
          |}
          """.stripMargin


      Given("Sample data for station_status")
      val testDF1 = Seq(testStationData).toDF("raw_payload")


      When("Transformations are applied")
      val resultDF1 = testDF1.transform(jsonToStationDataDF(_, spark))

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
      row1.get(0) should be(8)
      row1.get(1) should be(21)
      row1.get(2) shouldBe true
      row1.get(3) shouldBe true
      row1.get(4) should be(1628682745)
      row1.get(5) should be("46a983722ee1f51813a6a3eb6534a6e4")
      row1.get(6) should be("W 100 St & Manhattan Ave")
      row1.get(7) should be(40.795)
      row1.get(8) should be(-73.9645)
    }

    scenario("Transform france station data frame") {

      val testStationData =
        """
          |{
          |"payload":{
          |  "network": {
          |    "company": [
          |      "JCDecaux"
          |    ],
          |    "href": "/v2/networks/le-velo",
          |    "id": "le-velo",
          |    "license": {
          |      "name": "Open Licence",
          |      "url": "https://developer.jcdecaux.com/#/opendata/licence"
          |    },
          |    "location": {
          |      "city": "Marseille",
          |      "country": "FR",
          |      "latitude": 43.296482,
          |      "longitude": 5.36978
          |    },
          |    "name": "Le v\u00e9lo",
          |    "source": "https://developer.jcdecaux.com",
          |    "stations": [
          |      {
          |        "empty_slots": 13,
          |        "extra": {
          |          "address": "391 MICHELET - 391 BOULEVARD MICHELET",
          |          "banking": true,
          |          "bonus": false,
          |          "last_update": 1628682844000,
          |          "slots": 19,
          |          "status": "OPEN",
          |          "uid": 8149
          |        },
          |        "free_bikes": 6,
          |        "id": "686e48654a218c70daf950a4e893e5b0",
          |        "latitude": 43.25402727813068,
          |        "longitude": 5.401873594694653,
          |        "name": "8149-391 MICHELET",
          |        "timestamp": "2021-08-11T12:03:53.026000Z"
          |      },
          |      {
          |        "empty_slots": 2,
          |        "extra": {
          |          "address": "TEISSEIRE ROUBAUD - ANGLE RUE RAYMOND TEISSEIRE ET RUE MAGUY ROUBAUD",
          |          "banking": true,
          |          "bonus": false,
          |          "last_update": 1628683378000,
          |          "slots": 10,
          |          "status": "OPEN",
          |          "uid": 9207
          |        },
          |        "free_bikes": 7,
          |        "id": "259c5e65f9e2af98046069144c666aa6",
          |        "latitude": 43.27252367086013,
          |        "longitude": 5.399686062414487,
          |        "name": "9207- TEISSEIRE - ROUBAUD",
          |        "timestamp": "2021-08-11T12:03:53.034000Z"
          |      }]
          |     }
          |    }
          |   }
          """.stripMargin


      Given("Sample data for station_status")
      val testDF1 = Seq(testStationData).toDF("raw_payload")


      When("Transformations are applied")
      val resultDF1 = testDF1.transform(franceStationStatusJson2DF(_, spark))

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

      val rows = resultDF1.collectAsList()
      rows.get(1).get(0) should be(7)
      rows.get(1).get(1) should be(2)
      rows.get(1).get(2) shouldBe true
      rows.get(1).get(3) shouldBe true
      rows.get(1).get(4) should be(1628683433)
      rows.get(1).get(5) should be("259c5e65f9e2af98046069144c666aa6")
      rows.get(1).get(6) should be("9207- TEISSEIRE - ROUBAUD")
      rows.get(1).get(7) should be(43.27252367086013)
      rows.get(1).get(8) should be(5.399686062414487)
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
  }
}
