package com.tw.apps

import com.tw.apps.StationValidation.getInvalidRows
import org.apache.spark.sql.SparkSession
import org.scalatest.{FeatureSpec, GivenWhenThen, Matchers}

class StationValidationTest extends FeatureSpec with Matchers with GivenWhenThen {
  feature("Validation") {
    val spark = SparkSession.builder.appName("Test App").master("local").getOrCreate()
    import spark.implicits._

    scenario("Validates docks available") {
      Given("negative docks available")
      val testDF1 = Seq(
        (19, -1, true, true, 1536242527, "83", "Atlantic Ave & Fort Greene Pl", 40.68382604, -73.97632328),
        (10, 10, true, true, 1536242527, "10", "10 St", 10.10, -10.10)
      )
        .toDF("bikes_available", "docks_available", "is_renting", "is_returning", "last_updated", "station_id", "name", "latitude", "longitude")

      When("the row is validated")
      val resultDF1 = getInvalidRows(testDF1, spark)

      Then("the given row is returned")
      resultDF1.count() should be(1)

      val row1 = resultDF1.head()
      row1.get(0) should be(19)
      row1.get(1) should be(-1)
      row1.get(2) shouldBe true
      row1.get(3) shouldBe true
      row1.get(4) should be(1536242527)
      row1.get(5) should be("83")
      row1.get(6) should be("Atlantic Ave & Fort Greene Pl")
      row1.get(7) should be(40.68382604)
      row1.get(8) should be(-73.97632328)
    }
  }
}
