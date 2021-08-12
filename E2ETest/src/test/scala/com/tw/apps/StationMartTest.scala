package com.tw.apps

import org.scalatest._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import java.net.URI
import scala.collection.immutable.Stream

class StationMartTest extends FeatureSpec with Matchers with GivenWhenThen {

  feature("Apply station status transformations to data frame") {

    scenario("Transform nyc station data frame") {
      val hdfs = FileSystem.get(new URI("hdfs://localhost:9000"), new Configuration())
      val path = new Path("/tw/stationMart/data/year=2021/month=08/day=12/hour=09/part-00000-01f3b100-d6ef-47e8-9cad-9ef28317e8d4.c000.csv")

      hdfs.exists(path) should be(true)
    }
  }
}
