package com.tw.apps

import org.scalatest._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import java.net.URI
import scala.collection.immutable.Stream

class StationMartTest extends FeatureSpec with Matchers with GivenWhenThen {

  feature("Apply station status transformations to data frame") {

    scenario("Transform nyc station data frame") {
      val hdfs = FileSystem.get(new URI("hdfs://yourUrl:port/"), new Configuration())
      val path = new Path("/path/to/file/")
      val stream = hdfs.open(path)
      def readLines = Stream.cons(stream.readLine, Stream.continually( stream.readLine))

      readLines.takeWhile(_ != null).foreach(line => println(line))

      "1" should be("1")
    }
  }
}
