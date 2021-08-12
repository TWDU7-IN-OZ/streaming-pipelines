val sparkVersion = "2.3.0"

lazy val root = (project in file(".")).

  settings(
    inThisBuild(List(
      organization := "com.tw",
      scalaVersion := "2.11.8",
      version := "0.0.1"
    )),

    name := "end-to-end-test",

    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.0.5" % "test",
      "org.apache.hadoop" % "hadoop-client" % "3.3.1"
    )
  )