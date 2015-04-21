name := """SparklingRDF"""

version := "0.1"

scalaVersion := "2.11.6"

libraryDependencies ++= Seq(

  "org.apache.spark" %% "spark-core" % "1.3.1",

  // testing
  "org.scalatest" %% "scalatest" % "2.2.4" % "test",
  
  // 3rd party
  "org.openrdf.sesame" % "sesame-runtime" % "2.8.1",
  "commons-logging" % "commons-logging-api" % "1.1",
  "org.slf4j" % "slf4j-simple" % "1.7.10"
)

