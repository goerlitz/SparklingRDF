name := """SparklingRDF"""

version := "0.1"

scalaVersion := "2.10.6"

// Include only src/[main|test]/scala in the compile/test configuration
unmanagedSourceDirectories in Compile := (scalaSource in Compile).value :: Nil
unmanagedSourceDirectories in Test    := (scalaSource in Test).value :: Nil


libraryDependencies ++= Seq(

  "org.apache.spark" %% "spark-core" % "1.5.1",

  // testing
  "org.scalatest" %% "scalatest" % "2.2.5" % "test",
  "org.scalacheck" %% "scalacheck" % "1.12.4" % "test",

  // 3rd party
  "org.openrdf.sesame" % "sesame-runtime" % "2.8.6",
  "org.semanticweb.yars" % "nxparser-parsers" % "2.2",

  "commons-logging" % "commons-logging" % "1.2",
  "org.slf4j" % "slf4j-simple" % "1.7.12"
)


fork in run := true
