name := "StreamProcessing"

version := "0.1"

scalaVersion := "2.10.6"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.3",
  "org.apache.spark" %% "spark-sql" % "1.6.3",
  "org.scalatest" %% "scalatest" % "3.0.4" % Test
)
libraryDependencies += "com.databricks" %% "spark-csv" % "1.5.0"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "1.6.3"
