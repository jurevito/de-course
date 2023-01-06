name := "Spark Job"

version := "1.0"

scalaVersion := "2.12.15"

mainClass := Some("Job")

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.1"