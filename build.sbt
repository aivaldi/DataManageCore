name := "DataSpaEngine"

version := "1.1.1"

scalaVersion := "2.11.7"

organization := "com.ar.visionaris"

sbtVersion := "0.13.12"

scalaSource in Compile := baseDirectory.value / "src"

scalaSource in Test := baseDirectory.value / "test"

libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.11"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.5"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.2"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.2"
libraryDependencies += "org.mongodb.spark" %% "mongo-spark-connector" % "2.3.2"
//libraryDependencies += "com.databricks" % "spark-csv_2.11" % "1.5.0"
//libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "1.6.0-M1"

libraryDependencies += "com.github.potix2" %% "spark-google-spreadsheets" % "0.5.0"
libraryDependencies += "com.crealytics" %% "spark-google-adwords" % "0.9.0"
libraryDependencies += "com.crealytics" %% "spark-excel" % "0.9.8"
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.7.2"


// https://mvnrepository.com/artifact/com.google.apis/google-api-services-analytics
libraryDependencies += "com.google.apis" % "google-api-services-analytics" % "v3-rev142-1.22.0"


//libraryDependencies += "org.apache.spark" % "spark-hive_2.11" % "1.6.1"

//para el testing
libraryDependencies += "org.specs2" %% "specs2-core" % "3.0" % "test"
libraryDependencies += "org.specs2" % "specs2-junit_2.11" % "3.7-scalaz-7.1.6"

// https://mvnrepository.com/artifact/org.apache.pdfbox/pdfbox
libraryDependencies += "org.apache.pdfbox" % "pdfbox" % "2.0.7"

resolvers += "scalaz-bintray" at "http://dl.bintray.com/scalaz/releases"

scalacOptions in Test ++= Seq("-Yrangepos")


parallelExecution in Test := false
javaOptions in run +=
"-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=9999"
fork in run := true
