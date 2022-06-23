ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.10"

lazy val root = (project in file("."))
  .settings(
    name := "appsflyer-api-pull"
  )

val AkkaVersion = "2.6.8"
val AkkaHttpVersion = "10.2.9"
libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-http" % AkkaHttpVersion,
  "com.typesafe.akka" %% "akka-stream" % AkkaVersion
)


// spark libraries are provided at runtime, EMR 6.6.0 uses Spark 3.2.0
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.0"
