import sbt._

object Dependencies {
  val SparkVersion = "2.4.2"
  val CirceVersion = "0.11.1"

  lazy val sparkDeps = Seq(
    "org.apache.spark" %% "spark-core",
    "org.apache.spark" %% "spark-sql"
    //"org.apache.spark" %% "spark-mllib"
  ).map(_ % SparkVersion)


  lazy val circeJson = Seq(
    "io.circe" %% "circe-core",
    "io.circe" %% "circe-generic"
    //"io.circe" %% "circe-parser"
  ).map(_ % CirceVersion)

  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.0.5"
  lazy val dropwizardMetrics = "nl.grons" %% "metrics4-scala" % "4.0.4"
}
