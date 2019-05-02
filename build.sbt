// me.valik.osm %% osm-export % 0.1.0
// me.valik.osm.export.OsmExportJob

import Dependencies._

ThisBuild / scalaVersion     := "2.12.8"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "me.valik.osm"

lazy val root = (project in file("."))
  .settings(
    name := "osm-export",
    libraryDependencies ++= Seq(
      dropwizardMetrics,
      scalaTest % Test)
      ++ sparkDeps.map(_ % Provided)
      ++ circeJson
  )
