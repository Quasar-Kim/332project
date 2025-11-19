import Dependencies._

ThisBuild / scalaVersion := "2.13.16"
ThisBuild / version := "0.1.0"
ThisBuild / organization := "org.postech.csed332_25.red"

// Scalatest recommends turning this off since it implmenets its own
// buffering algorithm.
Test / logBuffered := false

lazy val commonSettings = Seq(
  scalafmtOnCompile := true,
  libraryDependencies ++= deps,
  Compile / run / fork := true,
  scalacOptions ++= Seq("-feature", "-language:reflectiveCalls")
)

lazy val jobs = (project in file("jobs"))
  .settings(
    commonSettings,
    scalapbCodeGeneratorOptions += CodeGeneratorOption.FlatPackage,
  )
  .enablePlugins(Fs2Grpc)

lazy val master = (project in file("master"))
  .settings(commonSettings)
  .settings(
    assembly / assemblyJarName := "master.jar",
  )
  .dependsOn(jobs)

lazy val worker = (project in file("worker"))
  .settings(commonSettings)
  .settings(
    assembly / assemblyJarName := "worker.jar"
  )
  .dependsOn(jobs)
