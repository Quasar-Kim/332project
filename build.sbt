import Dependencies._

ThisBuild / scalaVersion := "2.13.16"
ThisBuild / version := "0.1.0"
ThisBuild / organization := "org.postech.csed332_25.red"

Global / cancelable := true

lazy val commonSettings = Seq(
  scalafmtOnCompile := true,
  libraryDependencies ++= deps,
  Compile / run / fork := true,
  scalacOptions ++= Seq("-feature", "-language:reflectiveCalls", "-Werror", "-deprecation"),
  // Scalatest recommends turning this off since it implmenets its own
  // buffering algorithm.
  Test / logBuffered := false,
  // reprint all errors at the bottom of the test suite run.
  Test / testOptions += Tests.Argument("-oG"),

  // Several libraries bring conflicting META-INF files. META-INF files are not needed at runtime.
  assembly / assemblyMergeStrategy := {
    case PathList("META-INF", "versions", xs @ _*) => MergeStrategy.discard
    case "module-info.class"                       => MergeStrategy.discard
    case x =>
      val oldStrategy = (assembly / assemblyMergeStrategy).value
      oldStrategy(x)
  }
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
    assembly / mainClass := Some("redsort.master.Main"),
  )
  .dependsOn(jobs)

lazy val worker = (project in file("worker"))
  .settings(commonSettings)
  .settings(
    assembly / assemblyJarName := "worker.jar",
    assembly / mainClass := Some("redsort.worker.Main"),
  )
  .dependsOn(jobs)

lazy val root = (project in file("."))
  .settings(commonSettings)
  .settings(
    Test / parallelExecution := false
  )
  .dependsOn(master, worker)
  .aggregate(jobs, master, worker)
