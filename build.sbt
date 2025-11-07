ThisBuild / scalaVersion := "2.13.16"
ThisBuild / version := "0.1.0"
ThisBuild / organization := "org.postech.csed332_25.red"

lazy val jobs = (project in file("jobs"))
    .enablePlugins(Fs2Grpc)

lazy val master = (project in file("master"))
    .settings(
        assembly / assemblyJarName := "master.jar",
        Compile / PB.targets := Seq(
            scalapb.gen() -> (Compile / sourceManaged).value / "scalapb"
        )
    )
    .dependsOn(jobs)

lazy val worker = (project in file("worker"))
    .settings(
        assembly / assemblyJarName := "worker.jar"
    )
    .dependsOn(jobs)