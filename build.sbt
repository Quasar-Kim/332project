ThisBuild / scalaVersion := "2.13.16"
ThisBuild / version := "0.1.0"
ThisBuild / organization := "org.postech.csed332_25.red"

lazy val commonSettings = Seq(
  scalafmtOnCompile := true,

  // Dependencies
  libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.19" % "test",
  libraryDependencies += "org.scalactic" %% "scalactic" % "3.2.19",
  libraryDependencies += "com.google.protobuf" % "protobuf-java" % "3.25.3" % "protobuf",
  libraryDependencies += "com.github.scopt" %% "scopt" % "4.1.0",
  libraryDependencies += "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf",
  libraryDependencies += "io.grpc" % "grpc-netty-shaded" % "1.60.0",
  libraryDependencies += "org.scalamock" %% "scalamock" % "5.2.0" % Test,
  Compile / PB.targets := Seq(
    scalapb.gen() -> (Compile / sourceManaged).value / "scalapb"
  ),
)

lazy val jobs = (project in file("jobs"))
  .settings(commonSettings)
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
