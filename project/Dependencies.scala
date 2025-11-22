import sbt._

object Dependencies {
    // Versions
    val scalamockVersion = "7.5.2"
    val monocleVersion = "3.1.0"
    val fs2Version = "3.12.0"
    
    // Libraries
    val catsEffect = "org.typelevel" %% "cats-effect" % "3.6.3"
    val grpcNetty = "io.grpc" % "grpc-netty-shaded" % scalapb.compiler.Version.grpcJavaVersion
    val scalapbRuntime = "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf"
    val monocleCore = "dev.optics" %% "monocle-core" % monocleVersion
    val monocleMacro = "dev.optics" %% "monocle-macro" % monocleVersion
    val log4s = "org.log4s" %% "log4s" % "1.10.0"
    val logback = "ch.qos.logback" % "logback-classic" % "1.3.5"
    val fs2core = "co.fs2" %% "fs2-core" % fs2Version
    val fs2io = "co.fs2" %% "fs2-io" % fs2Version

    // -- testing deps
    val scalaTest = "org.scalatest" %% "scalatest" % "3.2.19" % "test"
    val scalactic = "org.scalactic" %% "scalactic" % "3.2.19"
    val scalamock = "org.scalamock" %% "scalamock" % scalamockVersion % Test
    val scalamockCatsEffect = "org.scalamock" %% "scalamock-cats-effect" % scalamockVersion % Test
    val catsEffectTestingScalaTest = "org.typelevel" %% "cats-effect-testing-scalatest" % "1.7.0"

    // Projects
    val deps = Seq(
        catsEffect,
        grpcNetty,
        monocleCore,
        monocleMacro,
        scalaTest,
        scalactic,
        scalamock,
        scalamockCatsEffect,
        catsEffectTestingScalaTest,
        scalapbRuntime,
        fs2core,
        fs2io,
        log4s,
        logback,
    )
}