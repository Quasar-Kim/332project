import sbt._

object Dependencies {
    // Versions
    val scalamockVersion = "7.5.2"
    val monocleVersion = "3.1.0"
    
    // Libraries
    val catsEffect = "org.typelevel" %% "cats-effect" % "3.6.3"
    val log4cats = "org.typelevel" %% "log4cats-slf4j" % "2.7.1"
    val grpcNetty = "io.grpc" % "grpc-netty-shaded" % scalapb.compiler.Version.grpcJavaVersion
    val scalapbRuntime = "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf"
    val monocleCore = "dev.optics" %% "monocle-core" % monocleVersion
    val monocleMacro = "dev.optics" %% "monocle-macro" % monocleVersion

    // -- testing deps
    val scalaTest = "org.scalatest" %% "scalatest" % "3.2.19" % "test"
    val scalactic = "org.scalactic" %% "scalactic" % "3.2.19"
    val scalamock = "org.scalamock" %% "scalamock" % scalamockVersion % Test
    val scalamockCatsEffect = "org.scalamock" %% "scalamock-cats-effect" % scalamockVersion % Test
    val catsEffectTestingScalaTest = "org.typelevel" %% "cats-effect-testing-scalatest" % "1.7.0"

    // Projects
    val deps = Seq(
        catsEffect,
        log4cats,
        grpcNetty,
        monocleCore,
        monocleMacro,
        scalaTest,
        scalactic,
        scalamock,
        scalamockCatsEffect,
        catsEffectTestingScalaTest,
        scalapbRuntime,
    )
}