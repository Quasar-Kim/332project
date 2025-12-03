package redsort.worker

import cats.data.Validated
import com.monovore.decline._
import cats.effect._
import cats.syntax.all._
import redsort.jobs.worker.Worker
import scala.concurrent.duration._
import fs2.io.file.{Files, Path}
import redsort.jobs.worker._
import redsort.jobs.context._
import redsort.jobs.context.impl._

import redsort.worker.handlers

import redsort.jobs.SourceLogger
import org.log4s.getLogger
import redsort.jobs.Common.NetAddr
import com.monovore.decline.effect.CommandIOApp
import ch.qos.logback.classic.util.ContextInitializer
import ch.qos.logback.classic.LoggerContext
import org.slf4j.LoggerFactory
import org.slf4j.helpers.SubstituteLoggerFactory

trait ProductionWorkerCtx
    extends WorkerCtx
    with ProductionFileStorage
    with ProductionWorkerRpcServer
    with ProductionSchedulerRpcClient
    with ProductionReplicatorLocalRpcClient
    with ProductionReplicatorLocalRpcServer
    with ProductionReplicatorRemoteRpcClient
    with ProductionReplicatorRemoteRpcServer
    with ProductionNetInfo

// container of command line options
final case class Configuration(
    masterAddress: NetAddr,
    inputDirs: Seq[Path],
    outputDir: Path,
    workingDir: Option[Path],
    threads: Int,
    port: Int,
    replicatorLocalPort: Int,
    verbose: Boolean = false
)
object Configuration {
  def apply(
      masterAddress: NetAddr,
      inputDirs: Seq[Path],
      outputDir: Path,
      workingDir: Option[Path],
      threads: Int,
      port: Int,
      replicatorLocalPort: Int,
      verbose: Boolean = false
  ) =
    new Configuration(
      masterAddress,
      inputDirs,
      outputDir,
      workingDir,
      threads,
      port,
      replicatorLocalPort,
      verbose
    )
}

// command line parser
object CmdParser {
  val masterAddr: Opts[NetAddr] =
    Opts.argument[String](metavar = "master_addr").mapValidated { s =>
      s.split(":").toList match {
        case ip :: portStr :: Nil =>
          portStr.toIntOption match {
            case Some(port) => Validated.valid(new NetAddr(ip, port))
            case None       => Validated.invalidNel("must be formatted as <ip>:<port>")
          }
        case _ => Validated.invalidNel("must be formatted as <ip>:<port>")
      }
    }

  val inputDirHead: Opts[Path] =
    Opts
      .option[String]("input", help = "input directory", short = "I", metavar = "input_dir")
      .map(Path(_))

  val inputDirTail: Opts[Seq[Path]] =
    Opts.arguments[String]("input_dir").map(_.map(Path(_)).toList.toSeq).withDefault(Seq())

  val inputDir = (inputDirHead, inputDirTail).mapN { case (head, tail) => Seq(head) ++ tail }

  val outputDir: Opts[Path] =
    Opts
      .option[String]("output", help = "output directory", short = "O", metavar = "output_dir")
      .map(Path(_))

  val workingDir: Opts[Option[Path]] =
    Opts
      .option[String]("working", help = "working directory", metavar = "working_dir")
      .map(Path(_))
      .orNone

  val threads: Opts[Int] =
    Opts
      .option[Int]("threads", "number of worker threads per machine (default: 4)", metavar = "n")
      .withDefault(4)

  val port: Opts[Int] =
    Opts
      .option[Int]("port", "port number of first worker (default: 6001)", metavar = "port")
      .withDefault(6001)

  val replicatorLocalPort: Opts[Int] =
    Opts
      .option[Int](
        "replicator-local-port",
        "port number of replicator local service (default: 7000)",
        metavar = "port"
      )
      .withDefault(7000)

  val verbose: Opts[Boolean] = Opts.flag("verbose", "print logs to stdout").orFalse

  val parser: Opts[Configuration] =
    (
      masterAddr,
      inputDir,
      outputDir,
      workingDir,
      threads,
      port,
      replicatorLocalPort,
      verbose
    ).mapN(Configuration.apply)
}

object Main extends CommandIOApp(name = "worker", header = "worker binary") {
  private[this] val logger = new SourceLogger(getLogger, "workerBin")
  override def main: Opts[IO[ExitCode]] =
    CmdParser.parser.map { case config @ Configuration(_, _, _, _, _, _, _, _) =>
      configureLogging(config) >>
        workerProgram(config).map(_ => ExitCode.Success)
    }

  def configureLogging(config: Configuration): IO[Unit] = IO {
    if (config.verbose) {
      System.setProperty("CONSOLE_LOG_LEVEL", "INFO")

      // reload logger
      val context = getLoggerContext
      context.reset()
      val contextInitializer = new ContextInitializer(context)
      contextInitializer.autoConfig()
    } else ()
  }

  def getLoggerContext: LoggerContext = {
    // sometimes `asInstanceOf[LoggerContext]` fails if logback is not fully initialized.
    // try mutiple times until we can get logger.
    var iLoggerFactory = LoggerFactory.getILoggerFactory
    var attempts = 0
    val maxAttempts = 10
    while (iLoggerFactory.isInstanceOf[SubstituteLoggerFactory] && attempts < maxAttempts) {
      // Trigger initialization again
      LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME)
      Thread.sleep(50) // Give it a moment to bind
      iLoggerFactory = LoggerFactory.getILoggerFactory
      attempts += 1
    }
    if (iLoggerFactory.isInstanceOf[SubstituteLoggerFactory]) {
      throw new IllegalStateException("SLF4J failed to bind to Logback after waiting.")
    }

    val rootLogger = LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME)
    LoggerFactory.getILoggerFactory().asInstanceOf[LoggerContext]
  }

  val handlerMap: Map[String, JobHandler] = Map(
    "sample" -> new handlers.SampleJobHandler(),
    "sort" -> new handlers.SortJobHandler(),
    "partition" -> new handlers.PartitionJobHandler(),
    "merge" -> new handlers.MergeJobHandler()
  )

  def workerProgram(config: Configuration): IO[Unit] = {
    val workerIds = (0 until config.threads).toList

    workerIds.parTraverse { id =>
      Worker(
        handlerMap = handlerMap,
        masterAddr = config.masterAddress,
        inputDirectories = config.inputDirs,
        outputDirectory = config.outputDir,
        workingDirectory = config.workingDir,
        wtid = id,
        port = config.port + id,
        ctx = new ProductionWorkerCtx {},
        replicatorLocalPort = config.replicatorLocalPort,
        replicatorRemotePort = config.port - 1
      ) { worker =>
        for {
          _ <- worker.waitForComplete
        } yield ()
      }
    }.void
  }
}
