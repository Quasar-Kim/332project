package redsort.worker

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

object logger extends SourceLogger(getLogger, "workerBin")

trait ProductionWorkerCtx
    extends WorkerCtx
    with ProductionFileStorage
    with ProductionWorkerRpcServer
    with ProductionSchedulerRpcClient
    with ProductionReplicatorLocalRpcClient
    with ProductionNetInfo

case class Configuration(
    val masterAddress: String, // <masterIp>:<masterPort>
    val inputDir: Seq[String],
    val outputDir: String
) {
  lazy val masterIp: String = masterAddress.split(":")(0)
  lazy val masterPort: Int = masterAddress.split(":")(1).toInt
}

// Pure utility singleton object
case object ArgParser {
  def parseAndValidate(args: List[String]): Either[String, Configuration] = {
    val config = parse(args)
    if (validate(config)) Right(config)
    else Left("Invalid configuration: masterAddress, inputDir, and outputDir are required.")
  }
  def parse(args: List[String]): Configuration = {
    def parserLoop(args: List[String], state: Int, config: Configuration): Configuration = {
      args match {
        case Nil =>
          config
        case "-I" :: tail =>
          parserLoop(tail, 2, config)
        case "-O" :: tail =>
          parserLoop(tail, 3, config)
        case head :: tail =>
          state match {
            case 1 => // arg: ip:port
              parserLoop(tail, 0, config.copy(masterAddress = head))
            case 2 => // opt: input
              parserLoop(tail, 2, config.copy(inputDir = config.inputDir :+ head))
            case 3 => // opt: input
              parserLoop(tail, 0, config.copy(outputDir = head))
            case 0 => // Ignore
              parserLoop(tail, 0, config)
            case _ => // Error
              parserLoop(tail, 0, config)
          }
      }
    }
    parserLoop(args, 1, new Configuration("", Seq.empty, ""))
  }
  def validate(config: Configuration): Boolean = {
    config.masterAddress.nonEmpty && config.inputDir.nonEmpty && config.outputDir.nonEmpty
  }
}

object Main extends IOApp {
  override def run(args: List[String]): IO[ExitCode] =
    for {
      config <- IO.fromEither(
        ArgParser
          .parseAndValidate(args)
          .leftMap(err => new IllegalArgumentException(err))
      )
      _ <- logger.info(
        s"Starting worker with master at ${config.masterAddress}, input dirs: ${config.inputDir
            .mkString(",")}, output dir: ${config.outputDir}"
      )
      _ <- workerProgram(config)
    } yield ExitCode.Success

  val handlerMap: Map[String, JobHandler] = Map(
    "sample" -> new handlers.JobSampler(),
    "sort" -> new handlers.JobSorter(),
    "partition" -> new handlers.JobPartitioner(),
    "merge" -> new handlers.JobMerger()
  )

  def workerProgram(config: Configuration): IO[Unit] = {
    val workerIds = (0 until 4).toList

    val workersResource: Resource[IO, List[Worker]] = workerIds.parTraverse { id =>
      for {
        _ <- Resource.eval(logger.info(s"[Init] Initializing Worker $id..."))
        worker <- Worker(
          handlerMap = handlerMap,
          masterAddr = redsort.jobs.Common.NetAddr(config.masterIp, config.masterPort),
          inputDirectories = config.inputDir.map(dir => Path(dir)),
          outputDirectory = Path(config.outputDir),
          wtid = id,
          port = 5001 + id,
          ctx = new ProductionWorkerCtx {}
        )
      } yield worker
    }

    workersResource.use { workers =>
      logger.info(s"Started ${workers.length} workers. Waiting for completion...")
      workers.parTraverse { worker =>
        worker.waitForComplete
      }.void
    }
  }
}
