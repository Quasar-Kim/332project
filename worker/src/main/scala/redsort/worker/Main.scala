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

object logger extends SourceLogger(getLogger, "workerBin")

trait ProductionWorkerCtx
    extends WorkerCtx
    with ProductionFileStorage
    with ProductionWorkerRpcServer
    with ProductionSchedulerRpcClient
    with ProductionReplicatorLocalRpcClient
    with ProductionNetInfo

// container of command line options
final case class Configuration(masterAddress: NetAddr, inputDirs: Seq[Path], outputDir: Path)
object Configuration {
  def apply(masterAddress: NetAddr, inputDirs: Seq[Path], outputDir: Path) =
    new Configuration(masterAddress, inputDirs, outputDir)
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
    Opts.arguments[String]("input_dir").map(_.map(Path(_)).toList.toSeq)

  val inputDir = (inputDirHead, inputDirTail).mapN { case (head, tail) => Seq(head) ++ tail }

  val outputDir: Opts[Path] =
    Opts
      .option[String]("output", help = "output directory", short = "O", metavar = "output_dir")
      .map(Path(_))

  val parser: Opts[Configuration] =
    (masterAddr, inputDir, outputDir).mapN(Configuration.apply)
}

object Main extends CommandIOApp(name = "worker", header = "worker binary") {
  override def main: Opts[IO[ExitCode]] =
    CmdParser.parser.map { case config @ Configuration(_, _, _) =>
      workerProgram(config).map(_ => ExitCode.Success)
    }

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
          masterAddr = config.masterAddress,
          inputDirectories = config.inputDirs,
          outputDirectory = config.outputDir,
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
