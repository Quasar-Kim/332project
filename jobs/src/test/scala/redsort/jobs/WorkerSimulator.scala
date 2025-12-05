package redsort.jobs

import cats._
import cats.effect._
import cats.syntax.all._
import com.monovore.decline._
import com.monovore.decline.effect.CommandIOApp
import redsort.Logging.fileLogger
import redsort.jobs.worker.JobHandler
import com.google.protobuf.any
import fs2.io.file.Path
import redsort.jobs.context.interface.FileStorage
import redsort.jobs.worker.Directories
import fs2.io.file.Files
import fs2._
import java.nio.charset.StandardCharsets
import redsort.jobs.worker.Worker
import redsort.jobs.context.WorkerCtx
import redsort.jobs.context.impl._
import redsort.jobs.Common.NetAddr
import scala.concurrent.duration._
import org.log4s._

final case class WorkerSimArgs(
    testName: String,
    workerId: Int,
    basePort: Int,
    masterPort: Int,
    inputDir: String,
    outputDir: String,
    suicide: Boolean
)

object WorkerSimArgs {
  def apply(
      testName: String,
      workerId: Int,
      basePort: Int,
      masterPort: Int,
      inputDir: String,
      outputDir: String,
      suicide: Boolean
  ): WorkerSimArgs =
    new WorkerSimArgs(
      testName = testName,
      workerId = workerId,
      basePort = basePort,
      masterPort = masterPort,
      inputDir = inputDir,
      outputDir = outputDir,
      suicide = suicide
    )
}

object WorkerSimCmdParser {
  val testName: Opts[String] =
    Opts.option[String]("test", "test name")

  val workerId: Opts[Int] =
    Opts.option[Int]("worker-id", "woker ID")

  val basePort: Opts[Int] =
    Opts.option[Int]("base-port", "base port")

  val masterPort: Opts[Int] =
    Opts.option[Int]("master-port", "master port")

  val inputDir: Opts[String] =
    Opts.option[String]("input-dir", "input directory")

  val outputDir: Opts[String] =
    Opts.option[String]("output-dir", "output directory")

  val suicide: Opts[Boolean] =
    Opts.flag("suicide", "kill myself").orFalse

  val parser = (testName, workerId, basePort, masterPort, inputDir, outputDir, suicide).mapN(
    WorkerSimArgs.apply
  )
}

object WorkerSimCtx
    extends WorkerCtx
    with ProductionFileStorage
    with ProductionWorkerRpcServer
    with ProductionSchedulerRpcClient
    with ProductionReplicatorLocalRpcClient
    with ProductionReplicatorLocalRpcServer
    with ProductionReplicatorRemoteRpcClient
    with ProductionReplicatorRemoteRpcServer
    with ProductionNetInfo

object WorkerSimulator extends CommandIOApp(name = "worker-smulator", header = "worker simulator") {
  private[this] def logger = new SourceLogger(getLogger, "worker-sim")

  override def main: Opts[IO[ExitCode]] =
    WorkerSimCmdParser.parser.map { args =>
      fileLogger(
        args.testName + s".worker${args.workerId}" + (if (args.suicide) ".dead" else "")
      )
        .use { _ =>
          startWorker(args)
        }
        .map(_ => ExitCode.Success)
    }

  def startWorker(args: WorkerSimArgs): IO[Unit] = {
    args.testName match {
      case "while-running-job-length_short" | "while-running-job-length_long" => {
        val handlers = Map(
          "length" -> new LengthJobHandler(suicide = args.suicide, logger = logger),
          "sum" -> new SumJobHandler(suicide = false, logger = logger)
        )
        twoMachineTest(args, handlers)
      }
      case "while-running-job-sum_short" | "while-running-job-sum_long" => {
        val handlers = Map(
          "length" -> new LengthJobHandler(suicide = false, logger = logger),
          "sum" -> new SumJobHandler(suicide = args.suicide, logger = logger)
        )
        twoMachineTest(args, handlers)
      }

      case _ => IO.raiseError(new RuntimeException(s"invalid test name: ${args.testName}"))
    }
  }

  def twoMachineTest(args: WorkerSimArgs, handlers: Map[String, JobHandler]): IO[Unit] =
    Worker(
      handlerMap = handlers,
      masterAddr = new NetAddr("127.0.0.1", args.masterPort),
      inputDirectories = Seq(Path(args.inputDir)),
      outputDirectory = Path(args.outputDir),
      wtid = 0,
      port = args.basePort + 1,
      ctx = WorkerSimCtx,
      replicatorLocalPort = args.basePort + 2,
      replicatorRemotePort = args.basePort
    ) { worker => worker.waitForComplete }
}

class LengthJobHandler(suicide: Boolean, logger: SourceLogger) extends JobHandler {
  override def apply(
      args: Seq[any.Any],
      inputs: Seq[Path],
      outputs: Seq[Path],
      ctx: FileStorage,
      d: Directories
  ): IO[Option[Array[Byte]]] =
    for {
      contents <- ctx.readAll(inputs(0).toString)
      _ <- IO.whenA(suicide)(
        for {
          _ <- ctx.deleteRecursively(d.outputDirectory.toString)
          _ <- logger.error("MACHINE FAULT (from handler job)")
          _ <- IO(Runtime.getRuntime.halt(137))
        } yield ()
      )
      _ <- ctx.save(
        outputs(0).toString,
        Stream.chunk(Chunk.from(contents.length.toString().map(_.toByte)))
      )
    } yield None
}

class SumJobHandler(suicide: Boolean, logger: SourceLogger) extends JobHandler {
  override def apply(
      args: Seq[any.Any],
      inputs: Seq[Path],
      outputs: Seq[Path],
      ctx: FileStorage,
      d: Directories
  ): IO[Option[Array[Byte]]] =
    for {
      contentsA <- ctx.readAll(inputs(0).toString)
      _ <- IO.whenA(suicide)(
        for {
          _ <- ctx.deleteRecursively(d.outputDirectory.toString)
          _ <- logger.error("MACHINE FAULT (from handler sum)")
          _ <- IO(Runtime.getRuntime.halt(137))
        } yield ()
      )

      lengthA <- IO((new String(contentsA, StandardCharsets.US_ASCII)).toInt)
      contentsB <- ctx.readAll(inputs(1).toString)
      lengthB <- IO(new String(contentsB, StandardCharsets.US_ASCII).toInt)
      length = lengthA + lengthB

      _ <- ctx.save(
        outputs(0).toString,
        Stream.chunk(Chunk.from(length.toString().map(_.toByte)))
      )
    } yield None
}
