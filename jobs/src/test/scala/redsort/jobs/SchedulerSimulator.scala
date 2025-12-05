package redsort.jobs

import cats._
import cats.effect._
import cats.syntax.all._
import com.monovore.decline._
import redsort.jobs.context.SchedulerCtx
import redsort.jobs.context.impl.ProductionWorkerRpcClient
import redsort.jobs.context.impl.ProductionSchedulerRpcServer
import redsort.jobs.context.impl.ProductionNetInfo
import com.monovore.decline.effect.CommandIOApp
import redsort.Logging.fileLogger
import redsort.jobs.scheduler.Scheduler
import redsort.jobs.scheduler.JobSpec
import redsort.jobs.Common.FileEntry
import org.checkerframework.checker.units.qual.s

final case class SchedulerSimArgs(
    testName: String,
    port: Int
)

object SchedulerSimArgs {
  def apply(testName: String, port: Int): SchedulerSimArgs =
    new SchedulerSimArgs(
      testName = testName,
      port = port
    )
}

object SchedulerSimCmdParser {
  val testName: Opts[String] =
    Opts.option[String]("test", "test name", metavar = "name")

  val port: Opts[Int] =
    Opts.option[Int]("port", "port number", metavar = "port")

  val parser = (testName, port).mapN(SchedulerSimArgs.apply)
}

object SchedulerSimCtx
    extends SchedulerCtx
    with ProductionWorkerRpcClient
    with ProductionSchedulerRpcServer
    with ProductionNetInfo

object SchedulerSimulator
    extends CommandIOApp(name = "scheduler-simulator", header = "scheduler simulator") {
  override def main: Opts[IO[ExitCode]] =
    SchedulerSimCmdParser.parser.map { args =>
      fileLogger(args.testName + s".scheduler")
        .use { _ =>
          startScheduler(args)
        }
        .map(_ => ExitCode.Success)
    }

  def startScheduler(args: SchedulerSimArgs): IO[Unit] = {
    args.testName match {
      case "while-running-job-length_short" | "while-running-job-length_long" |
          "while-running-job-sum_short" | "while-running-job-sum_long" =>
        twoMachineTest(args)
      case _ => IO.raiseError(new RuntimeException(s"invalid test name: ${args.testName}"))
    }
  }

  def twoMachineTest(args: SchedulerSimArgs): IO[Unit] =
    Scheduler(
      port = args.port,
      numMachines = 2,
      numWorkersPerMachine = 1,
      ctx = SchedulerSimCtx
    ) { scheduler =>
      for {
        initialFiles <- scheduler.waitInit
        _ <- scheduler.runJobs(
          Seq(
            new JobSpec(
              name = "length",
              args = Seq(),
              inputs = Seq(initialFiles(0).head._2),
              outputs =
                Seq(new FileEntry(path = "@{working}/length.0", size = -1, replicas = Seq(0)))
            ),
            new JobSpec(
              name = "length",
              args = Seq(),
              inputs = Seq(initialFiles(1).head._2),
              outputs =
                Seq(new FileEntry(path = "@{working}/length.1", size = -1, replicas = Seq(1)))
            )
          )
        )
        _ <- scheduler.runJobs(
          Seq(
            new JobSpec(
              name = "sum",
              args = Seq(),
              inputs = Seq(
                new FileEntry(path = "@{working}/length.0", size = -1, replicas = Seq(0, 1)),
                new FileEntry(path = "@{working}/length.1", size = -1, replicas = Seq(1, 0))
              ),
              outputs = Seq(new FileEntry(path = "@{output}/sum.0", size = -1, replicas = Seq(0)))
            ),
            new JobSpec(
              name = "sum",
              args = Seq(),
              inputs = Seq(
                new FileEntry(path = "@{working}/length.0", size = -1, replicas = Seq(0, 1)),
                new FileEntry(path = "@{working}/length.1", size = -1, replicas = Seq(1, 0))
              ),
              outputs = Seq(new FileEntry(path = "@{output}/sum.1", size = -1, replicas = Seq(1)))
            )
          )
        )
        _ <- scheduler.complete
      } yield ()
    }

}
