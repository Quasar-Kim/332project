package redsort.master

import cats._
import cats.effect._
import cats.syntax.all._
import cats.effect.std.Console
import redsort.jobs.scheduler.Scheduler
import scala.concurrent.duration._
import com.monovore.decline._
import com.monovore.decline.effect._
import redsort.jobs.context.impl.ProductionWorkerRpcClient
import redsort.jobs.context.impl.ProductionSchedulerRpcServer
import redsort.jobs.context.SchedulerCtx
import org.log4s._
import redsort.jobs.context.impl.ProductionNetInfo
import redsort.jobs.Common.Mid
import redsort.jobs.Common.NetAddr
import redsort.jobs.Common.Wid

// container of command line options
final case class Args(numMachines: Int, port: Int, threads: Int)
import redsort.jobs.SourceLogger
object Args {
  def apply(numMachiens: Int, port: Int, threads: Int) =
    new Args(numMachiens, port, threads)
}

// command line options
object CmdParser {
  val numMachines: Opts[Int] =
    Opts.argument[Int](metavar = "numMachines")

  val port: Opts[Int] =
    Opts.option[Int]("port", "port number (default: 5000)", metavar = "port").withDefault(5000)

  val threads: Opts[Int] =
    Opts
      .option[Int]("threads", "number of worker threads per machine (default: 4)", metavar = "n")
      .withDefault(4)

  val parser: Opts[Args] = (numMachines, port, threads).mapN(Args.apply)
}

// dependency injection
object Ctx
    extends SchedulerCtx
    with ProductionWorkerRpcClient
    with ProductionSchedulerRpcServer
    with ProductionNetInfo

object Main extends CommandIOApp(name = "master", header = "master binary") {
  override def main: Opts[IO[ExitCode]] =
    CmdParser.parser.map { case args @ Args(_, _, _) =>
      startScheduler(args).map(_ => ExitCode.Success)
    }

  def startScheduler(args: Args): IO[Map[Wid, NetAddr]] =
    Scheduler(
      port = args.port,
      numMachines = args.numMachines,
      numWorkersPerMachine = args.threads,
      ctx = Ctx
    )(DistributedSorting.run)
}
