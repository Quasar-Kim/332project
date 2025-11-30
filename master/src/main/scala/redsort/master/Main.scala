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
import redsort.master.CmdParser.outFileSize
import ch.qos.logback.classic.LoggerContext
import org.slf4j.LoggerFactory
import org.slf4j.helpers.SubstituteLoggerFactory
import ch.qos.logback.classic.util.ContextInitializer

// container of command line options
final case class Args(
    numMachines: Int,
    port: Int,
    threads: Int,
    outFileSize: Long,
    log: Boolean = false
)
import redsort.jobs.SourceLogger
object Args {
  def apply(numMachiens: Int, port: Int, threads: Int, outFileSize: Long, log: Boolean = false) =
    new Args(numMachiens, port, threads, outFileSize, log)
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

  val outFileSize: Opts[Long] =
    Opts
      .option[Long](
        "out-file-size",
        "max. output size of each output partition files, in MB (default: 128)",
        metavar = "size"
      )
      .withDefault(128L)

  val verbose: Opts[Boolean] = Opts.flag("verbose", help = "log to standard output").orFalse

  val parser: Opts[Args] = (numMachines, port, threads, outFileSize, verbose).mapN(Args.apply)
}

// dependency injection
object Ctx
    extends SchedulerCtx
    with ProductionWorkerRpcClient
    with ProductionSchedulerRpcServer
    with ProductionNetInfo

object Main extends CommandIOApp(name = "master", header = "master binary") {
  override def main: Opts[IO[ExitCode]] =
    CmdParser.parser.map { case args @ Args(_, _, _, _, _) =>
      configureLogging(args) >>
        startScheduler(args).map(_ => ExitCode.Success)
    }

  def configureLogging(args: Args): IO[Unit] = IO {
    if (args.log) {
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

  def startScheduler(args: Args): IO[Map[Wid, NetAddr]] = {
    val distributedSortingConfig = new DistributedSortingConfig(
      outFileSize = args.outFileSize * 1000L * 1000L
    )

    Scheduler(
      port = args.port,
      numMachines = args.numMachines,
      numWorkersPerMachine = args.threads,
      ctx = Ctx
    )(DistributedSorting.run(distributedSortingConfig))
  }

}
