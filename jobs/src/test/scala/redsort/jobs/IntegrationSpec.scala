package redsort.jobs

import redsort.{AsyncFunSpec, NetworkTest}
import cats.effect._
import cats.syntax.all._
import scala.concurrent.duration._
import org.log4s._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.{Args, Status}
import org.scalatest.FutureOutcome
import scala.concurrent.Future
import redsort.Logging._
import redsort.jobs.scheduler.Scheduler
import redsort.jobs.Common.NetAddr
import redsort.jobs.context.SchedulerCtx
import redsort.jobs.context.WorkerCtx
import redsort.jobs.context.impl.ProductionWorkerRpcClient
import redsort.jobs.context.impl.ProductionSchedulerRpcServer
import redsort.jobs.context.impl.InMemoryFileStorage
import redsort.jobs.context.impl.ProductionWorkerRpcServer
import redsort.jobs.context.impl.ProductionSchedulerRpcClient
import redosrt.jobs.context.impl.ProductionNetInfo
import redsort.jobs.context.interface.ReplicatorLocalRpcClient
import redsort.jobs.context.impl.ProductionReplicatorLocalRpcClient
import redsort.jobs.worker.Worker
import fs2.io.file.Path
import cats.effect.std.Supervisor
import redsort.jobs.context.interface.NetInfo
import redsort.jobs.worker.JobHandler
import redsort.jobs.context.interface.FileStorage
import redsort.jobs.scheduler.JobSpec
import com.google.protobuf.any

object SchedulerTestCtx
    extends SchedulerCtx
    with ProductionWorkerRpcClient
    with ProductionSchedulerRpcServer

trait FakeNetInfo extends NetInfo {
  override def getIP: IO[String] =
    IO.pure("127.0.0.1")
}

class WorkerTestCtx(ref: Ref[IO, Map[String, Array[Byte]]])
    extends InMemoryFileStorage(ref)
    with WorkerCtx
    with ProductionWorkerRpcServer
    with ProductionSchedulerRpcClient
    with FakeNetInfo
    with ProductionReplicatorLocalRpcClient

object NoopJobHandler extends JobHandler {
  override def apply(
      args: Seq[any.Any],
      inputs: Seq[Path],
      outputs: Seq[Path],
      ctx: FileStorage
  ): IO[Option[Array[Byte]]] =
    IO.pure(None)
}

class IntegrationSpec extends AsyncFunSpec with BeforeAndAfterEach {
  override val timeLimit = 10.seconds

  def fixture = new {
    def worker(handlerMap: Map[String, JobHandler]) = for {
      fs <- Ref.of[IO, Map[String, Array[Byte]]](Map()).toResource
      worker <- Worker(
        handlerMap = handlerMap,
        masterAddr = new NetAddr("127.0.0.1", 5000),
        inputDirectories = Seq(),
        outputDirectory = Path("/output"),
        wtid = 0,
        port = 6000,
        ctx = new WorkerTestCtx(fs)
      )
    } yield ()

    val getScheduler = Scheduler(
      port = 5000,
      workers = Seq(Seq(new NetAddr("127.0.0.1", 6000))),
      ctx = SchedulerTestCtx
    )
  }

  test("worker registration", NetworkTest) {
    val f = fixture

    fileLogger("worker-registration").use { _ =>
      val res = for {
        // XXX: why Resource.eval only works?
        // spawn worker in background
        supervisor <- Supervisor[IO]
        _ <- Resource.eval(supervisor.supervise(f.worker(Map()).useForever))

        // start scheduler and wait for registration
        scheduler <- f.getScheduler
        _ <- scheduler.waitInit.toResource
      } yield ()
      res.use(_ => IO.unit).timeout(5.second)
    }
  }

  test("noop job execution without sync", NetworkTest) {
    val f = fixture

    val handlers = Map("noop" -> NoopJobHandler)
    val jobSpec = new JobSpec(
      name = "noop",
      args = Seq(),
      inputs = Seq(),
      outputs = Seq()
    )

    fileLogger("noop-job-execution-no-sync").use { _ =>
      val res = for {
        // spawn worker in background
        supervisor <- Supervisor[IO]
        _ <- Resource.eval(supervisor.supervise(f.worker(handlers).useForever))
        scheduler <- f.getScheduler
        _ <- scheduler.waitInit.toResource

        // run job "noop" and get result
        result <- scheduler.runJobs(Seq(jobSpec), sync = false).toResource
      } yield {
        result.results(0)._1 should be(jobSpec)
        result.results(0)._2.success should be(true)
      }
      res.use(_ => IO.unit).timeout(10.second)
    }
  }

  test("noop job execution", NetworkTest) {
    val f = fixture

    val handlers = Map("noop" -> NoopJobHandler)
    val jobSpec = new JobSpec(
      name = "noop",
      args = Seq(),
      inputs = Seq(),
      outputs = Seq()
    )

    fileLogger("noop-job-execution").use { _ =>
      val res = for {
        // spawn worker in background
        supervisor <- Supervisor[IO]
        _ <- Resource.eval(supervisor.supervise(f.worker(handlers).useForever))
        scheduler <- f.getScheduler
        _ <- scheduler.waitInit.toResource

        // run job "noop" and get result
        result <- scheduler.runJobs(Seq(jobSpec)).toResource
      } yield {
        result.results(0)._1 should be(jobSpec)
        result.results(0)._2.success should be(true)
      }
      res.use(_ => IO.unit).timeout(10.second)
    }
  }
}
