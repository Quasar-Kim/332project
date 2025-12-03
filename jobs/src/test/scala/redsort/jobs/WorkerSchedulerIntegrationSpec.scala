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
import redsort.jobs.context.impl.ProductionNetInfo
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
import redsort.jobs.worker.Directories
import redsort.jobs.messages._
import com.google.protobuf.ByteString
import redsort.jobs.context.impl.ProductionReplicatorLocalRpcServer
import redsort.jobs.context.impl.ProductionReplicatorRemoteRpcClient
import redsort.jobs.context.impl.ProductionReplicatorRemoteRpcServer
import redsort.jobs.Common.FileEntry

trait FakeNetInfo extends NetInfo {
  override def getIP: IO[String] =
    IO.pure("127.0.0.1")
}

object SchedulerTestCtx
    extends SchedulerCtx
    with ProductionWorkerRpcClient
    with ProductionSchedulerRpcServer
    with FakeNetInfo

class WorkerTestCtx(ref: Ref[IO, Map[String, Array[Byte]]])
    extends InMemoryFileStorage(ref)
    with WorkerCtx
    with ProductionWorkerRpcServer
    with ProductionSchedulerRpcClient
    with FakeNetInfo
    with ProductionReplicatorLocalRpcClient
    with ProductionReplicatorLocalRpcServer
    with ProductionReplicatorRemoteRpcClient
    with ProductionReplicatorRemoteRpcServer

object NoopJobHandler extends JobHandler {
  override def apply(
      args: Seq[any.Any],
      inputs: Seq[Path],
      outputs: Seq[Path],
      ctx: FileStorage,
      dirs: Directories
  ): IO[Option[Array[Byte]]] =
    IO.pure(None)
}

object IdentityJobHandler extends JobHandler {
  override def apply(
      args: Seq[any.Any],
      inputs: Seq[Path],
      outputs: Seq[Path],
      ctx: FileStorage,
      d: Directories
  ): IO[Option[Array[Byte]]] =

    IO.pure(Some(args(0).unpack[BytesArg].value.toByteArray()))
}

object FaultyJobHandler extends JobHandler {
  override def apply(
      args: Seq[any.Any],
      inputs: Seq[Path],
      outputs: Seq[Path],
      ctx: FileStorage,
      dirs: Directories
  ): IO[Option[Array[Byte]]] =
    IO.raiseError(new RuntimeException("handle me"))
}

class WorkerSchedulerIntegrationSpec extends AsyncFunSpec with BeforeAndAfterEach {
  override val timeLimit = 10.seconds

  def fixture(portOffset: Int) = new {
    def integrationTest(
        logName: String,
        handlers: Map[String, JobHandler]
    )(body: (Scheduler, Worker, WorkerTestCtx) => IO[Unit]): IO[Unit] =
      fileLogger(logName)
        .use { _ =>
          // run scheduler
          Scheduler(
            port = 5000 + portOffset,
            numMachines = 1,
            numWorkersPerMachine = 1,
            ctx = SchedulerTestCtx
          ) { scheduler =>
            for {
              fs <- IO.ref[Map[String, Array[Byte]]](Map())
              ctx = new WorkerTestCtx(fs)
              _ <- Worker(
                handlerMap = handlers,
                masterAddr = new NetAddr("127.0.0.1", 5000 + portOffset),
                inputDirectories = Seq(),
                outputDirectory = Path("/output"),
                wtid = 0,
                port = 6000 + portOffset,
                ctx = ctx,
                replicatorLocalPort = 6050 + portOffset,
                replicatorRemotePort = 6070 + portOffset
              ) { worker =>
                body(scheduler, worker, ctx)
              }
            } yield ()
          }
        }
        .timeout(5.second)
  }

  test("worker registration", NetworkTest) {
    val f = fixture(0)

    f.integrationTest("worker-registratiion", Map()) { case (scheduler, worker, _) =>
      scheduler.waitInit.void
    }
  }

  test("noop job execution without sync", NetworkTest) {
    val f = fixture(1)

    val handlers = Map("noop" -> NoopJobHandler)
    val jobSpec = new JobSpec(
      name = "noop",
      args = Seq(),
      inputs = Seq(),
      outputs = Seq()
    )

    f.integrationTest("noop-job-execution-no-sync", handlers) { case (scheduler, worker, _) =>
      for {
        _ <- scheduler.waitInit

        // run job "noop" and get result
        result <- scheduler.runJobs(Seq(jobSpec), sync = false)
      } yield {
        result.results(0)._1 should be(jobSpec)
        result.results(0)._2.success should be(true)
      }
    }
  }

  test("noop job execution", NetworkTest) {
    val f = fixture(2)

    val handlers = Map("noop" -> NoopJobHandler)
    val jobSpec = new JobSpec(
      name = "noop",
      args = Seq(),
      inputs = Seq(),
      outputs = Seq()
    )

    f.integrationTest("noop-job-execution", handlers) { case (scheduler, worker, _) =>
      for {
        _ <- scheduler.waitInit
        result <- scheduler.runJobs(Seq(jobSpec))
      } yield {
        result.results(0)._1 should be(jobSpec)
        result.results(0)._2.success should be(true)
      }
    }
  }

  test("job with args", NetworkTest) {
    val f = fixture(3)

    val handlers = Map("identity" -> IdentityJobHandler)
    val buffer = ByteString.copyFrom(Array(0xde, 0xad, 0xbe, 0xef).map(_.toByte))
    val arg = new BytesArg(buffer)
    val jobSpec = new JobSpec(
      name = "identity",
      args = Seq(arg),
      inputs = Seq(),
      outputs = Seq()
    )

    f.integrationTest("job-with-name", handlers) { case (scheduler, worker, _) =>
      for {
        _ <- scheduler.waitInit

        // run job and get result
        result <- scheduler.runJobs(Seq(jobSpec))
      } yield {
        result.results(0)._1 should be(jobSpec)
        result.results(0)._2.success should be(true)
        result.results(0)._2.retval.get should be(buffer)
      }
    }
  }

  test("completion", NetworkTest) {
    val f = fixture(4)

    f.integrationTest("completion", Map()) { case (scheduler, worker, _) =>
      for {
        _ <- scheduler.waitInit
        _ <- (
          worker.waitForComplete,
          scheduler.complete
        ).parTupled
      } yield ()
    }
  }

  test("failing job execution", NetworkTest) {
    val f = fixture(5)

    val handlers = Map("faulty" -> FaultyJobHandler)
    val jobSpec = new JobSpec(
      name = "faulty",
      args = Seq(),
      inputs = Seq(),
      outputs = Seq()
    )

    f.integrationTest("failing-job-execution", handlers) { case (scheduler, worker, _) =>
      for {
        _ <- scheduler.waitInit
        result <- scheduler.runJobs(Seq(jobSpec)).attempt
      } yield {
        result match {
          case Left(e)  => ()
          case Right(_) => fail("did not errored")
        }
      }
    }
  }
}
