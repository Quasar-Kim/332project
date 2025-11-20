package redsort.jobs.scheduler

import redsort.FlatSpecBase
import redsort.jobs.Common._
import cats.effect._
import cats.effect.std.Queue
import org.scalamock.stubs.Stubs
import cats.effect.std.Supervisor
import scala.concurrent.duration._
import redsort.jobs.context.SchedulerCtx
import io.grpc.Server
import redsort.jobs.messages.WorkerFs2Grpc
import io.grpc.Metadata
import scala.concurrent.TimeoutException
import redsort.jobs.messages.SchedulerFs2Grpc
import io.grpc.netty.shaded.io.netty.handler.timeout
import redsort.jobs.messages.WorkerHello

class SchedulerSpec extends FlatSpecBase {
  def fixture = new {
    val workers = Seq(
      Seq(NetAddr("1.1.1.1", 5000), NetAddr("1.1.1.1", 5001)),
      Seq(NetAddr("1.1.1.2", 5000), NetAddr("1.1.1.2", 5001))
    )

    val ctxStub = stub[SchedulerCtx]
    val serverStub = stub[Server]
    (serverStub.start _).returnsWith(serverStub)
    val workerRpcClientStub = stub[WorkerFs2Grpc[IO, Metadata]]
    (ctxStub.schedulerRpcServer _).returnsWith(Resource.eval(IO(serverStub)))
    (ctxStub.workerRpcClient _).returnsWith(Resource.eval(IO(workerRpcClientStub)))

    val schedulerAndServer = for {
      // intercept grpc server implementation using ctxStub.schedulerRpcServer
      grpcDeferred <- Resource.eval(IO.deferred[SchedulerFs2Grpc[IO, Metadata]])
      _ <- Resource.eval(IO((ctxStub.schedulerRpcServer _).returns { case (grpc, port) =>
        Resource.eval(grpcDeferred.complete(grpc) >> IO(serverStub))
      }))

      // start scheduler
      scheduler <- Scheduler(workers, ctxStub, SimpleScheduleLogic)

      // get intercepted server implementation
      grpc <- Resource.eval(grpcDeferred.get)
    } yield (scheduler, grpc)
  }

  behavior of "scheduler.waitInit"

  ignore should "wait until all workers are initialized" in {
    val f = fixture

    f.schedulerAndServer.use { case (scheduler, grpc) =>
      for {
        // no workers are initialized, so this should time out
        _ <- scheduler.waitInit.timeout(100.millis).assertThrows[TimeoutException]

        // initialize workers
        _ <- grpc.registerWorker(
          new WorkerHello(wtid = 0, storageInfo = None, ip = "1.1.1.1"),
          new Metadata
        )
        _ <- grpc.registerWorker(
          new WorkerHello(wtid = 1, storageInfo = None, ip = "1.1.1.1"),
          new Metadata
        )
        _ <- grpc.registerWorker(
          new WorkerHello(wtid = 0, storageInfo = None, ip = "1.1.1.2"),
          new Metadata
        )
        _ <- grpc.registerWorker(
          new WorkerHello(wtid = 1, storageInfo = None, ip = "1.1.1.2"),
          new Metadata
        )

        // now waitWorkers should pass
        _ <- scheduler.waitInit.timeout(100.millis)
      } yield ()
    }
  }

  behavior of "scheduler.runJobs"

  it should "return stats on success"

  it should "return error if one of jobs errors"

  it should "return error if one of workers request halt"
}
