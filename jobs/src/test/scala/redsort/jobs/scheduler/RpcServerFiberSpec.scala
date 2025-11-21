package redsort.jobs.scheduler

import redsort.AsyncSpec
import redsort.jobs.Common._
import cats.effect._
import cats.effect.std.Queue
import io.grpc.Server
import redsort.jobs.context.interface.SchedulerRpcServer
import org.scalamock.stubs.Stubs
import redsort.jobs.messages.SchedulerFs2Grpc
import io.grpc.Metadata
import cats.effect.std.Supervisor
import redsort.jobs.messages.WorkerHello
import redsort.jobs.messages.SchedulerHello
import redsort.jobs.messages.JobSystemError
import redsort.jobs.messages.NetAddrMsg
import scala.concurrent.duration._
import redsort.jobs.messages.HaltRequest
import redsort.jobs.messages.LocalStorageInfo
import redsort.jobs.messages.FileEntryMsg

class RpcServerFiberSpec extends AsyncSpec {
  def fixture = new {
    val workerAddrs = Map(
      new Wid(0, 0) -> new NetAddr("1.1.1.1", 5000),
      new Wid(0, 1) -> new NetAddr("1.1.1.1", 5001),
      new Wid(1, 0) -> new NetAddr("1.1.1.2", 5000),
      new Wid(1, 1) -> new NetAddr("1.1.1.2", 5001)
    )
    val sharedState = SharedState.init(workerAddrs)

    val schedulerFiberQueue = Queue.unbounded[IO, SchedulerFiberEvents]
    val serverStub = stub[Server]
    (serverStub.start _).returnsWith(serverStub)
    val ctxStub = stub[SchedulerRpcServer]

    val grpc = Supervisor[IO].evalMap(sv =>
      for {
        stateR <- sharedState
        schedulerFiberQueue <- schedulerFiberQueue
        grpcDeferred <- IO.deferred[SchedulerFs2Grpc[IO, Metadata]]
        _ <- IO((ctxStub.schedulerRpcServer _).returns { case (grpc, port) =>
          Resource.eval(grpcDeferred.complete(grpc) >> IO(serverStub))
        })
        _ <- sv
          .supervise(
            RpcServerFiber
              .start(5000, stateR, schedulerFiberQueue, ctxStub, workerAddrs)
              .useForever
          )
          .void
        grpc <- grpcDeferred.get
      } yield (grpc, schedulerFiberQueue)
    )

    val metadata = new Metadata()
    val wid = new Wid(1, 0)
    val workerHello = new WorkerHello(
      wtid = 0,
      storageInfo = None,
      ip = "1.1.1.2",
      port = 5000
    )
  }

  behavior of "server fiber (when RegisterWorker called)"

  it should "enqueue WorkerHello to input queue of scheduler fiber" in {
    val f = fixture

    f.grpc
      .use { case (grpc, schedulerFiberQueue) =>
        for {
          _ <- grpc.registerWorker(f.workerHello, f.metadata)
          event <- schedulerFiberQueue.take
        } yield {
          inside(event) { case SchedulerFiberEvents.WorkerRegistration(hello, from) =>
            hello should equal(f.workerHello)
            from should equal(f.wid)
          }
        }
      }
      .timeout(1.seconds)
  }

  it should "reply with SchedulerHello" in {
    val f = fixture

    f.grpc
      .use { case (grpc, schedulerFiberQueue) =>
        for {
          schedulerHello <- grpc.registerWorker(f.workerHello, f.metadata)
        } yield {
          schedulerHello.mid should equal(f.wid.mid)
          schedulerHello.replicatorAddrs should equal(
            Map(
              0 -> new NetAddrMsg("1.1.1.1", 4999),
              1 -> new NetAddrMsg("1.1.1.2", 4999)
            )
          )
        }
      }
      .timeout(1.seconds)
  }

  // this test case is required only for fault tolerance and not implemented for now

  // behavior of "server fiber (when NotifyUp called)"
  // ignore should "prevent HeartbeatTimout to be sent to scheduler fiber"

  behavior of "server fiber (when HaltOnError called)"

  it should "enqueue Halt to scheduler fiber" in {
    val f = fixture

    val error = new JobSystemError(
      message = "some error",
      cause = None
    )
    val req = new HaltRequest(
      err = error,
      source = Wid.toMsg(f.wid)
    )

    f.grpc
      .use { case (grpc, schedulerFiberQueue) =>
        for {
          _ <- grpc.haltOnError(req, f.metadata)
          evt <- schedulerFiberQueue.take
        } yield {
          inside(evt) { case SchedulerFiberEvents.Halt(err, from) =>
            err should equal(error)
            from should equal(f.wid)
          }
        }
      }
      // BUG(junseong): timeout not working without this...?
      .timeout(1.seconds)
  }
}
