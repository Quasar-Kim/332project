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
import monocle.syntax.all._
import scala.collection.immutable

class RpcServerFiberSpec extends AsyncSpec {
  def fixture = new {
    val wids = Seq(
      new Wid(0, 0),
      new Wid(0, 1),
      new Wid(1, 0),
      new Wid(1, 1)
    )
    val sharedState = SharedState.init(wids)

    val getSchedulerFiberQueue = Queue.unbounded[IO, SchedulerFiberEvents]
    val getRpcServerFiberQueue = Queue.unbounded[IO, RpcServerFiberEvents]
    val serverStub = stub[Server]
    (serverStub.start _).returnsWith(serverStub)
    val ctxStub = stub[SchedulerRpcServer]

    val grpc = Supervisor[IO].evalMap(sv =>
      for {
        stateR <- sharedState
        schedulerFiberQueue <- getSchedulerFiberQueue
        rpcServerFiberQueue <- getRpcServerFiberQueue
        grpcDeferred <- IO.deferred[SchedulerFs2Grpc[IO, Metadata]]
        _ <- IO((ctxStub.schedulerRpcServer _).returns { case (grpc, port) =>
          Resource.eval(grpcDeferred.complete(grpc) >> IO(serverStub))
        })
        _ <- sv
          .supervise(
            RpcServerFiber
              .start(5000, stateR, schedulerFiberQueue, ctxStub, rpcServerFiberQueue)
              .useForever
          )
          .void
        grpc <- grpcDeferred.get
      } yield (stateR, grpc, schedulerFiberQueue, rpcServerFiberQueue)
    )

    val metadata = new Metadata()
    val wid = new Wid(1, 0)
    val workerHello = new WorkerHello(
      wtid = 0,
      storageInfo = None,
      ip = "1.1.1.2",
      port = 5000
    )

    val initializedWorkerStates = Map(
      new Wid(0, 0) -> new WorkerState(
        wid = new Wid(0, 0),
        netAddr = Some(new NetAddr("1.1.1.1", 5000)),
        status = WorkerStatus.Up,
        pendingJobs = immutable.Queue(),
        runningJob = None,
        completedJobs = immutable.Queue(),
        initialized = true,
        completed = false
      ),
      new Wid(0, 1) -> new WorkerState(
        wid = new Wid(0, 1),
        netAddr = Some(new NetAddr("1.1.1.1", 5001)),
        status = WorkerStatus.Up,
        pendingJobs = immutable.Queue(),
        runningJob = None,
        completedJobs = immutable.Queue(),
        initialized = true,
        completed = false
      ),
      new Wid(1, 0) -> new WorkerState(
        wid = new Wid(1, 0),
        netAddr = Some(new NetAddr("1.1.1.2", 5000)),
        status = WorkerStatus.Up,
        pendingJobs = immutable.Queue(),
        runningJob = None,
        completedJobs = immutable.Queue(),
        initialized = true,
        completed = false
      ),
      new Wid(1, 1) -> new WorkerState(
        wid = new Wid(0, 0),
        netAddr = Some(new NetAddr("1.1.1.2", 5001)),
        status = WorkerStatus.Up,
        pendingJobs = immutable.Queue(),
        runningJob = None,
        completedJobs = immutable.Queue(),
        initialized = true,
        completed = false
      )
    )
  }

  behavior of "server fiber (when RegisterWorker called)"

  it should "enqueue WorkerHello to input queue of scheduler fiber" in {
    val f = fixture

    f.grpc
      .use { case (stateR, grpc, schedulerFiberQueue, _) =>
        for {
          _ <- grpc.registerWorker(f.workerHello, f.metadata).timeout(100.millis).attempt
          event <- schedulerFiberQueue.take
        } yield {
          inside(event) { case SchedulerFiberEvents.WorkerRegistration(hello) =>
            hello should equal(f.workerHello)
          }
        }
      }
      .timeout(1.seconds)
  }

  it should "reply with SchedulerHello when AllWorkersInitialized events are received" in {
    val f = fixture
    val replicatorAddrs = Map(
      0 -> new NetAddr("1.1.1.1", 4999),
      1 -> new NetAddr("1.1.1.2", 4999)
    )

    f.grpc
      .use { case (stateR, grpc, schedulerFiberQueue, rpcServerFiberQueue) =>
        for {
          _ <- stateR.update { state =>
            state
              .focus(_.schedulerFiber.workers)
              .replace(f.initializedWorkerStates)
          }
          _ <- rpcServerFiberQueue.offer(
            new RpcServerFiberEvents.AllWorkersInitialized(replicatorAddrs)
          )
          schedulerHello <- grpc.registerWorker(f.workerHello, f.metadata)
        } yield {
          schedulerHello.mid should equal(f.wid.mid)
          schedulerHello.replicatorAddrs.view.mapValues(NetAddr.fromMsg(_)).toMap should equal(
            replicatorAddrs
          )
        }
      }
      .timeout(1.seconds)
  }

  it should "reply with failing SchedulerHello when AllWorkersInitialized event is received and worker netAddr is not in worker shared state" in {
    val f = fixture
    val replicatorAddrs = Map(
      0 -> new NetAddr("1.1.1.1", 4999),
      1 -> new NetAddr("1.1.1.2", 4999)
    )
    val badWorkerHello = new WorkerHello(
      wtid = 0,
      storageInfo = None,
      ip = "1.1.1.110",
      port = 5000
    )

    f.grpc
      .use { case (stateR, grpc, schedulerFiberQueue, rpcServerFiberQueue) =>
        for {
          _ <- stateR.update { state =>
            state
              .focus(_.schedulerFiber.workers)
              .replace(f.initializedWorkerStates)
          }
          _ <- rpcServerFiberQueue.offer(
            new RpcServerFiberEvents.AllWorkersInitialized(replicatorAddrs)
          )
          schedulerHello <- grpc.registerWorker(badWorkerHello, f.metadata)
        } yield {
          schedulerHello.success should be(false)
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
      .use { case (stateR, grpc, schedulerFiberQueue, _) =>
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
