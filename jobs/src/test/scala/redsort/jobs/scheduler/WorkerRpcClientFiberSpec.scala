package redsort.jobs.scheduler

import cats.effect._
import org.scalamock.stubs.Stubs
import redsort.AsyncSpec
import redsort.jobs.Common._
import cats.effect.std.Queue
import redsort.jobs.context.interface.WorkerRpcClient
import redsort.jobs.messages.WorkerFs2Grpc
import io.grpc.Metadata
import redsort.jobs.messages.JobResult
import redsort.jobs.messages.JobExecutionStats
import scala.concurrent.duration._
import redsort.jobs.messages.WorkerError
import redsort.jobs.messages.WorkerErrorKind
import redsort.jobs.messages.JobSystemError
import com.google.protobuf.empty.Empty

class WorkerRpcClientFiberSpec extends AsyncSpec {
  def fixture =
    new {
      val sharedState = SharedState.init(
        Seq(new Wid(0, 0), new Wid(0, 1))
      )
      val ctxStub = stub[WorkerRpcClient]
      val workerfs2GrpcStub = stub[WorkerFs2Grpc[IO, Metadata]]
      (ctxStub.workerRpcClient _).returnsWith(Resource.eval(IO(workerfs2GrpcStub)))

      val inputQueue = Queue.unbounded[IO, WorkerFiberEvents]
      val schedulerFiberQueue = Queue.unbounded[IO, SchedulerFiberEvents]
    }

  val spec = new JobSpec(
    name = "test",
    args = Seq(),
    inputs = Seq(),
    outputs = Seq()
  )

  val successfulJobResult = new JobResult(
    success = true,
    retval = None,
    error = None,
    stats = Some(
      new JobExecutionStats(
        calculationTime = 1
      )
    )
  )

  val failingJobResult = new JobResult(
    success = false,
    retval = None,
    error = Some(
      new WorkerError(
        kind = WorkerErrorKind.BODY_ERROR,
        inner = Some(
          new JobSystemError(
            message = "some error",
            cause = None,
            context = Map()
          )
        )
      )
    ),
    stats = Some(
      new JobExecutionStats(
        calculationTime = 1
      )
    )
  )

  val wid = new Wid(0, 0)

  behavior of "worker RPC client fiber (upon receiving Initialized)"

  it should "establish connection to worker RPC server" in {
    val f = fixture

    val io = for {
      stateR <- f.sharedState
      inputQueue <- f.inputQueue
      schedulerFiberQueue <- f.schedulerFiberQueue

      // will set this deferred value once client fiber calls ctxStub.workerRpcClient
      deferred <- IO.deferred[Unit]
      _ <- IO((f.ctxStub.workerRpcClient _).returnsWith({
        deferred.complete(()).toResource.flatMap(_ => Resource.eval(IO(f.workerfs2GrpcStub)))
      }))

      _ <- WorkerRpcClientFiber
        .start(stateR, wid, inputQueue, schedulerFiberQueue, f.ctxStub)
        .useForever
        .background
        .use { _ =>
          inputQueue.offer(WorkerFiberEvents.Initialized(new NetAddr("1.2.3.4", 5000))) >>
            deferred.get
        }
    } yield ()
    io.timeout(1.second)
  }

  behavior of "worker RPC client fiber (upon receiving Job(spec))"

  it should "request worker to run the job" in {
    val f = fixture
    (f.workerfs2GrpcStub.runJob _).returnsWith(IO(successfulJobResult))

    val io = for {
      stateR <- f.sharedState
      inputQueue <- f.inputQueue
      schedulerFiberQueue <- f.schedulerFiberQueue
      event <- WorkerRpcClientFiber
        .start(stateR, wid, inputQueue, schedulerFiberQueue, f.ctxStub)
        .useForever
        .background
        .use { _ =>
          inputQueue.offer(WorkerFiberEvents.Initialized(new NetAddr("1.2.3.4", 5000))) >>
            inputQueue.offer(WorkerFiberEvents.Job(spec)) >>
            schedulerFiberQueue.take
        }
    } yield {
      assertResult(1, "runJob not called once") {
        (f.workerfs2GrpcStub.runJob _).calls.length
      }
      assertResult(JobSpec.toMsg(spec), "runJob not called with corret JobSpecMsg as argument") {
        (f.workerfs2GrpcStub.runJob _).calls(0)._1
      }
    }
    io.timeout(1.second)
  }

  it should "enqueue JobComplete event to scheduler fiber on success" in {
    val f = fixture
    (f.workerfs2GrpcStub.runJob _).returnsWith(IO(successfulJobResult))

    val io = for {
      stateR <- f.sharedState
      inputQueue <- f.inputQueue
      schedulerFiberQueue <- f.schedulerFiberQueue
      event <- WorkerRpcClientFiber
        .start(stateR, wid, inputQueue, schedulerFiberQueue, f.ctxStub)
        .useForever
        .background
        .use { _ =>
          inputQueue.offer(WorkerFiberEvents.Initialized(new NetAddr("1.2.3.4", 5000))) >>
            inputQueue.offer(WorkerFiberEvents.Job(spec)) >>
            schedulerFiberQueue.take
        }
    } yield {
      inside(event) { case SchedulerFiberEvents.JobCompleted(result, wid_) =>
        result should equal(successfulJobResult)
        wid_ should equal(wid)
      }
    }
    io.timeout(1.second)
  }

  it should "enqueue JobFailed event to scheduler fiber on job error" in {
    val f = fixture
    (f.workerfs2GrpcStub.runJob _).returnsWith(IO(failingJobResult))

    val io = for {
      stateR <- f.sharedState
      inputQueue <- f.inputQueue
      schedulerFiberQueue <- f.schedulerFiberQueue
      event <- WorkerRpcClientFiber
        .start(stateR, wid, inputQueue, schedulerFiberQueue, f.ctxStub)
        .useForever
        .background
        .use { _ =>
          inputQueue.offer(WorkerFiberEvents.Initialized(new NetAddr("1.2.3.4", 5000))) >>
            inputQueue.offer(WorkerFiberEvents.Job(spec)) >>
            schedulerFiberQueue.take
        }
    } yield {
      inside(event) { case SchedulerFiberEvents.JobFailed(result, from) =>
        result should equal(failingJobResult)
        from should equal(wid)
      }
    }
    io.timeout(1.second)
  }

  behavior of "worker RPC client fiber (upon receiving Complete)"

  it should "call Complete RPC method of worker and enqueue WorkerCompleted event to scheduler fiber" in {
    val f = fixture
    (f.workerfs2GrpcStub.complete _).returnsWith(IO(new Empty))

    val io = for {
      stateR <- f.sharedState
      inputQueue <- f.inputQueue
      schedulerFiberQueue <- f.schedulerFiberQueue
      evt <- WorkerRpcClientFiber
        .start(stateR, wid, inputQueue, schedulerFiberQueue, f.ctxStub)
        .useForever
        .background
        .use { _ =>
          inputQueue.offer(WorkerFiberEvents.Initialized(new NetAddr("1.2.3.4", 5000))) >>
            inputQueue.offer(WorkerFiberEvents.Complete) >>
            schedulerFiberQueue.take
        }
    } yield {
      evt should be(new SchedulerFiberEvents.WorkerCompleted(wid))
      (f.workerfs2GrpcStub.complete _).calls.length should be > 0
    }
    io.timeout(1.second)
  }

  // it should "flush messages until WorkerUp if WorkerDown was received before getting job result back" in {}

  // it should "enqueue WorkerNotResponding event to scheduler fiber on connection error" in {}

  // behavior of "scheduler fiber (upon receiving WorkerDown)"

  // it should "flush messages until WorkerUp" {}

  // it should "cancel RPC request" {}

  // behavior of "upon receiving WorkerUp" {}

  // it should "ignore message" {}
}
