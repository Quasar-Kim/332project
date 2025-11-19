package redsort.jobs.scheduler

import cats.effect._
import org.scalamock.stubs.Stubs
import redsort.FlatSpecBase
import redsort.jobs.Common._
import cats.effect.std.Queue
import redsort.jobs.context.interface.WorkerRpcClient
import redsort.jobs.messages.WorkerFs2Grpc
import io.grpc.Metadata
import redsort.jobs.messages.JobResult
import redsort.jobs.messages.JobExecutionStats
import scala.concurrent.duration._

class WorkerRpcClientFiberSpec extends FlatSpecBase {
  def fixture =
    new {
      val sharedState = SharedState.init(
        Map(
          new Wid(0, 0) -> new NetAddr("1.2.3.4", 5000),
          new Wid(0, 1) -> new NetAddr("1.2.3.4", 5001)
        )
      )
      val ctxStub = stub[WorkerRpcClient]
      val workerfs2GrpcStub = stub[WorkerFs2Grpc[IO, Metadata]]
      (ctxStub.workerRpcClient _).returnsWith(Resource.eval(IO(workerfs2GrpcStub)))

      val inputQueue = Queue.unbounded[IO, WorkerFiberEvents]
    }

  val spec = new JobSpec(
    name = "test",
    args = Seq(),
    inputs = Seq(),
    outputs = Seq(),
    outputSize = 0
  )

  val jobResult = new JobResult(
    success = true,
    retval = None,
    error = None,
    stats = Some(
      new JobExecutionStats(
        calculationTime = 1
      )
    )
  )

  "Scheduler Fiber" should "call RunJob RPC method of worker on receiving Job event" in {
    val f = fixture
    (f.workerfs2GrpcStub.runJob _).returnsWith(IO(jobResult))

    for {
      stateR <- f.sharedState
      inputQueue <- f.inputQueue
      _ <- WorkerRpcClientFiber.start(stateR, new Wid(0, 0), inputQueue, f.ctxStub).use { _ =>
        inputQueue.offer(WorkerFiberEvents.Job(spec)) >> IO.sleep(100.milli)
      }
    } yield {
      assertResult(JobSpec.toMsg(spec)) {
        (f.workerfs2GrpcStub.runJob _).calls(0)._1
      }
    }
  }
}
