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
  "Scheduler Fiber" should "call RunJob RPC method on receiving Job event" in {
    val wid = new Wid(0, 0)
    val spec = new JobSpec(
      name = "test",
      args = Seq(),
      inputs = Seq(),
      outputs = Seq(),
      outputSize = 0
    )
    val result = new JobResult(
      success = true,
      retval = None,
      error = None,
      stats = Some(
        new JobExecutionStats(
          calculationTime = 1
        )
      )
    )

    val ctx = stub[WorkerRpcClient]
    val innerStub = stub[WorkerFs2Grpc[IO, Metadata]]
    (innerStub.runJob _).returnsWith(IO(result))
    (ctx.workerRpcClient _).returnsWith(Resource.eval(IO(innerStub)))

    for {
      stateR <- SharedState.init(Map(wid -> new NetAddr("1.2.3.4", 5000)))
      queue <- Queue.unbounded[IO, WorkerFiberEvents]
      _ <- WorkerRpcClientFiber.start(stateR, wid, queue, ctx).use { _ =>
        queue.offer(WorkerFiberEvents.Job(spec)) >> IO.sleep(100.milli)
      }
    } yield {
      assertResult(JobSpec.toMsg(spec)) {
        (innerStub.runJob _).calls(0)._1
      }
    }
  }
}
