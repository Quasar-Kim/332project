package redsort.jobs.worker

import cats.effect._
import redsort.AsyncSpec
import redsort.jobs.workers.SharedState
import redsort.jobs.context.WorkerCtx
import io.grpc.Server
import redsort.jobs.worker.jobrunner.JobRunner
import redsort.jobs.scheduler.JobSpec
import io.grpc.Metadata
import redsort.jobs.messages.JobResult
import redsort.jobs.messages.JobSystemError
import redsort.jobs.JobSystemException
import redsort.jobs.SourceLogger
import org.log4s._
import com.google.protobuf.empty.Empty

class WorkerServerFiberSpec extends AsyncSpec {
  def fixture = new {
    val getSharedState = SharedState.init
    val jobRunnerStub = stub[JobRunner]
    val getService = for {
      stateR <- getSharedState
      completed <- IO.deferred[Unit]
    } yield WorkerRpcService.init(stateR, jobRunnerStub, new SourceLogger(getLogger), completed)
  }

  behavior of "WorkerRpcService.runJob"

  it should "return job execution result" in {
    val f = fixture
    val jobSpec = new JobSpec(
      name = "job",
      args = Seq(),
      inputs = Seq(),
      outputs = Seq()
    )
    val jobResult = new JobResult(
      success = true,
      retval = None,
      error = None,
      stats = None
    )
    (f.jobRunnerStub.runJob _).returnsWith(IO.pure(jobResult))

    for {
      service <- f.getService
      result <- service.runJob(JobSpec.toMsg(jobSpec), new Metadata)
    } yield {
      result should be(jobResult)
    }
  }

  behavior of "WorkerRpcService.halt"

  it should "raise received error" in {
    val f = fixture
    val err = new JobSystemError(
      message = "some error",
      cause = None,
      context = Map("source" -> "test")
    )

    for {
      service <- f.getService
      result <- service.halt(err, new Metadata).attempt
    } yield {
      result match {
        case Left(JobSystemException(msg, src, ctx, cause)) => {
          msg should be("some error")
          ctx should be(Map("source" -> "test"))
        }
        case _ => fail("halt did not raised JobSystemException")
      }
    }
  }
}
