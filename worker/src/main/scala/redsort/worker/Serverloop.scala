package redsort.worker.serverloop

import redsort.worker.config.Config

import redsort.jobs.messages.Job._
import redsort.jobs.JobRunner
import redsort.jobs.handler._
import scala.concurrent._

// class WorkerServiceImpl(implicit ec: ExecutionContext) extends WorkerServiceGrpc.WorkerService {

class WorkerServiceImpl(
    otherWorkerStub: Option[WorkerServiceGrpc.WorkerServiceStub] = None
)(implicit ec: ExecutionContext)
    extends WorkerServiceGrpc.WorkerService {

  val jobHandlers: Map[JobType, JobSpec => JobResult] = Map(
    JobType.Sampling -> SamplingHandler.run,
    JobType.Sorting -> SortingHandler.run,
    JobType.Partitioning -> PartitioningHandler.run,
    JobType.Merging -> MergingHandler.run
  )
  val Runner = new JobRunner(jobHandlers)

  override def submitJob(request: JobSpec): Future[JobResult] = {
    println(s"[Worker] Job received: JID=${request.jid}")
    val jid = request.jid
    Future {
      val processedOutputs = Runner.runJob(request)
      println(s"[Worker] Complete job: JID=${jid}")
      processedOutputs
    }
  }
}
