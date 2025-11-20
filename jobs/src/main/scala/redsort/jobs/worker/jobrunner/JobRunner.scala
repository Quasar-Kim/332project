package redsort.jobs.worker.jobrunner

import cats._
import cats.effect._
import cats.syntax._
import redsort.jobs.Common._
import redsort.jobs.messages._

import redsort.jobs.worker.jobrunner._

class JobRunner(handlers: Map[JobType, JobSpecMsg => IO[JobResult]]) {
  def runJob(job: JobSpecMsg): IO[JobResult] = {
    handlers.get(job.jobType) match {
      case Some(jobFunc) =>
        jobFunc(job).handleErrorWith { e =>
          for {
            _ <- IO.println(
              s"[JobRunner] Error while processing job type ${job.jobType}: ${e.getMessage}"
            )
          } yield JobResult(
            success = false
            // TODO
            // message = s"Job failed with error: ${e.getMessage}"
          )
        }

      case None =>
        for {
          _ <- IO.println(s"[JobRunner] No handler found for job type ${job.jobType}")
        } yield JobResult(
          success = false
          // TODO
          // message = s"No handler found for job type ${job.jobType}"
        )
    }
  }
}
