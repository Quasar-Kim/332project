package redsort.jobs.worker.jobrunner

import cats._
import cats.effect._
import cats.syntax._
import redsort.jobs.Common._
import redsort.jobs.messages._

import redsort.jobs.worker.jobrunner._

class JobRunner(handlers: Map[String, JobSpecMsg => IO[JobResult]]) {
  def runJob(job: JobSpecMsg): IO[JobResult] = {
    handlers.get(job.name) match {
      case Some(jobFunc) =>
        jobFunc(job).handleErrorWith { e =>
          for {
            _ <- IO.println(
              s"[JobRunner] Error while processing job type ${job.name}: ${e.getMessage}"
            )
          } yield JobResult(
            success = false,
            retval = None,
            error = Some(
              WorkerError(
                kind = WorkerErrorKind.BODY_ERROR,
                inner = Some(
                  JobSystemError(
                    message = s"Job failed with error: ${e.getMessage}"
                  )
                )
              )
            ),
            stats = None
          )
        }

      case None =>
        for {
          _ <- IO.println(s"[JobRunner] No handler found for job type ${job.name}")
        } yield JobResult(
          success = false,
          retval = None,
          error = Some(
            WorkerError(
              kind = WorkerErrorKind.BODY_ERROR,
              inner = Some(
                JobSystemError(
                  message = s"No handler found for job type ${job.name}"
                )
              )
            )
          ),
          stats = None
        )
    }
  }
}
