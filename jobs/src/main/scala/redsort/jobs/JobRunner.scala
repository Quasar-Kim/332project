package redsort.jobs

import java.io.{File, FileInputStream, FileOutputStream}
import scala.util.{Try, Success, Failure}

import redsort.jobs.messages.Job._
import redsort.jobs.handler._
import redsort.jobs.jobhelper._

// class JobRunner(handlers: Map[JobType, JobSpec => JobResult]) {

// Dependency Injection (trait FileStorage)
class JobRunner(handlers: Map[JobType, JobSpec => JobResult]) {
  def runJob(job: JobSpec): JobResult = {
    handlers.get(job.jobType) match {
      case Some(jobFunc) =>
        Try(jobFunc(job)) match {
          case Success(result) => result
          case Failure(e)      => JobDone.fail(job.jid)
        }
      case None =>
        JobDone.fail(job.jid)
    }
  }
}
