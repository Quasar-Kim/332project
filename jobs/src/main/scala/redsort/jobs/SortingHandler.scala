package redsort.jobs.handler

import java.io.{File, FileInputStream, FileOutputStream}
import scala.util.{Try, Success, Failure}

import redsort.jobs.messages.Job._
import redsort.jobs.jobhelper._

object SortingHandler {
  def run(job: JobSpec): JobResult = {
    println(s"Sorting job: ${job.jid}")
    JobDone.fail(job.jid)
  }
}
