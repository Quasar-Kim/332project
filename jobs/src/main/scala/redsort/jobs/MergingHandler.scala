package redsort.jobs.handler

import java.io.{File, FileInputStream, FileOutputStream}
import scala.util.{Try, Success, Failure}

import redsort.jobs.messages.Job._
import redsort.jobs.jobhelper._

object MergingHandler {
  def run(job: JobSpec): JobResult = {
    println(s"Merging job: ${job.jid}")
    JobDone.fail(job.jid)
  }
}
