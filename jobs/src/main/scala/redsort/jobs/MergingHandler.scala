package redsort.jobs.handler

import java.io.{File, FileInputStream, FileOutputStream}
import scala.util.{Try, Success, Failure}

import redsort.jobs.messages.Job._
import redsort.jobs.jobhelper._
import redsort.jobs.fileserver.FileStorage

object MergingHandler {
  def run(fs: FileStorage)(job: JobSpec): JobResult = {
    println(s"Merging job: ${job.jid}")
    JobDone.fail(job.jid)
  }
}
