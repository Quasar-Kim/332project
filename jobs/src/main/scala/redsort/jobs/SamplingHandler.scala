package redsort.jobs.handler

import java.io.{File, FileInputStream, FileOutputStream}
import scala.util.{Try, Success, Failure}

import redsort.jobs.messages.Job._
import redsort.jobs.jobhelper._

object SamplingHandler {
  def run(job: JobSpec): JobResult = {
    println(s"Sampling job: ${job.jid}")
    val SAMPLING_SIZE = 1 * 1000 * 1000 // 1MB, 100 Entry, 100 byte per entry
    job.inputs.headOption match {
      case None =>
        println(s"Error: No input files found for job ${job.jid}")
        JobDone.fail(job.jid)

      case Some(inputFile) =>
        val sampleData = new Array[Byte](SAMPLING_SIZE)
        val readBytesCount =
          Try {
            val fis = new FileInputStream(inputFile.path)
            try {
              fis.read(sampleData, 0, SAMPLING_SIZE)
            } finally {
              fis.close()
            }
          }
        readBytesCount match {
          case Failure(e) =>
            println(s"Error reading input file ${inputFile.path}: ${e.getMessage}")
            JobDone.fail(job.jid)

          case Success(bytesRead) if bytesRead <= 0 =>
            println(s"Warning: No data read from input file ${inputFile.path}")
            JobDone.fail(job.jid)

          case Success(bytesRead) =>
            JobDone.success(job.jid)
        }
    }
  }
}
