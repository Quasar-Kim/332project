package redsort.jobs.handler

import java.io.{File, FileInputStream, FileOutputStream}
import scala.util.{Try, Success, Failure}

import redsort.jobs.messages.Job._
import redsort.jobs.jobhelper._
import redsort.jobs.fileserver.FileStorage

object SamplingHandler {
  def run(fs: FileStorage)(job: JobSpec): JobResult = {
    println(s"Sampling job: ${job.jid}")
    val SAMPLING_SIZE = 1 * 1000 * 1000
    val inputfile: FileEntry = job.inputs.head
    val outputfile: FileEntry = job.outputs.head

    val inputfileName: String = inputfile.path
    val outputfileName: String = outputfile.path

    val inputs: Option[Array[Byte]] = fs.readFile(inputfileName)

    inputs match {
      case None =>
        println(s"SamplingHandler: Failed to read input file $inputfileName")
        JobDone.fail(job.jid)
      case Some(data) =>
        val sampleSize = Math.min(SAMPLING_SIZE, data.length)
        val sampleData = data.slice(0, sampleSize)
        val writeSuccess = fs.writeFile(outputfileName, sampleData)
        if (writeSuccess) {
          println(s"SamplingHandler: Successfully wrote sample file $outputfileName")
          JobDone.success(job.jid)
        } else {
          println(s"SamplingHandler: Failed to write sample file $outputfileName")
          JobDone.fail(job.jid)
        }
    }
  }
}
