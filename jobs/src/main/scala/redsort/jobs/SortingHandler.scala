package redsort.jobs.handler

import java.io._
import scala.util.{Try, Success, Failure}

import redsort.jobs.messages.Job._
import redsort.jobs.jobhelper._
import redsort.jobs.fileserver.FileStorage

object SortingHandler {

  private val KeySize = 10
  private val RecordSize = 100

  private def compareKeys(data: Array[Byte], offsetA: Int, offsetB: Int): Int = {
    var i = 0
    while (i < KeySize) {
      val a = data(offsetA + i) & 0xff
      val b = data(offsetB + i) & 0xff
      if (a != b) {
        return a - b
      }
      i += 1
    }
    0
  }

  def run(fs: FileStorage)(job: JobSpec): JobResult = {
    println(s"Sorting job: ${job.jid}")
    val inputPath: String = job.inputs.head.path
    val outputPath: String = job.outputs.head.path

    var inStream: InputStream = null
    var outStream: OutputStream = null

    val sortTry: Try[Unit] = Try {
      inStream = fs.inputStream(inputPath)

      // InMemory, IN PRODUCTION, REPLACE THIS WITH EXTERNAL SORTING ALGORITHM
      // val data = inStream.readAllBytes()
      // readAllBytes() is not supported in older Java versions (before Java 9)
      val dataBuffer = new ByteArrayOutputStream()
      val buffer = new Array[Byte](8192)
      var bytesRead = inStream.read(buffer)
      while (bytesRead != -1) {
        dataBuffer.write(buffer, 0, bytesRead)
        bytesRead = inStream.read(buffer)
      }
      val data = dataBuffer.toByteArray()
      // end of readAllBytes() replacement

      val numRecords = data.length / RecordSize
      if (data.length % RecordSize != 0) {
        println(s"Warning: File $inputPath size (${data.length}) is not a multiple of $RecordSize")
      }

      val offsets: Array[Int] = Array.tabulate(numRecords)(_ * RecordSize)
      val sortedoffsets =
        offsets.sortWith((offsetA, offsetB) => compareKeys(data, offsetA, offsetB) < 0)
      val sortedData = new Array[Byte](data.length)

      var i = 0
      while (i < numRecords) {
        val srcOffset = sortedoffsets(i)
        val destOffset = i * RecordSize
        System.arraycopy(data, srcOffset, sortedData, destOffset, RecordSize)
        i += 1
      }

      outStream = fs.outputStream(outputPath)
      outStream.write(sortedData)
    }

    val result = sortTry match {
      case Success(_) =>
        println(s"SortingHandler: Successfully sorted file $outputPath")
        JobDone.success(job.jid)
      case Failure(e) =>
        println(s"SortingHandler: Failed job ${job.jid}: ${e.getMessage}")
        JobDone.fail(job.jid)
    }

    try { if (inStream != null) inStream.close() }
    catch {
      case e: Exception => /* ignore */
    }
    try { if (outStream != null) outStream.close() }
    catch {
      case e: Exception => /* ignore */
    }

    result
  }
}
