import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.concurrent.ScalaFutures
import org.scalamock.scalatest.MockFactory
import scala.concurrent.{ExecutionContext, Future}

import redsort.jobs.messages.Job._
import redsort.worker
import redsort.worker.config.Config
import redsort.worker.serverloop._
import java.io.FileOutputStream
import scala.sys.process._
import redsort.jobs.fileserver._

import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.time.{Span, Seconds}

class WorkerServiceImplSpec extends AnyFlatSpec with Matchers with ScalaFutures with MockFactory {

  implicit val ec: ExecutionContext = ExecutionContext.global

  "WorkerServiceImpl" should "handle sampling jobs (test)" in {
    val inputFileEntry = FileEntry(
      path = "50MB.txt",
      size = 50 * 1000 * 1000,
      replicas = Seq.empty
    )
    val outputFileEntry = FileEntry(
      path = "sampled-50MB.txt",
      size = 1 * 1000 * 1000,
      replicas = Seq.empty
    )
    val samplingJob = JobSpec(
      jobType = JobType.Sampling,
      args = Seq.empty,
      inputs = Seq(inputFileEntry),
      outputs = Seq(outputFileEntry),
      jid = "job-sampling-001"
    )
    val fs = new InMemoryFileStorage
    fs.setup() // debugging & testing purpose
    val service = new WorkerServiceImpl(fs)

    whenReady(service.submitJob(samplingJob)) { result =>
      result.success shouldBe true
      fs.listFiles().contains("sampled-50MB.txt") shouldBe true
      fs.fileSize("50MB.txt") shouldBe Some(50 * 1000 * 1000)
      fs.fileSize("sampled-50MB.txt") shouldBe Some(1 * 1000 * 1000)
    }
  }

  it should "handle sorting jobs (test)" in {
    val inputFileEntry = FileEntry(
      path = "50MB.txt",
      size = 50 * 1000 * 1000,
      replicas = Seq.empty
    )
    val outputFileEntry = FileEntry(
      path = "sorted-50MB.txt",
      size = 50 * 1000 * 1000,
      replicas = Seq.empty
    )
    val sortingJob = JobSpec(
      jobType = JobType.Sorting,
      args = Seq.empty,
      inputs = Seq(inputFileEntry),
      outputs = Seq(outputFileEntry),
      jid = "job-sorting-001"
    )
    val fs = new InMemoryFileStorage
    fs.setup() // debugging & testing purpose
    val service = new WorkerServiceImpl(fs)

    whenReady(service.submitJob(sortingJob), timeout = Timeout(scaled(Span(10, Seconds)))) {
      result =>
        result.success shouldBe true
        fs.listFiles().contains("sorted-50MB.txt") shouldBe true
        fs.fileSize("50MB.txt") shouldBe Some(50 * 1000 * 1000)
        fs.fileSize("sorted-50MB.txt") shouldBe Some(50 * 1000 * 1000)

        // Save the contents of sorted-50MB.txt to local file for manual verification
        val sortedDataOpt = fs.readFile("sorted-50MB.txt")
        sortedDataOpt match {
          case Some(data) =>
            val fos = new FileOutputStream("./test_output/sorted-50MB-output.txt")
            fos.write(data)
            fos.close()
            try {
              "./gensort/64/valsort ./test_output/sorted-50MB-output.txt".!! // manual verification
            } catch {
              case e: Exception => fail(s"Manual verification failed: ${e.getMessage}")

            }
          case None =>
            fail("Failed to read sorted-50MB.txt from InMemoryFileStorage")
        }
    }
  }
}
