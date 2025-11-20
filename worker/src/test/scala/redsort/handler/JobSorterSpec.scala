package redsort.worker.handlers

import cats.effect._
import cats.effect.testing.scalatest.AsyncIOSpec
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import fs2.io.file.{Files, Path}
import redsort.jobs.worker.filestorage._
import redsort.jobs.messages._
import java.nio.charset.StandardCharsets

import redsort.worker.gensort._

class JobSorterSpec extends AsyncFlatSpec with AsyncIOSpec with Matchers {

  def withStorage(
      ctx: AppContext = AppContext.testingContext
  )(testCode: (FileStorage[AppContext], String) => IO[Unit]): IO[Unit] = {
    FileStorage.create(ctx).flatMap { storage =>
      Files[IO]
        .tempDirectory(Some(Path("/tmp")), "redsort-test-", None)
        .use { tempPath =>
          testCode(storage, tempPath.absolute.toString)
        }
    }
  }

  "JobSorter" should "exactly sample 10,000 records (1MB)" in {
    withStorage() { (storage, root) =>
      val inputPath = s"$root/input_gensort"
      val outputPath = s"$root/output_sorted"
      val totalRecords = 10000
      val inputData = gensort.generate(totalRecords)

      inputData.length shouldBe (totalRecords * 100)

      val jobSpec = JobSpecMsg(
        name = "test-sorting",
        inputs = Seq(FileEntryMsg(path = inputPath)),
        outputs = Seq(FileEntryMsg(path = outputPath))
      )
      val sorter = new JobSorter(storage)

      for {
        _ <- storage.writeAll(inputPath, inputData)
        result <- sorter.run(jobSpec)
        outputSize <- storage.fileSize(outputPath)
        outputBytes <- storage.readAll(outputPath)
        valid = gensort.validate(outputBytes)
      } yield {
        result.success shouldBe true
        outputSize shouldBe (10000 * 100)
        outputBytes.takeRight(2) shouldBe Array('\r'.toByte, '\n'.toByte)
        valid shouldBe true
      }
    }
  }
}
