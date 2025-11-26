package redsort.worker.handlers

import cats.effect._
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import fs2.{Pipe, Stream}
import fs2.io.file.{Files, Path}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import redsort.jobs.Common.FileEntry
import redsort.jobs.worker.Directories
import redsort.worker.gensort.gensort

import redsort.jobs.context._
import redsort.jobs.context.interface._
import redsort.jobs.context.impl._

import redsort.worker.testctx._

class SampleJobHandlerSpec extends AsyncFlatSpec with AsyncIOSpec with Matchers {

  "JobSampler" should "exactly sample 10,000 records (1MB)" in {
    val inputPathStr = "/data/input_gensort"
    val inputPath = Path(inputPathStr)

    val totalRecords = 10005
    val inputData = gensort.generate(totalRecords)
    val dummyDirs: Directories = null

    for {
      fs <- Ref.of[IO, Map[String, Array[Byte]]](Map.empty)
      ctx = new WorkerTestCtx(fs)
      sampler = new SampleJobHandler()
      _ <- ctx.writeAll(inputPathStr, inputData)

      resultOpt <- sampler.apply(
        args = Seq.empty,
        inputs = Seq(inputPath),
        outputs = Seq(),
        ctx = ctx,
        d = dummyDirs
      )
    } yield {
      val output = resultOpt.get
      output.size should be(10000 * 100)
      output(999_998) should be('\r'.toByte)
      output(999_999) should be('\n'.toByte)
    }
  }

  it should "sample fewer records if file is smaller than 1MB" in {
    val inputPathStr = "/data/input_gensort"
    val inputPath = Path(inputPathStr)

    val totalRecords = 100
    val inputData = gensort.generate(totalRecords)
    val dummyDirs: Directories = null

    for {
      fs <- Ref.of[IO, Map[String, Array[Byte]]](Map.empty)
      ctx = new WorkerTestCtx(fs)
      sampler = new SampleJobHandler()
      _ <- ctx.writeAll(inputPathStr, inputData)

      resultOpt <- sampler.apply(
        args = Seq.empty,
        inputs = Seq(inputPath),
        outputs = Seq(),
        ctx = ctx,
        d = dummyDirs
      )
    } yield {
      val output = resultOpt.get
      output.size should be(100 * 100)
      output(9998) should be('\r'.toByte)
      output(9999) should be('\n'.toByte)
    }
  }
}
