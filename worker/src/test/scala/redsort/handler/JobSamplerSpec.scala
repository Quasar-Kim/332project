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

class JobSamplerSpec extends AsyncFlatSpec with AsyncIOSpec with Matchers {

  "JobSampler" should "exactly sample 10,000 records (1MB)" in {
    val inputPathStr = "/data/input_gensort"
    val outputPathStr = "/data/output_sample"
    val inputPath = Path(inputPathStr)
    val outputPath = Path(outputPathStr)

    val totalRecords = 10005
    val inputData = gensort.generate(totalRecords)
    val dummyDirs: Directories = null

    for {
      fs <- Ref.of[IO, Map[String, Array[Byte]]](Map.empty)
      ctx = new WorkerTestCtx(fs)
      sampler = new JobSampler()
      _ <- ctx.writeAll(inputPathStr, inputData)

      resultOpt <- sampler.apply(
        args = Seq.empty,
        inputs = Seq(inputPath),
        outputs = Seq(outputPath),
        ctx = ctx,
        d = dummyDirs
      )

      outputSize <- ctx.fileSize(outputPathStr)
      outputBytes <- ctx.readAll(outputPathStr)

    } yield {
      outputSize shouldBe (10000 * 100)
      outputBytes.takeRight(2) shouldBe Array('\r'.toByte, '\n'.toByte)
    }
  }

  it should "sample fewer records if file is smaller than 1MB" in {
    val inputPathStr = "/data/input_gensort"
    val outputPathStr = "/data/output_sample"
    val inputPath = Path(inputPathStr)
    val outputPath = Path(outputPathStr)

    val totalRecords = 100
    val inputData = gensort.generate(totalRecords)
    val dummyDirs: Directories = null

    for {
      fs <- Ref.of[IO, Map[String, Array[Byte]]](Map.empty)
      ctx = new WorkerTestCtx(fs)
      sampler = new JobSampler()
      _ <- ctx.writeAll(inputPathStr, inputData)

      resultOpt <- sampler.apply(
        args = Seq.empty,
        inputs = Seq(inputPath),
        outputs = Seq(outputPath),
        ctx = ctx,
        d = dummyDirs
      )

      outputSize <- ctx.fileSize(outputPathStr)
      outputBytes <- ctx.readAll(outputPathStr)

    } yield {
      outputSize shouldBe (100 * 100)
      outputBytes.takeRight(2) shouldBe Array('\r'.toByte, '\n'.toByte)
    }

  }

  it should "broadcasts to several output files" in {
    val inputPathStr = "/data/input_gensort"
    val outputPathStrs =
      List("/data/output_sample_1", "/data/output_sample_2", "/data/output_sample_3")
    val inputPath = Path(inputPathStr)
    val outputPaths = outputPathStrs.map(p => Path(p))

    val totalRecords = 20000
    val inputData = gensort.generate(totalRecords)
    val dummyDirs: Directories = null

    for {
      fs <- Ref.of[IO, Map[String, Array[Byte]]](Map.empty)
      ctx = new WorkerTestCtx(fs)
      sampler = new JobSampler()
      _ <- ctx.writeAll(inputPathStr, inputData)

      resultOpt <- sampler.apply(
        args = Seq.empty,
        inputs = Seq(inputPath),
        outputs = outputPaths,
        ctx = ctx,
        d = dummyDirs
      )

      firstOutputSize <- ctx.fileSize(outputPathStrs(0))
      firstOutputBytes <- ctx.readAll(outputPathStrs(0))

      secondOutputSize <- ctx.fileSize(outputPathStrs(1))
      secondOutputBytes <- ctx.readAll(outputPathStrs(1))

      thirdOutputSize <- ctx.fileSize(outputPathStrs(2))
      thirdOutputBytes <- ctx.readAll(outputPathStrs(2))

    } yield {
      firstOutputSize shouldBe (10000 * 100)
      firstOutputBytes.takeRight(2) shouldBe Array('\r'.toByte, '\n'.toByte)
      firstOutputBytes shouldBe secondOutputBytes
      firstOutputBytes shouldBe thirdOutputBytes
    }
  }
}
