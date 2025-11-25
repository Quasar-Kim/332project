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

class JobSorterSpec extends AsyncFlatSpec with AsyncIOSpec with Matchers {

  "JobSorter" should "merge multiple inputs, sort them, and broadcast to all outputs" in {
    val inputPathStrs = List("/data/input_1", "/data/input_2")
    val outputPathStrs = List("/data/output_1", "/data/output_2", "/data/output_3")

    val inputPaths = inputPathStrs.map(Path(_))
    val outputPaths = outputPathStrs.map(Path(_))

    val recordsPerFile = 100
    val input1Data = gensort.generate(recordsPerFile)
    val input2Data = gensort.generate(recordsPerFile)
    val totalExpectedSize = (input1Data.length + input2Data.length).toLong

    val dummyDirs: Directories = null

    for {
      fs <- Ref.of[IO, Map[String, Array[Byte]]](Map.empty)
      ctx = new WorkerTestCtx(fs)
      sorter = new JobSorter()

      _ <- ctx.writeAll(inputPathStrs(0), input1Data)
      _ <- ctx.writeAll(inputPathStrs(1), input2Data)

      resultOpt <- sorter.apply(
        args = Seq.empty,
        inputs = inputPaths,
        outputs = outputPaths,
        ctx = ctx,
        d = dummyDirs
      )

      out1Bytes <- ctx.readAll(outputPathStrs(0))
      out2Bytes <- ctx.readAll(outputPathStrs(1))
      out3Bytes <- ctx.readAll(outputPathStrs(2))

    } yield {
      resultOpt shouldBe defined
      new String(resultOpt.get) shouldBe "OK"
      out1Bytes shouldBe out2Bytes
      out2Bytes shouldBe out3Bytes
      out1Bytes.length.toLong shouldBe totalExpectedSize
      gensort.validate(out1Bytes) shouldBe true
    }
  }
}
