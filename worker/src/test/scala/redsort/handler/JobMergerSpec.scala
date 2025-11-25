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

class JobMergerSpec extends AsyncFlatSpec with AsyncIOSpec with Matchers {

  "JobMerger" should "exactly merge 10,000 records (1MB)" in {

    val recordsPerFile = 1
    val input1Data = gensort.generate(recordsPerFile)
    val input2Data = gensort.generate(recordsPerFile)
    val data =
      input1Data.grouped(100).map(_.toList).toSet ++ input2Data.grouped(100).map(_.toList).toSet
    val sortinputStr = "/data/input1"
    val sortinputStr2 = "/data/input2"
    val sortoutputStr = "/data/output1"
    val sortoutputStr2 = "/data/output2"
    val mergedoutputStr = "/data/mergedoutput"
    val dummyDirs: Directories = null

    for {
      fs <- Ref.of[IO, Map[String, Array[Byte]]](Map.empty)
      ctx = new WorkerTestCtx(fs)
      sorter = new JobSorter()
      merger = new JobMerger()
      _ <- ctx.writeAll(sortinputStr, input1Data)
      _ <- ctx.writeAll(sortinputStr2, input2Data)

      resultOpt1 <- sorter.apply(
        args = Seq.empty,
        inputs = Seq(Path(sortinputStr)),
        outputs = Seq(Path(sortoutputStr)),
        ctx = ctx,
        d = dummyDirs
      )
      resultOpt2 <- sorter.apply(
        args = Seq.empty,
        inputs = Seq(Path(sortinputStr2)),
        outputs = Seq(Path(sortoutputStr2)),
        ctx = ctx,
        d = dummyDirs
      )
      resultOpt <- merger.apply(
        args = Seq.empty,
        inputs = Seq(Path(sortoutputStr), Path(sortoutputStr2)),
        outputs = Seq(Path(mergedoutputStr)),
        ctx = ctx,
        d = dummyDirs
      )

      middleBytes1 <- ctx.readAll(sortoutputStr)
      middleBytes2 <- ctx.readAll(sortoutputStr2)

      outputSize <- ctx.fileSize(mergedoutputStr)
      outputBytes <- ctx.readAll(mergedoutputStr)

    } yield {
      resultOpt1 shouldBe defined
      new String(resultOpt1.get) shouldBe "OK"
      resultOpt2 shouldBe defined
      new String(resultOpt2.get) shouldBe "OK"
      resultOpt shouldBe defined
      new String(resultOpt.get) shouldBe "OK"
      outputSize shouldBe (recordsPerFile * 2 * 100) // 200 records, 100 bytes each
      outputBytes.takeRight(2) shouldBe Array('\r'.toByte, '\n'.toByte)
      gensort.validate(middleBytes1) shouldBe true
      gensort.validate(middleBytes2) shouldBe true
      gensort.validate(outputBytes) shouldBe true
      outputBytes.grouped(100).map(_.toList).toSet shouldBe data
    }
  }

}
