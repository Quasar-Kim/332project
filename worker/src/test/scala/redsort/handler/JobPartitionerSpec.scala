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

import com.google.protobuf.any
import redsort.jobs.messages.BytesArg
import com.google.protobuf.any.{Any => ProtobufAny}
import com.google.protobuf.ByteString

import redsort.worker.testctx._

class JobPartitionerSpec extends AsyncFlatSpec with AsyncIOSpec with Matchers {

  val MAX_KEY = "~~~~~~~~~~~" // 11 bytes, all 10-byte keys are less than this

  // Left inclusive, right exclusive
  def isIn(key: String, start: String, last: String): Boolean = {
    (start.isEmpty || key >= start) && (last.isEmpty || key < last)
  }

  def isWellPartitioned(
      records: Array[Array[Byte]],
      start: String,
      last: String
  ): Boolean = {
    records.forall { record =>
      val key = new String(record.slice(0, 10))
      isIn(key, start, last)
    }
  }

  def makeArgFromBytes(bytes: Array[Byte]): ProtobufAny = {
    any.Any.pack(new BytesArg(value = ByteString.copyFrom(bytes)))
  }
  def makeArgFromString(str: String): ProtobufAny = {
    makeArgFromBytes(str.getBytes)
  }

  "JobPartitioner" should "separates simple data in the partition (left inclusive)" in {
    val inputContents = (
      "0000000001 " + "a" * 87 + "\r\n" +
        "0000000005 " + "b" * 87 + "\r\n" +
        "0000000010 " + "c" * 87 + "\r\n" +
        "0000000015 " + "d" * 87 + "\r\n" +
        "0000000020 " + "e" * 87 + "\r\n"
    ).getBytes()
    val inputPathStr = "/data/input_simple"
    val outputPathStrs = List("/data/partition1", "/data/partition2")
    val inputPath = Path(inputPathStr)
    val outputPaths = outputPathStrs.map(p => Path(p))
    val dummyDirs: Directories = null
    val argList = Seq("0000000010", MAX_KEY)
    val args = argList.map(makeArgFromString)

    for {
      fs <- Ref.of[IO, Map[String, Array[Byte]]](Map.empty)
      ctx = new WorkerTestCtx(fs)
      partitioner = new JobPartitioner()
      _ <- ctx.writeAll(inputPathStr, inputContents)

      resultOpt <- partitioner.apply(
        args = args,
        inputs = Seq(inputPath),
        outputs = outputPaths,
        ctx = ctx,
        d = dummyDirs
      )

      firstPartitionBytes <- ctx.readAll(outputPathStrs(0))
      secondPartitionBytes <- ctx.readAll(outputPathStrs(1))

      firstPartitionRecords = firstPartitionBytes.grouped(100).toArray
      secondPartitionRecords = secondPartitionBytes.grouped(100).toArray

    } yield {
      resultOpt shouldBe defined
      new String(resultOpt.get) shouldBe "OK"
      firstPartitionBytes shouldBe (
        "0000000001 " + "a" * 87 + "\r\n" +
          "0000000005 " + "b" * 87 + "\r\n"
      ).getBytes()
      secondPartitionBytes shouldBe (
        "0000000010 " + "c" * 87 + "\r\n" +
          "0000000015 " + "d" * 87 + "\r\n" +
          "0000000020 " + "e" * 87 + "\r\n"
      ).getBytes()
    }
  }

  it should "create the file even if it contains no content" in {
    val inputPathStr = "/data/input"
    val outputPathStrs = List("/data/partition1", "/data/partition2")
    val inputPath = Path(inputPathStr)
    val outputPaths = outputPathStrs.map(p => Path(p))
    val inputContents = (
      "0000000001 " + "a" * 87 + "\r\n" +
        "0000000005 " + "b" * 87 + "\r\n" +
        "0000000010 " + "c" * 87 + "\r\n" +
        "0000000015 " + "d" * 87 + "\r\n" +
        "0000000020 " + "e" * 87 + "\r\n"
    ).getBytes()
    val dummyDirs: Directories = null
    val argList = Seq("0000000030", MAX_KEY)
    val args = argList.map(makeArgFromString)

    for {
      fs <- Ref.of[IO, Map[String, Array[Byte]]](Map.empty)
      ctx = new WorkerTestCtx(fs)
      partitioner = new JobPartitioner()
      _ <- ctx.writeAll(inputPathStr, inputContents)

      resultOpt <- partitioner.apply(
        args = args,
        inputs = Seq(inputPath),
        outputs = outputPaths,
        ctx = ctx,
        d = dummyDirs
      )

      firstPartitionBytes <- ctx.readAll(outputPathStrs(0))
      secondPartitionBytes <- ctx.readAll(outputPathStrs(1))

      firstPartitionRecords = firstPartitionBytes.grouped(100).toArray
      secondPartitionRecords = secondPartitionBytes.grouped(100).toArray

    } yield {
      resultOpt shouldBe defined
      new String(resultOpt.get) shouldBe "OK"
      firstPartitionBytes shouldBe (
        "0000000001 " + "a" * 87 + "\r\n" +
          "0000000005 " + "b" * 87 + "\r\n" +
          "0000000010 " + "c" * 87 + "\r\n" +
          "0000000015 " + "d" * 87 + "\r\n" +
          "0000000020 " + "e" * 87 + "\r\n"
      ).getBytes()
      secondPartitionBytes shouldBe ("").getBytes()
    }
  }

  it should "separates exactly" in {
    val inputPathStr = "/data/input_gensort"
    val outputPathStrs = List("/data/partition1", "/data/partition2", "/data/partition3")
    val inputPath = Path(inputPathStr)
    val outputPaths = outputPathStrs.map(p => Path(p))
    val totalRecords = 10000
    val inputData = gensort.generate(totalRecords)
    val dummyDirs: Directories = null
    val argList = Seq("3333333333", "6666666666", MAX_KEY)
    val args = argList.map(makeArgFromString)

    for {
      fs <- Ref.of[IO, Map[String, Array[Byte]]](Map.empty)
      ctx = new WorkerTestCtx(fs)
      partitioner = new JobPartitioner()
      _ <- ctx.writeAll(inputPathStr, inputData)

      resultOpt <- partitioner.apply(
        args = args,
        inputs = Seq(inputPath),
        outputs = outputPaths,
        ctx = ctx,
        d = dummyDirs
      )

      firstPartitionBytes <- ctx.readAll(outputPathStrs(0))
      secondPartitionBytes <- ctx.readAll(outputPathStrs(1))
      thirdPartitionBytes <- ctx.readAll(outputPathStrs(2))
      firstPartitionRecords = firstPartitionBytes.grouped(100).toArray
      secondPartitionRecords = secondPartitionBytes.grouped(100).toArray
      thirdPartitionRecords = thirdPartitionBytes.grouped(100).toArray

    } yield {
      resultOpt shouldBe defined
      new String(resultOpt.get) shouldBe "OK"
      isWellPartitioned(
        firstPartitionRecords,
        start = "",
        last = "3333333333"
      ) shouldBe true
      isWellPartitioned(
        secondPartitionRecords,
        start = "3333333333",
        last = "6666666666"
      ) shouldBe true
      isWellPartitioned(
        thirdPartitionRecords,
        start = "6666666666",
        last = MAX_KEY
      ) shouldBe true

    }
  }

}
