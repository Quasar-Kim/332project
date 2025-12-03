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
import scala.math.Ordering.comparatorToOrdering
import scala.math.Ordering.Implicits._

import redsort.worker.testctx._
import redsort.worker.handlers.PartitionJobHandler._

class PartitionJobHandlerSpec extends AsyncFlatSpec with AsyncIOSpec with Matchers {
  def makeArgFromByteString(bytes: ByteString): ProtobufAny = {
    any.Any.pack(new BytesArg(value = bytes))
  }

  def fixture = new {
    val records = Seq(
      ByteString.fromHex("00000000000000000001").concat(ByteString.copyFromUtf8("a" * 88 + "\r\n")),
      ByteString.fromHex("00000000000000000005").concat(ByteString.copyFromUtf8("b" * 88 + "\r\n")),
      ByteString.fromHex("00000000000000000010").concat(ByteString.copyFromUtf8("c" * 88 + "\r\n")),
      ByteString.fromHex("00000000000000000015").concat(ByteString.copyFromUtf8("d" * 88 + "\r\n")),
      ByteString.fromHex("00000000000000000020").concat(ByteString.copyFromUtf8("e" * 88 + "\r\n"))
    )
    val inputContents = records.fold(ByteString.empty)((acc, s) => acc.concat(s))
  }

  // "PartitionJobHandler.isInPartition" should "determine whether key is in partition using lexicographical ordering" in {
  //   val key = ByteString.fromHex("11112233445566778899")
  //   val partition = (
  //     ByteString.fromHex("11112233445566778899"),
  //     MAX_KEY
  //   )
  //   isInPartition(key, partition) should be(true)
  // }

  "PartitionJobHandler.apply" should "separates simple data in the partition (left inclusive)" in {
    val f = fixture
    val inputPathStr = "/data/input_simple"
    val outputPathStrs = List("/data/partition1", "/data/partition2")
    val inputPath = Path(inputPathStr)
    val outputPaths = outputPathStrs.map(p => Path(p))
    val dummyDirs: Directories = null
    val argList = Seq(ByteString.fromHex("00000000000000000009"), MAX_KEY)
    val args = argList.map(makeArgFromByteString)

    for {
      fs <- Ref.of[IO, Map[String, Array[Byte]]](Map.empty)
      ctx = new WorkerTestCtx(fs)
      partitioner = new PartitionJobHandler()
      _ <- ctx.writeAll(inputPathStr, f.inputContents.toByteArray())

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
      firstPartitionBytes shouldBe (f.records(0).concat(f.records(1)).toByteArray())
      secondPartitionBytes shouldBe (
        f.records(2).concat(f.records(3)).concat(f.records(4)).toByteArray()
      )
    }
  }

  it should "create the file even if it contains no content" in {
    val f = fixture
    val inputPathStr = "/data/input"
    val outputPathStrs = List("/data/partition1", "/data/partition2")
    val inputPath = Path(inputPathStr)
    val outputPaths = outputPathStrs.map(p => Path(p))
    val dummyDirs: Directories = null
    val argList = Seq(ByteString.fromHex("00000000000000000030"), MAX_KEY)
    val args = argList.map(makeArgFromByteString)

    for {
      fs <- Ref.of[IO, Map[String, Array[Byte]]](Map.empty)
      ctx = new WorkerTestCtx(fs)
      partitioner = new PartitionJobHandler()
      _ <- ctx.writeAll(inputPathStr, f.inputContents.toByteArray())

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

      firstPartitionBytes shouldBe (f.inputContents.toByteArray())
      secondPartitionBytes shouldBe (Array.emptyByteArray)
    }
  }

  it should "handle case where only one partition is given" in {
    val f = fixture
    val inputPathStr = "/data/input_simple"
    val outputPathStrs = List("/data/partition1")
    val inputPath = Path(inputPathStr)
    val outputPaths = outputPathStrs.map(p => Path(p))
    val dummyDirs: Directories = null
    val argList = Seq(MAX_KEY)
    val args = argList.map(makeArgFromByteString)

    for {
      fs <- Ref.of[IO, Map[String, Array[Byte]]](Map.empty)
      ctx = new WorkerTestCtx(fs)
      partitioner = new PartitionJobHandler()
      _ <- ctx.writeAll(inputPathStr, f.inputContents.toByteArray())

      resultOpt <- partitioner.apply(
        args = args,
        inputs = Seq(inputPath),
        outputs = outputPaths,
        ctx = ctx,
        d = dummyDirs
      )

      firstPartitionBytes <- ctx.readAll(outputPathStrs(0))
    } yield {
      firstPartitionBytes shouldBe (f.inputContents.toByteArray())
    }
  }
}
