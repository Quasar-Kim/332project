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
import redsort.worker.handlers.Record

class PartitionJobHandlerSpec extends AsyncFlatSpec with AsyncIOSpec with Matchers {
  def makeArgFromByteString(bytes: ByteString): ProtobufAny = {
    any.Any.pack(new BytesArg(value = bytes))
  }

  def compareKeys(x: Array[Byte], y: Array[Byte]): Boolean = {
    // x <= y : true
    var i = 0
    while (i < 10) {
      val a = x(i) & 0xff
      val b = y(i) & 0xff
      if (a < b) return true
      else if (a > b) return false
      else i += 1
    }
    true
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

  it should "handle large input" in {
    def fixtureLarge = new {
      val records = (1 to 100000).map { i =>
        val keyHex = "%020d".format(i)
        ByteString.fromHex(keyHex).concat(ByteString.copyFromUtf8("x" * 88 + "\r\n"))
      }
      val inputContents = records.fold(ByteString.empty)((acc, s) => acc.concat(s))
    }
    val f = fixtureLarge
    val inputPathStr = "/data/input_simple"
    val outputPathStrs = List("/data/partition1", "/data/partition2")
    val inputPath = Path(inputPathStr)
    val outputPaths = outputPathStrs.map(p => Path(p))
    val dummyDirs: Directories = null
    val argList = Seq(ByteString.fromHex("00000000000000080000"), MAX_KEY)
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

      _ <- IO.println(
        s"First partition size: ${firstPartitionBytes.length}, Second partition size: ${secondPartitionBytes.length}"
      )

      lastOfFirst = firstPartitionBytes.takeRight(100).take(10).toArray
      firstOfSecond = secondPartitionBytes.take(100).take(10).toArray
      separator = ByteString.fromHex("00000000000000080000").toByteArray
    } yield {
      firstPartitionBytes ++ secondPartitionBytes shouldBe (f.inputContents.toByteArray())
      compareKeys(lastOfFirst, separator) shouldBe true
      compareKeys(separator, firstOfSecond) shouldBe true
      compareKeys(lastOfFirst, firstOfSecond) shouldBe true
    }
  }

  it should "benchmark throughput with 500k records (~50MB)" in {
    val numRecords = 500000
    val recordSize = 100
    val keySize = 10
    val commonPayload = ("x" * 88 + "\r\n").getBytes("UTF-8")
    val totalSize = numRecords * recordSize

    val inputBytes = new Array[Byte](totalSize)
    val buffer = java.nio.ByteBuffer.wrap(inputBytes)

    for (i <- 0 until numRecords) {
      val keyBytes = new Array[Byte](keySize)
      var value = i
      var k = keySize - 1
      while (value > 0 && k >= 0) {
        keyBytes(k) = (value & 0xff).toByte
        value >>= 8
        k -= 1
      }
      buffer.put(keyBytes)
      buffer.put(commonPayload)
    }

    val inputPathStr = "/data/input_bench"
    val outputPathStrs = List("/data/part_bench_1", "/data/part_bench_2")
    val outputPaths = outputPathStrs.map(Path(_))
    val dummyDirs: Directories = null

    val midBytes = new Array[Byte](keySize)
    var midVal = numRecords / 2
    var k = keySize - 1
    while (midVal > 0 && k >= 0) {
      midBytes(k) = (midVal & 0xff).toByte
      midVal >>= 8
      k -= 1
    }

    val midKeyArg = makeArgFromByteString(ByteString.copyFrom(midBytes))
    val args = Seq(midKeyArg, makeArgFromByteString(MAX_KEY))

    for {
      fs <- Ref.of[IO, Map[String, Array[Byte]]](Map.empty)
      ctx = new WorkerTestCtx(fs)
      partitioner = new PartitionJobHandler()

      _ <- ctx.writeAll(inputPathStr, inputBytes)
      _ <- IO.println(
        s"Starting benchmark with ${numRecords} records (${totalSize / 1024 / 1024} MB)..."
      )

      start <- IO(System.nanoTime())
      _ <- partitioner.apply(args, Seq(Path(inputPathStr)), outputPaths, ctx, dummyDirs)
      end <- IO(System.nanoTime())

      part1 <- ctx.readAll(outputPathStrs(0))
      part2 <- ctx.readAll(outputPathStrs(1))
    } yield {
      val durationSeconds = (end - start) / 1e9
      val throughputMBps = (totalSize / 1024.0 / 1024.0) / durationSeconds

      println(f"Benchmark Finished in $durationSeconds%.4f s")
      println(f"Throughput: $throughputMBps%.2f MB/s")

      (part1.length + part2.length) shouldBe totalSize
      part1.length should be > 0
      part2.length should be > 0
      compareKeys(
        part1.takeRight(100).take(10).toArray,
        midBytes
      ) shouldBe true
      compareKeys(
        midBytes,
        part2.take(100).take(10).toArray
      ) shouldBe true
    }
  }
}
