package redsort.worker.handlers

import cats._
import cats.effect._
import cats.syntax.all._
import fs2.{Chunk, Stream}
import fs2.io.file.Path
import redsort.jobs.Common._
import redsort.jobs.messages._

import redsort.jobs.context.interface._
import redsort.jobs.worker._
import com.google.protobuf.any
import com.google.protobuf.any.{Any => ProtobufAny}

import java.util.Arrays
import cats.effect.std.Queue
import java.io.File
import com.google.protobuf.ByteString
import scala.math.Ordering.Implicits._
import fs2.io.file.Files
import fs2.Pipe

class PartitionJobHandler extends JobHandler {
  import PartitionJobHandler._
  override def apply(
      args: Seq[ProtobufAny],
      inputs: Seq[Path],
      outputs: Seq[Path],
      ctx: FileStorage,
      d: Directories
  ): IO[Option[Array[Byte]]] = {
    for {
      _ <- assertIO(inputs.length == 1, "input length must be 1")
      _ <- assertIO(outputs.length > 0, "output length must be longer than 0")

      contents <- ctx.readAll(inputs.head.toString)
      partitions = partitionsFromArgs(args)

      slices <- Stream
        .chunk(Chunk.array(contents))
        .chunkN(100)
        .groupAdjacentBy(findPartition(partitions))
        .covary[IO]
        .compile
        .toList
      sliceMap = slices.toMap

      _ <- outputs.zipWithIndex.map { case (p, i) =>
        ctx.save(p.toString, Stream.chunk(sliceMap.getOrElse(i, Chunk.empty)).unchunks)
      }.parSequence
    } yield None
  }
}

object PartitionJobHandler {
  val RECORD_SIZE = 100 // 100 bytes
  val MIN_KEY = ByteString.fromHex("00" * 10)
  val MAX_KEY = ByteString.fromHex("ff" * 11)

  def partitionsFromArgs(args: Seq[ProtobufAny]): Seq[Tuple2[Chunk[Byte], Chunk[Byte]]] = {
    val ends = args.map(_.unpack[BytesArg].value)
    ends.zipWithIndex.map { case (end, i) =>
      val start = if (i == 0) MIN_KEY else ends(i - 1)
      (Chunk.byteBuffer(start.asReadOnlyByteBuffer()), Chunk.byteBuffer(end.asReadOnlyByteBuffer()))
    }.toSeq
  }

  implicit object RecordOrder extends Order[Chunk[Byte]] {
    override def compare(x: Chunk[Byte], y: Chunk[Byte]): Int = {
      if (x == MAX_KEY) return 1
      else if (y == MAX_KEY) return -1

      var i = 0
      while (i < 10) {
        val a = x(i) & 0xff
        val b = y(i) & 0xff

        if (a != b) return a - b
        else i += 1
      }
      0
    }
  }

  def isInPartition(key: Chunk[Byte])(p: Tuple2[Chunk[Byte], Chunk[Byte]]): Boolean = {
    val result = (key >= p._1) && (key < p._2)
    result
  }

  def findPartition(partitions: Seq[Tuple2[Chunk[Byte], Chunk[Byte]]])(chunk: Chunk[Byte]): Int = {
    partitions.indexWhere(isInPartition(chunk))
  }
}
