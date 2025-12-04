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

      // Comment(jaehwan): Split into big chunks first.
      // If an entire big chunk belongs to a single partition, emit it as is.
      // If not, we can use existed logic to split into every records and assign partitions.
      // This optimization makes about 5x ~ 30x speedup in my test.
      // The input file MUST be sorted!
      slices = Stream
        .chunk(Chunk.array(contents))
        .chunkN(BULK_SIZE)
        .flatMap { bigChunk =>
          if (bigChunk.isEmpty) Stream.empty
          else {
            val firstRecord = bigChunk.take(RECORD_SIZE)
            val lastRecordOfs = bigChunk.size - RECORD_SIZE
            val lastRecord = bigChunk.drop(lastRecordOfs)
            val startPartition = findPartition(partitions)(firstRecord.take(10))
            val endPartition = findPartition(partitions)(lastRecord.take(10))
            if (startPartition == endPartition) {
              Stream.emit((startPartition, bigChunk))
            } else {
              Stream
                .chunk(bigChunk)
                .chunkN(RECORD_SIZE)
                .map(record => (findPartition(partitions)(record), record))
            }
          }
        }
        .compile
        .toList

      groupedSlices = slices.groupMap(_._1)(_._2)

      _ <- outputs.zipWithIndex.map { case (p, i) =>
        val chunksForPartition = groupedSlices.getOrElse(i, List.empty)
        ctx.save(p.toString, Stream.emits(chunksForPartition).unchunks)
      }.parSequence
    } yield None
  }
}

object PartitionJobHandler {
  val RECORD_SIZE = 100 // 100 bytes
  val MIN_KEY = ByteString.fromHex("00" * 10)
  val MAX_KEY = ByteString.fromHex("ff" * 11)
  val BULK_SIZE = 640 * 100 // 640 records, 64KB
  val MAX_KEY_CHUNK = Chunk.byteBuffer(MAX_KEY.asReadOnlyByteBuffer())

  def partitionsFromArgs(args: Seq[ProtobufAny]): Seq[Tuple2[Chunk[Byte], Chunk[Byte]]] = {
    val ends = args.map(_.unpack[BytesArg].value)
    ends.zipWithIndex.map { case (end, i) =>
      val start = if (i == 0) MIN_KEY else ends(i - 1)
      (Chunk.byteBuffer(start.asReadOnlyByteBuffer()), Chunk.byteBuffer(end.asReadOnlyByteBuffer()))
    }.toSeq
  }

  implicit object RecordOrder extends Order[Chunk[Byte]] {
    override def compare(x: Chunk[Byte], y: Chunk[Byte]): Int = {
      if (x == MAX_KEY_CHUNK) return 1
      else if (y == MAX_KEY_CHUNK) return -1

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
