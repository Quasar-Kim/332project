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

    // pipe that writes chunk that is in `partition` to `path`.
    def outputPipe(
        path: Path,
        partition: Tuple2[ByteString, ByteString]
    ): Pipe[IO, Chunk[Byte], Unit] =
      (inStream: Stream[IO, Chunk[Byte]]) => {
        val sink = ctx.create(path.toString)
        inStream
          .filter { chunk =>
            val key = ByteString.copyFrom(chunk.toArray.slice(0, 10))
            isInPartition(key, partition)
          }
          .unchunks
          .through(sink)
      }

    for {
      _ <- assertIO(inputs.length == 1, "input length must be 1")
      _ <- assertIO(outputs.length > 0, "output length must be longer than 0")

      // Create output pipes for each output files
      partitions = partitionsFromArgs(args)
      outPipes = outputs.lazyZip(partitions).map { case (path, partition) =>
        outputPipe(path, partition)
      }

      // Construct and run stream that reads from input file and partition it into outupt files
      stream =
        ctx
          .read(inputs.head.toString)
          .chunkN(RECORD_SIZE, allowFewer = false)
          .broadcastThrough(outPipes: _*)
      _ <- stream.compile.drain
    } yield None
  }
}

object PartitionJobHandler {
  val RECORD_SIZE = 100 // 100 bytes
  val MIN_KEY = ByteString.fromHex("00" * 10)
  val MAX_KEY = ByteString.fromHex("ff" * 11)

  implicit val byteStringComparator: Ordering[ByteString] =
    Ordering.comparatorToOrdering(ByteString.unsignedLexicographicalComparator())

  def partitionsFromArgs(args: Seq[ProtobufAny]): Seq[Tuple2[ByteString, ByteString]] = {
    val ends = args.map(_.unpack[BytesArg].value)
    ends.zipWithIndex.map { case (end, i) =>
      val start = if (i == 0) MIN_KEY else ends(i - 1)
      (start, end)
    }.toSeq
  }

  def isInPartition(key: ByteString, p: Tuple2[ByteString, ByteString]): Boolean = {
    val result = (key >= p._1) && (key < p._2)
    result
  }
}
