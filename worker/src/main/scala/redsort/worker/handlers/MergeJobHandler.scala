package redsort.worker.handlers

import cats._
import cats.effect._
import cats.syntax.all._
import fs2.{Chunk, Stream}
import fs2.io.file.Path
import redsort.jobs.Common._
import redsort.jobs.context.interface._
import redsort.jobs.worker._
import java.nio.ByteBuffer
import com.google.protobuf.ByteString
import redsort.worker.handlers.Record
import redsort.jobs.messages.LongArg

class MergeJobHandler extends JobHandler {
  import MergeJobHandler._

  override def apply(
      args: Seq[com.google.protobuf.any.Any],
      inputs: Seq[Path],
      outputs: Seq[Path],
      ctx: FileStorage,
      d: Directories
  ): IO[Option[Array[Byte]]] = {

    for {
      // first argument is max size of each output partition files
      _ <- assertIO(args.length == 1, "output file size argument must be present")
      outFileSize <- IO(args(0).unpack[LongArg].value)
      _ <- assertIO(
        outFileSize % RECORD_SIZE == 0,
        "output file size must be divisible by record size (100)"
      )
      recordsPerFile = (outFileSize / RECORD_SIZE).toInt

      // create stream for each input files
      inputStreams = inputs.map { path =>
        ctx
          .read(path.toString)
          .chunkN(RECORD_SIZE)
      }
      // merge input stream into one stream
      mergedStream = sortedMerge(inputStreams)

      // write contents of merged stream into multiple outputs, grouping contents
      // by max size.
      writeStream = mergedStream
        .chunkN(recordsPerFile)
        .zipWithIndex
        .evalMap { case (chunk, index) =>
          val ouputPath = outputs(index.toInt)
          val stream = Stream.chunk(chunk)
          ctx.save(ouputPath.toString, stream.unchunks)
        }

      _ <- writeStream.compile.drain
    } yield None
  }

  implicit object RecordOrder extends Order[Chunk[Byte]] {
    override def compare(x: Chunk[Byte], y: Chunk[Byte]): Int = {
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

  /** Merge sorted streams into one sorted stream by combining streams in balanced binary tree
    * fashion.
    *
    * @param streams
    *   sequence of already sorted streams
    * @return
    *   sorted stream combining `streams`
    */
  def sortedMerge(streams: Seq[Stream[IO, Chunk[Byte]]]): Stream[IO, Chunk[Byte]] = {
    streams.size match {
      case 0 => Stream.empty
      case 1 => streams.head
      case _ => {
        val merged = streams
          .grouped(2)
          .map {
            case Seq(s1, s2) => s1.interleaveOrdered(s2)
            case Seq(s1)     => s1
          }
          .toSeq

        sortedMerge(merged)
      }
    }
  }
}

object MergeJobHandler {
  val RECORD_SIZE = 100 // 100 bytes
}
