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

class MergeJobHandler extends JobHandler {

  private val MAX_FILE_SIZE = 128 * 1000 * 1000 // 128 MB
  private val RECORD_SIZE = 100 // 100 bytes
  private val RECORDS_PER_FILE = MAX_FILE_SIZE / RECORD_SIZE

  case class Record(buf: Array[Byte]) {
    def key: ByteBuffer = ByteBuffer.wrap(buf.slice(0, 10))
    def value: ByteBuffer = ByteBuffer.wrap(buf.slice(10, 90))
  }

  implicit object RecordOrder extends Order[Record] {
    def compare(x: Record, y: Record): Int =
      x.key.compareTo(y.key)
  }

  override def apply(
      args: Seq[com.google.protobuf.any.Any],
      inputs: Seq[Path],
      outputs: Seq[Path],
      ctx: FileStorage,
      d: Directories
  ): IO[Option[Array[Byte]]] = {
    // create stream for each input files
    val inputStreams = inputs.map { path =>
      ctx.read(path.toString).chunkN(RECORD_SIZE).map(chunk => new Record(chunk.toArray))
    }

    // merge input stream into one stream
    val mergedStream = sortedMerge(inputStreams)

    // write contents of merged stream into multiple outputs, grouping contents
    // by max size.
    val writeStream = mergedStream
      .map(record => Chunk.array(record.buf))
      .chunkN(RECORDS_PER_FILE)
      .zipWithIndex
      .evalMap { case (chunk, index) =>
        val ouputPath = outputs(index.toInt)
        val stream = Stream.chunk(chunk)
        ctx.save(ouputPath.toString, stream.unchunks)
      }
    writeStream.compile.drain.map(_ => None)
  }

  /** Merge sorted streams into one sorted stream by combining streams in balanced binary tree
    * fashion.
    *
    * @param streams
    *   sequence of already sorted streams
    * @return
    *   sorted stream combining `streams`
    */
  def sortedMerge(streams: Seq[Stream[IO, Record]]): Stream[IO, Record] = {
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
