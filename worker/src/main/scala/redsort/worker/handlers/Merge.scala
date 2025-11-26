package redsort.worker.handlers

import cats._
import cats.effect._
import cats.syntax.all._
import fs2.{Chunk, Stream}
import fs2.io.file.Path
import redsort.jobs.Common._
import redsort.jobs.context.interface._
import redsort.jobs.worker._
import redsort.worker.logger

class JobMerger extends JobHandler {

  private val MAX_FILE_SIZE = 128 * 1000 * 1000 // 128 MB
  private val RECORD_SIZE = 100 // 100 bytes
  private val RECORDS_PER_FILE = MAX_FILE_SIZE / RECORD_SIZE

  private def compareBytes(a: Array[Byte], b: Array[Byte]): Int = {
    val len = Math.min(a.length, b.length)
    var i = 0
    while (i < len) {
      val diff = (a(i) & 0xff) - (b(i) & 0xff)
      if (diff != 0) return diff
      i += 1
    }
    a.length - b.length
  }

  def mergeSorted(
      s1: Stream[IO, Chunk[Byte]],
      s2: Stream[IO, Chunk[Byte]]
  ): Stream[IO, Chunk[Byte]] = {
    def go(
        p1: Option[(Chunk[Byte], Stream[IO, Chunk[Byte]])],
        p2: Option[(Chunk[Byte], Stream[IO, Chunk[Byte]])]
    ): Stream[IO, Chunk[Byte]] = {
      (p1, p2) match {
        case (Some((h1, t1)), Some((h2, t2))) =>
          if (compareBytes(h1.toArray, h2.toArray) <= 0) {
            Stream.emit(h1) ++ t1.pull.uncons1.flatMap(next => go(next, p2).pull.echo).stream
          } else {
            Stream.emit(h2) ++ t2.pull.uncons1.flatMap(next => go(p1, next).pull.echo).stream
          }
        case (Some((h1, t1)), None) =>
          Stream.emit(h1) ++ t1
        case (None, Some((h2, t2))) =>
          Stream.emit(h2) ++ t2
        case (None, None) =>
          Stream.empty
      }
    }
    (s1.pull.uncons1, s2.pull.uncons1).tupled.flatMap { case (p1, p2) =>
      go(p1, p2).pull.echo
    }.stream
  }

  def mergeAll(streams: Seq[Stream[IO, Chunk[Byte]]]): Stream[IO, Chunk[Byte]] = {
    if (streams.isEmpty) Stream.empty
    else streams.reduce(mergeSorted)
  }

  override def apply(
      args: Seq[com.google.protobuf.any.Any],
      inputs: Seq[Path],
      outputs: Seq[Path],
      ctx: FileStorage,
      d: Directories
  ): IO[Option[Array[Byte]]] = {

    val inputStreams: Seq[Stream[IO, Chunk[Byte]]] = inputs.map { path =>
      ctx.read(path.toString).chunkN(RECORD_SIZE)
    }
    val mergedStream: Stream[IO, Chunk[Byte]] = mergeAll(inputStreams)

    val program: IO[Unit] = for {

      _ <- mergedStream.zipWithIndex
        .groupAdjacentBy { case (_, idx) =>
          idx / RECORDS_PER_FILE
        }
        .zip(Stream.emits(outputs))
        .evalMap { case ((_, groupChunk), outputPath) =>
          Stream
            .chunk(groupChunk)
            .map(_._1)
            .flatMap(Stream.chunk)
            .through(ctx.write(outputPath.toString))
            .compile
            .drain
        }
        .compile
        .drain

    } yield ()

    program.timed.attempt.map {
      case Right((duration, _)) => Some("OK".getBytes())
      case Left(err)            => Some("FAIL".getBytes())
    }
  }
}
