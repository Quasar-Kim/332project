package redsort.worker.handlers

import cats._
import cats.effect._
import cats.syntax.all._
import fs2.{Pipe, Stream}
import fs2.io.file.Path
import redsort.jobs.Common._
import redsort.jobs.context.interface._
import redsort.jobs.worker._
import fs2.Chunk

class SampleJobHandler extends JobHandler {
  val RECORD_SIZE = 100L
  val NUM_RECORDS = 10000L
  val SAMPLING_SIZE = RECORD_SIZE * NUM_RECORDS

  override def apply(
      args: Seq[com.google.protobuf.any.Any],
      inputs: Seq[Path],
      outputs: Seq[Path],
      ctx: FileStorage,
      d: Directories
  ): IO[Option[Array[Byte]]] = {
    for {
      _ <- assertIO(inputs.size == 1, "input size must be 1")
      chunk <- ctx.read(inputs.head.toString).take(SAMPLING_SIZE).compile.to(Chunk)
    } yield Some(chunk.toArray)
  }
}
