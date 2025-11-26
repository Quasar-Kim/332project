package redsort.worker.handlers

import cats._
import cats.effect._
import cats.syntax.all._
import fs2.{Pipe, Stream}
import fs2.io.file.Path
import redsort.jobs.Common._
import redsort.jobs.context.interface._
import redsort.jobs.worker._

class JobSampler extends JobHandler {
  override def apply(
      args: Seq[com.google.protobuf.any.Any],
      inputs: Seq[Path],
      outputs: Seq[Path],
      ctx: FileStorage,
      d: Directories
  ): IO[Option[Array[Byte]]] = {
    val RECORD_SIZE = 100L
    val NUM_RECORDS = 10000L
    val SAMPLING_SIZE = RECORD_SIZE * NUM_RECORDS

    val inputpath = inputs.head

    val writePipesResource: Resource[IO, List[Pipe[IO, Byte, Unit]]] =
      outputs.toList.traverse { path =>
        ctx.create(path.toString)
      }

    val program: IO[Unit] = writePipesResource.use { pipes =>
      ctx
        .read(inputpath.toString)
        .take(SAMPLING_SIZE)
        .broadcastThrough(pipes: _*)
        .compile
        .drain
    }

    program.map { _ => Some("OK".getBytes()) }
  }
}
