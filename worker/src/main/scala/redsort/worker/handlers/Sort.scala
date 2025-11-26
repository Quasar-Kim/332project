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

import java.util.Arrays

class JobSorter extends JobHandler {

  private val RECORD_SIZE = 100 // 100 bytes

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

  // This handler can works with multiple input and output paths, even though
  // the original design assumes single input and multiple outputs.
  override def apply(
      args: Seq[com.google.protobuf.any.Any],
      inputs: Seq[Path],
      outputs: Seq[Path],
      ctx: FileStorage,
      d: Directories
  ): IO[Option[Array[Byte]]] = {

    val program: IO[Unit] = for {

      allRecords <- Stream
        .emits(inputs)
        .flatMap(path => ctx.read(path.toString))
        .chunkN(RECORD_SIZE, allowFewer = false)
        .map(_.toArray)
        .compile
        .to(Array)

      _ <- IO.blocking {
        Arrays.sort(allRecords, (a: Array[Byte], b: Array[Byte]) => compareBytes(a, b))
      }

      _ <- outputs.toList.parTraverse { path =>
        ctx.create(path.toString).use { sink =>
          Stream
            .emits(allRecords)
            .flatMap(record => Stream.chunk(Chunk.array(record)))
            .through(sink)
            .compile
            .drain
        }
      }

    } yield ()

    program.map { _ => Some("OK".getBytes()) }
  }
}
