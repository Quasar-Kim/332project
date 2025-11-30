package redsort.worker.handlers

import cats._
import cats.effect._
import cats.syntax.all._
import fs2.{Chunk, Stream}
import fs2.io.file.Path
import redsort.jobs.Common._
import redsort.jobs.context.interface._
import redsort.jobs.worker._

import java.util.Arrays

class SortJobHandler extends JobHandler {

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
  override def apply(
      args: Seq[com.google.protobuf.any.Any],
      inputs: Seq[Path],
      outputs: Seq[Path],
      ctx: FileStorage,
      d: Directories
  ): IO[Option[Array[Byte]]] = {
    val program: IO[Unit] = for {
      // Read all records into memory.
      allRecords <- Stream
        .emits(inputs)
        .flatMap(path => ctx.read(path.toString))
        .chunkN(RECORD_SIZE, allowFewer = false)
        .map(_.toArray)
        .compile
        .to(Array)

      // Sort records.
      _ <- IO.blocking {
        Arrays.sort(allRecords, (a: Array[Byte], b: Array[Byte]) => compareBytes(a, b))
      }

      _ <- outputs.toList.parTraverse { path =>
        val fileStream =
          Stream.emits(allRecords).flatMap(record => Stream.chunk(Chunk.array(record)))
        ctx.save(path.toString, fileStream)
      }
    } yield ()

    program.map(_ => None)
  }
}
