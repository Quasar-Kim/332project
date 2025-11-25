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

class JobPartitional extends JobHandler {

  private val RECORD_SIZE = 100 // 100 bytes

  override def apply(
      args: Seq[com.google.protobuf.any.Any],
      inputs: Seq[Path],
      outputs: Seq[Path],
      ctx: FileStorage,
      d: Directories
  ): IO[Option[Array[Byte]]] = {

    // ex) args: [100, 200] -> (,100], (100, 200]
    // assumption: 200 is the last max value
    def partitioner(record: Array[Byte]): Int = {
      val key = record.slice(0, 10)
      val keyStr = new String(key)
      val index = args.indexWhere { arg =>
        keyStr <= new String(arg.getValue.toByteArray)
      }
      if (index == -1) args.length else index
    }

    val program: IO[Unit] = for {
      inputPath = inputs.head
      outputPaths = outputs.map(ctx.write)

      


    } yield ()

    program.timed.attempt.map {
      case Right((duration, _)) =>
        println(s"[Partition] Job completed in ${duration.toMillis} ms")
        Some("OK".getBytes())
      case Left(err) =>
        println(s"[Partition] Job failed: ${err.getMessage}")
        Some("FAIL".getBytes())
    }
  }
}
