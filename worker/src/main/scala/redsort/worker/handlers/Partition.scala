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
import redsort.worker.logger

class JobPartitioner extends JobHandler {

  private val RECORD_SIZE = 100 // 100 bytes

  override def apply(
      args: Seq[ProtobufAny],
      inputs: Seq[Path],
      outputs: Seq[Path],
      ctx: FileStorage,
      d: Directories
  ): IO[Option[Array[Byte]]] = {

    val arguments = args.map(_.unpack[BytesArg])

    val partitionKeys: Vector[String] = arguments.map { arg =>
      new String(arg.value.toByteArray)
    }.toVector

    // ex) args: [100, MAX_KEY] -> [,100), [100, MAX_KEY)
    def getPartitionIndex(recordBytes: Array[Byte]): Int = {
      val keyBytes = recordBytes.slice(0, 10)
      val keyStr = new String(keyBytes)

      val index = partitionKeys.indexWhere(k => keyStr < k)
      if (index == -1) partitionKeys.length else index
      // returning partitionKeys.length means something wrong
    }

    val program: IO[Unit] = for {
      queue <- outputs.toVector.traverse(_ => Queue.unbounded[IO, Option[Chunk[Byte]]])
      writers = queue.zip(outputs).map { case (q, path) =>
        Stream
          .resource(ctx.create(path.toString))
          .flatMap { sink =>
            Stream.fromQueueNoneTerminated(q).flatMap(Stream.chunk).through(sink)
          }
          .compile
          .drain
      }

      input = inputs.head
      reader = Stream
        .emit(input)
        .flatMap(path => ctx.read(path.toString))
        .chunkN(RECORD_SIZE)
        .evalMap { chunk =>
          val recordBytes = chunk.toArray
          val partIndex = getPartitionIndex(recordBytes)
          queue(partIndex).offer(Some(chunk))
        }
        .compile
        .drain
        .guarantee {
          queue.traverse_(q => q.offer(None))
        }
      _ <- (reader :: writers.toList).parSequence_

    } yield ()

    program.timed.attempt.map {
      case Right((duration, _)) => Some("OK".getBytes())
      case Left(err)            => Some("FAIL".getBytes())
    }
  }
}
