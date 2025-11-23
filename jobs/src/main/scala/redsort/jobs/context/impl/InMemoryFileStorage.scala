package redsort.jobs.context.impl

import cats._
import cats.effect._
import cats.syntax._
import fs2.{Stream, Pipe, Chunk}
import redsort.jobs.context.interface.FileStorage

import java.io.FileNotFoundException
import redsort.jobs.Common.FileEntry
import fs2.io.file.FileAlreadyExistsException

class InMemoryFileStorage(ref: Ref[IO, Map[String, Array[Byte]]]) extends FileStorage {
  override def read(path: String): Stream[IO, Byte] = {
    Stream
      .eval(ref.get.map(_.get(path)))
      .flatMap {
        case Some(bytes) => Stream.chunk(Chunk.array(bytes))
        case None => Stream.raiseError[IO](new FileNotFoundException(s"File not found: $path"))
      }
  }
  // Pipe[F, I, O] = Stream[F, I] => Stream[F, O]
  override def write(path: String): Pipe[IO, Byte, Unit] = { in =>
    {
      Stream
        .eval(in.compile.to(Array))
        .evalMap { bytes =>
          ref.update(fs => fs + (path -> bytes))
        }
    }
  }

  override def delete(path: String): IO[Unit] = {
    ref.update(fs => fs - path)
  }

  override def exists(path: String): IO[Boolean] = {
    ref.get.map(_.contains(path))
  }

  override def rename(before: String, after: String): IO[Unit] = {
    val action = ref.modify { fs =>
      fs.get(before) match {
        case Some(data) =>
          (fs - before + (after -> data), Right(()))
        case None =>
          (fs, Left(new FileNotFoundException(s"File not found: $before")))
      }
    }

    action.flatMap {
      case Right(_) => IO.unit
      case Left(e)  => IO.raiseError(e)
    }
  }

  override def list(path: String): IO[Map[String, FileEntry]] = {
    Stream
      .eval(ref.get)
      .flatMap { fs =>
        val files = fs.keys.filter(_.startsWith(path)).toSeq
        Stream.emits(files)
      }
      .evalMap { pathStr =>
        fileSize(pathStr).map { size =>
          (pathStr -> new FileEntry(path = pathStr, size = size, replicas = Seq()))
        }
      }
      .compile
      .to(Map)
  }

  override def fileSize(path: String): IO[Long] = {
    ref.get.flatMap { fs =>
      fs.get(path) match {
        case Some(bytes) => IO.pure(bytes.length.toLong)
        case None        => IO.raiseError(new FileNotFoundException(s"File not found: $path"))
      }
    }
  }

  override def mkDir(path: String): IO[String] =
    // notion of "directory" does not exists in this implementation
    ref.get.flatMap { fs =>
      for {
        // abort if file with same name exists
        _ <- IO.raiseWhen(fs.contains(path))(
          new FileAlreadyExistsException(s"can't create directory because file $path exists")
        )
      } yield path
    }
}
