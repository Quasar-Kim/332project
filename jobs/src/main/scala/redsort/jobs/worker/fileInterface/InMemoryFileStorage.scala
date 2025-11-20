package redsort.jobs.worker.filestorage

import cats._
import cats.effect._
import cats.syntax._
import fs2.{Stream, Pipe, Chunk}

import java.io.FileNotFoundException

class InMemoryFileStorage(ref: Ref[IO, Map[String, Array[Byte]]]) extends FileStorage[AppContext] {
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
    // Why this code not work?
    // ref.modify { fs =>
    //   fs.get(before) match {
    //     case None => (fs, Left(new FileNotFoundException(s"File not found: $before")))
    //     case Some(data) => (fs - before + (after -> data), Right(()))
    //   }.flatMap {
    //     case Right(_) => IO.Unit
    //     case Left(_) => IO.raiseError(_)
    //   }
    // }
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
  override def list(path: String): Stream[IO, String] = {
    Stream.eval(ref.get).flatMap { fs =>
      val files = fs.keys.filter(_.startsWith(path)).toSeq
      Stream.emits(files)
    }
  }
  override def fileSize(path: String): IO[Long] = {
    ref.get.flatMap { fs =>
      fs.get(path) match {
        case Some(bytes) => IO.pure(bytes.length.toLong)
        case None        => IO.raiseError(new FileNotFoundException(s"File not found: $path"))
      }
    }
  }
  override def readAll(path: String): IO[Array[Byte]] = {
    ref.get.flatMap { fs =>
      fs.get(path) match {
        case Some(bytes) => IO.pure(bytes)
        case None        => IO.raiseError(new FileNotFoundException(s"File not found: $path"))
      }
    }
  }
  override def writeAll(path: String, data: Array[Byte]): IO[Unit] = {
    ref.update(fs => fs + (path -> data))
  }
}
