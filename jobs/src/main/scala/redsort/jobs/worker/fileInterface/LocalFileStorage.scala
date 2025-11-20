package redsort.jobs.worker.filestorage

import cats._
import cats.effect._
import cats.syntax._
import fs2.{Stream, Pipe, Chunk}
import fs2.io.file.{Files, Path}
import java.io.FileNotFoundException

class LocalFileStorage() extends FileStorage[AppContext] {

  def read(path: String): Stream[IO, Byte] = {
    val p = Path(path)
    Stream.eval(Files[IO].exists(p)).flatMap {
      case true  => Files[IO].readAll(p)
      case false => Stream.raiseError[IO](new FileNotFoundException(s"File not found: $path"))
    }
  }
  def write(path: String): Pipe[IO, Byte, Unit] = in => {
    val p = Path(path)
    val prepare = p.parent match {
      case Some(parent) => Files[IO].createDirectories(parent)
      case None         => IO.unit
    }
    Stream.eval(prepare).flatMap { _ => in.through(Files[IO].writeAll(p)) }
  }
  def delete(path: String): IO[Unit] =
    Files[IO].deleteIfExists(Path(path)).void

  def exists(path: String): IO[Boolean] =
    Files[IO].exists(Path(path))

  def rename(before: String, after: String): IO[Unit] =
    Files[IO].move(Path(before), Path(after)).void

  def list(path: String): Stream[IO, String] =
    Files[IO].list(Path(path)).map(_.toString)

  def fileSize(path: String): IO[Long] =
    Files[IO].size(Path(path))

  def readAll(path: String): IO[Array[Byte]] =
    Files[IO].readAll(Path(path)).compile.to(Array)

  def writeAll(path: String, data: Array[Byte]): IO[Unit] = {
    val p = Path(path)
    val prepare = p.parent match {
      case Some(parent) => Files[IO].createDirectories(parent)
      case None         => IO.unit
    }
    prepare *> Stream.chunk(Chunk.array(data)).through(Files[IO].writeAll(p)).compile.drain
  }

}
