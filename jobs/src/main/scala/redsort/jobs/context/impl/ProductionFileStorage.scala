package redsort.jobs.context.impl

import cats._
import cats.effect._
import cats.syntax._
import fs2.{Stream, Pipe, Chunk}
import fs2.io.file.{Files, Path}
import java.io.FileNotFoundException
import redsort.jobs.context.interface.FileStorage
import redsort.jobs.Common.FileEntry

/** Actual implementation of `FileStorage`.
  */
trait ProductionFileStorage extends FileStorage {
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

  def rename(before: String, after: String): IO[Unit] =
    Files[IO].move(Path(before), Path(after)).void

  def delete(path: String): IO[Unit] =
    Files[IO].deleteIfExists(Path(path)).void

  def exists(path: String): IO[Boolean] =
    Files[IO].exists(Path(path))

  def list(path: String): IO[Map[String, FileEntry]] =
    Files[IO]
      .list(Path(path))
      .parEvalMap(maxConcurrent = 16) { p =>
        val pathStr = p.absolute.toString

        fileSize(pathStr).map { size =>
          (pathStr -> new FileEntry(path = pathStr, size = size, replicas = Seq()))
        }
      }
      .compile
      .to(Map)

  def fileSize(path: String): IO[Long] =
    Files[IO].size(Path(path))

  def mkDir(path: String): IO[String] =
    Files[IO].createDirectory(Path(path)) >> IO.pure(path)
}
