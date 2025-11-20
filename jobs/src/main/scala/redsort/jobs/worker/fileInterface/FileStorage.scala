package redsort.jobs.worker.filestorage

import cats._
import cats.effect._
import cats.syntax._
import fs2.{Stream, Pipe, Chunk}

import java.io.FileNotFoundException

sealed abstract class AppContext
object AppContext {
  case object Production extends AppContext
  case object Testing extends AppContext

  implicit val testingContext: AppContext = Testing
}

trait FileStorage[C] {
  def read(path: String): Stream[IO, Byte]
  def write(path: String): Pipe[IO, Byte, Unit]
  def delete(path: String): IO[Unit]
  def exists(path: String): IO[Boolean]
  def rename(before: String, after: String): IO[Unit]
  def list(path: String): Stream[IO, String]
  def fileSize(path: String): IO[Long]

  // Use this method only for small files.
  def readAll(path: String): IO[Array[Byte]]
  def writeAll(path: String, data: Array[Byte]): IO[Unit]
}

object FileStorage {
  def apply[C](implicit ev: FileStorage[C]): FileStorage[C] = ev
  def create(ctx: AppContext): IO[FileStorage[AppContext]] = ctx match {
    // TODO: without a fault-tolerance, LocalFileStorage is enough for production.
    // However, in the future, we may want to use network-based file storage to access files from another worker machine.
    case AppContext.Production => IO.pure(new LocalFileStorage())
    case AppContext.Testing    =>
      Ref.of[IO, Map[String, Array[Byte]]](Map.empty).map(new InMemoryFileStorage(_))
  }
}
