package redsort.jobs.context.interface

import cats._
import cats.effect._
import cats.syntax.all._
import fs2.{Stream, Pipe, Chunk}
import fs2.io.file.{Files, Path}
import java.io.FileNotFoundException
import redsort.jobs.Common.FileEntry
import java.nio.file.StandardCopyOption
import fs2.io.file.CopyFlags
import fs2.io.file.CopyFlag
import cats.effect.kernel.Resource.ExitCase.Succeeded

/** Operations on file storage.
  */
trait FileStorage {

  /** Read a file as a stream.
    *
    * @param path
    *   absolute path to the file.
    */
  def read(path: String): Stream[IO, Byte]

  /** Write to file using a stream. Do not use this method directly as this is not atomic operation.
    * Instead, use `create()`.
    *
    * @param path
    *   absolute path to the file.
    */
  def write(path: String): Pipe[IO, Byte, Unit]

  /** Rename file `before` to `after`.
    *
    * @param before
    *   absolute path to the file to be renamed.
    * @param after
    *   new path of the file.
    */
  def rename(before: String, after: String): IO[Unit]

  /** Create a file. This method creates a temporary file to first write contents to, then renames
    * it to `path`. Temporary file gets deleted regardless of operation succeeds or fails.
    *
    * @param path
    *   absolute path to the file.
    * @return
    *   resource wrapping pipe to the new file.
    */
  def create(path: String): Pipe[IO, Byte, Unit] = { in =>
    val tempPath = s"$path.tmp"

    // use .attempt.void to supress error if the file was never created
    def deleteTemp = delete(tempPath).attempt.void

    in
      .through(write(tempPath))
      .onFinalizeCase {
        case Succeeded => rename(tempPath, path).onError(_ => deleteTemp)
        case _         => deleteTemp
      }
  }

  def save(path: String, data: Stream[IO, Byte]): IO[Unit]

  /** Delete file.
    *
    * @param path
    *   absolute path to the file.
    */
  def delete(path: String): IO[Unit]

  /** Check if file exists.
    *
    * @param path
    *   absolute path to the file.
    */
  def exists(path: String): IO[Boolean]

  /** list files in directory.
    *
    * @param path
    *   absolute path of directory.
    * @return
    *   map of files, indexed by its name.
    */
  def list(path: String): IO[Map[String, FileEntry]]

  /** Read a file size of a file.
    *
    * @param path
    *   absolute path to the file.
    * @return
    *   size of file in bytes.
    */
  def fileSize(path: String): IO[Long]

  // Use this method only for small files.

  /** Read entire contents of a file.
    *
    * @param path
    *   absolute path to the file.
    * @return
    *   contents of file as a array of bytes.
    */
  def readAll(path: String): IO[Array[Byte]] =
    read(path).compile.to(Chunk).map(_.toArray)

  /** Write entire contents of a file.
    *
    * @param path
    *   absolute path to the file.
    * @param data
    *   contents of file as a array of bytes.
    */
  def writeAll(path: String, data: Array[Byte]): IO[Unit] =
    Stream
      .chunk(Chunk.array(data))
      .through(create(path))
      .compile
      .drain

  /** Make a directory.
    *
    * @param path
    *   path to a directory.
    * @return
    *   path of a directory.
    */
  def mkDir(path: String): IO[String]
}
