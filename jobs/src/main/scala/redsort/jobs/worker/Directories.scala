package redsort.jobs.worker

import cats.effect._
import cats.syntax.all._
import fs2.io.file.Path
import redsort.jobs.context.interface.FileStorage
import java.io.FileNotFoundException
import java.io.File
import fs2.io.file.FileAlreadyExistsException

final case class Directories(
    inputDirectories: Seq[Path],
    outputDirectory: Path,
    workingDirectory: Path
)

object Directories {
  def init(
      inputDirectories: Seq[Path],
      outputDirectory: Path,
      workingDirectory: Path
  ): IO[Directories] =
    for {
      inputDirs <- inputDirectories.parTraverse(toCanonicalPath)
      outputDir <- toCanonicalPath(outputDirectory)
      workingDir <- toCanonicalPath(workingDirectory)
    } yield new Directories(
      inputDirectories = inputDirs,
      outputDirectory = outputDir,
      workingDirectory = workingDir
    )

  def toCanonicalPath(p: Path): IO[Path] =
    IO(Path(new File(p.toString).getCanonicalPath()))

  def ensureDirs(d: Directories, ctx: FileStorage): IO[Unit] =
    for {
      _ <- ensureDir(d.outputDirectory, ctx, create = true)
      _ <- ensureDir(d.workingDirectory, ctx, create = true)
      _ <- d.inputDirectories.traverse(ensureDir(_, ctx, create = false))
    } yield ()

  private def ensureDir(p: Path, ctx: FileStorage, create: Boolean): IO[Unit] =
    for {
      exists <- ctx.exists(p.toString)
      _ <- IO.whenA(!exists && !create)(
        IO.raiseError(new FileNotFoundException(s"directory $p does not exists"))
      )
      _ <- IO.unlessA(exists)(
        ctx
          .mkDir(p.absolute.toString)
          .handleErrorWith { case _: FileAlreadyExistsException =>
            IO.unit
          }
          .void
      )
    } yield ()

  def resolvePath(d: Directories, p: Path): Path = {
    val path = p.toNioPath
    if (p.startsWith("@{input}")) {
      // convert @{input} to absolute path
      Path("/") / Path.fromNioPath(path.subpath(1, path.getNameCount()))
    } else if (p.startsWith("@{working}")) {
      d.workingDirectory / Path.fromNioPath(path.subpath(1, path.getNameCount()))
    } else if (p.startsWith("@{output}")) {
      d.outputDirectory / Path.fromNioPath(path.subpath(1, path.getNameCount()))
    } else p
  }

}
