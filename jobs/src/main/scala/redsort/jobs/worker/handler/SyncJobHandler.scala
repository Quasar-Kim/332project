package scala.redsort.jobs.worker.handler

import redsort.jobs.worker.JobHandler
import cats.effect._
import cats.syntax.all._
import fs2.io.file.Path
import redsort.jobs.context.interface.FileStorage
import com.google.protobuf.any.{Any => ProtobufAny}
import redsort.jobs.worker.Directories
import redsort.jobs.messages.FileEntryMsg
import redsort.jobs.Common.FileEntry
import scala.concurrent.duration._
import redsort.jobs.Common.assertIO

/** Synchornize local working directory entries with entries scheduler knows. This effectively
  * cleans up no longer required intermediate files. Also fetches missing files from remote
  * machines. (NOT IMPLEMENTED)
  */
object SyncJobHandler extends JobHandler {
  override def apply(
      args: Seq[ProtobufAny],
      inputs: Seq[Path],
      outputs: Seq[Path],
      ctx: FileStorage,
      dirs: Directories
  ): IO[Option[Array[Byte]]] =
    for {
      _ <- assertIO(outputs.length == 1, "one output file needs to be specified")

      localEntries <- ctx.list(dirs.workingDirectory.toString)
      localFiles <- IO.pure(localEntries.keySet.map(Path(_)))
      remoteFiles <- IO.pure {
        args
          .map(path => Directories.resolvePath(dirs, Path(path.unpack[FileEntryMsg].path)))
          .filter(_.startsWith(dirs.workingDirectory.toString))
          .toSet
      }
      filesToDelete <- IO.pure(localFiles &~ remoteFiles)
      _ <- filesToDelete.toList.traverse(p => ctx.delete(p.toString))

      // output file is required to force sync job to be scheduled to desired machine
      // create output file with dummy contents, since not creating it can confuse scheduler
      _ <- ctx.save(outputs(0).toString, fs2.Stream.emit(42.toByte))
    } yield None
}
