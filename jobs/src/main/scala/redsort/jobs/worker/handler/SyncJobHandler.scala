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
      localEntries <- ctx.list(dirs.workingDirectory.toString)
      filesToDelete <- IO.pure {
        val remoteFiles =
          args
            .map(_.unpack[FileEntryMsg].path)
            .filter(_.startsWith(dirs.workingDirectory.toString))
            .toSet
        val localFiles = localEntries.keySet
        localFiles &~ remoteFiles
      }
      _ <- filesToDelete.toList.traverse(ctx.delete(_))
    } yield None
}
