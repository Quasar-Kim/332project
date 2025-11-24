package redsort.jobs.fileservice

import cats.effect.{Async, Concurrent, IO}
import fs2.Stream
import redsort.jobs.Common.{FileEntry, Mid}
import redsort.jobs.context.ReplicatorCtx
import redsort.jobs.messages.ReplicationErrorKind.FILE_NOT_FOUND_ERROR
import redsort.jobs.messages.{ReplicationErrorKind, ReplicationResult, ReplicationStats}

class FileReplicationService[F[_]: Async](
    network: NetworkAlgebra[F],
    myMid: Mid
) {
  def push(entry: FileEntry, dst: Mid): F[ReplicationResult] = {
//    for {
//      start <- Async[F].delay(System.currentTimeMillis())
//      res <- network.getClient(dst).use { ctx =>
//        for {
//          fileExists <- ctx.exists(entry.path)
//          transferRes <- if (!fileExists) {
//            Async[F].delay(ReplicationResult(
//              success = false,
//              error = Some(FILE_NOT_FOUND_ERROR),
//              stats = Some(ReplicationStats(
//                path = entry.path,
//                src = myMid,
//                dst = dst,
//                start = start,
//                end = start,
//                bytesTransferred = 0L
//              )
//            )))
//          } else {
//            for {
//              size <- ctx.fileSize(entry.path)
//              bytesTransferred <- executePush(ctx, entry, dst, size)
//              end <- Async[F].delay(System.currentTimeMillis())
//            } yield ReplicationResult(
//              success = true,
//              error = None,
//              stats = Some(ReplicationStats(
//                path = entry.path,
//                src = myMid,
//                dst = dst,
//                start = start,
//                end = end,
//                bytesTransferred = bytesTransferred
//              ))
//            )
//          }
//        } yield transferRes
//      }
//    } yield res
//
//    // TODO additional error handling???
    ???
  }
  private def executePush(ctx: ReplicatorCtx, entry: FileEntry, mid: Mid, size: Long): F[Long] = {
//    val fileStream: Stream[IO, Byte] = ctx.read(entry.path)
//
//    // TODO
//    ???
    ???
  }

  def pull(entry: FileEntry, src: Mid): F[ReplicationResult] = {
//    for {
//      start <- Async[F].delay(System.currentTimeMillis())
//      res <- network.getClient(src).use { ctx =>
//        for {
//          bytesTransferred <- executePull(ctx, entry, src)
//          end <- Async[F].delay(System.currentTimeMillis())
//
//          wroteFile <- ctx.exists(entry.path)
//          tranferRes <- if (wroteFile) {
//            Async[F].pure(ReplicationResult(
//              success = true,
//              error = None,
//              stats = Some(ReplicationStats(
//                path = entry.path,
//                src = src,
//                dst = myMid,
//                start = start,
//                end = end,
//                bytesTransferred = bytesTransferred
//              ))
//            ))
//          } else Async[F].delay(ReplicationResult(
//            success = false,
//            error = Some(FILE_NOT_FOUND_ERROR),
//            stats = Some(ReplicationStats(
//              path = entry.path,
//              src = src,
//              dst = myMid,
//              start = start,
//              end = end,
//              bytesTransferred = bytesTransferred
//            ))))
//        } yield tranferRes
//      }
//    } yield res
//
//    // TODO additional error handling???
    ???
  }
  private def executePull(ctx: ReplicatorCtx, entry: FileEntry, src: Mid): F[Long] = {
//    for {
//      fileStream <- network.readFile(ctx, entry.path, src) // FIXME have F[Stream[F, Byte]], instead need IO[Stream[F, Byte]] ?????
//      bytesWritten <- ???
//    } yield bytesWritten
    ???
  }
}
