package redsort.jobs.fileservice

import cats.effect.Async
import cats.implicits.{toFlatMapOps, toFunctorOps}
import redsort.jobs.Common.{FileEntry, Mid}
import redsort.jobs.context.ReplicatorCtx
import redsort.jobs.messages.ReplicationErrorKind.{CONNECTION_ERROR, FILE_NOT_FOUND_ERROR}
import redsort.jobs.messages.{ReplicationResult, ReplicationStats}

class FileReplicationService[F[_]: Async](
    network: NetworkAlgebra[F],
    myMid: Mid
) {
  def push(entry: FileEntry, dst: Mid): F[ReplicationResult] = {
    for {
      start <- Async[F].delay(System.currentTimeMillis())
      resFinal <- network.getClient(myMid).use { myCtx =>
        for {
          fileExists <- myCtx.exists(entry.path) // FIXME type mismatch (IO/F)
          resTransfer <-
            if (!fileExists) {
              Async[F].delay(
                ReplicationResult(
                  success = false,
                  error = Some(FILE_NOT_FOUND_ERROR),
                  stats = Some(ReplicationStats(entry.path, src = myMid, dst, start, end = start, bytesTransferred = 0L))
                )
              )
            } else {
              val data = myCtx.read(entry.path)

              network.getClient(dst).use { dstCtx =>
                val transfer: F[Either[Throwable, Unit]] =
                  network
                    .writeFile(dstCtx, entry.path, dst, data) // FIXME type mismatch in data (IO/F)
                    .attempt // FIXME data is Stream[IO, Byte], but required Stream[F, Byte]
                transfer.flatMap {
                  case Left(_) =>
                    // TODO also log the specific error?
                    Async[F]
                      .delay(System.currentTimeMillis())
                      .map { end =>
                        ReplicationResult(
                          success = false,
                          error = Some(CONNECTION_ERROR),
                          stats = Some(ReplicationStats(entry.path, src = myMid, dst, start, end, bytesTransferred = 0L))
                        )
                      }
                  case Right(_) =>
                    Async[F]
                      .delay(System.currentTimeMillis())
                      .map { end =>
                        ReplicationResult(
                          success = true,
                          error = None,
                          stats = Some(ReplicationStats(entry.path, src = myMid, dst, start, end, bytesTransferred = 0L))
                          // TODO : request entry.size from dst???
                        )
                      }
                }
              }
            }
        } yield resTransfer
      }
    } yield resFinal

    // TODO additional error handling - incl. comparing bytes transferred (or is it checked by the scheduler?)
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
