package redsort.jobs.fileservice

import cats.effect.IO
import fs2.Stream
import redsort.jobs.Common.{FileEntry, Mid}
import redsort.jobs.messages.ReplicationErrorKind.{CONNECTION_ERROR, FILE_NOT_FOUND_ERROR}
import redsort.jobs.messages.{ReplicationResult, ReplicationStats}

class FileReplicationService(
    network: NetworkAlgebra[IO],
    myMid: Mid
) {
  def push(entry: FileEntry, dst: Mid): IO[ReplicationResult] = {
    // TODO : clean up code
    for {
      start <- IO(System.currentTimeMillis())
      resFinal <- network.getClient(myMid).use { myCtx =>
        for {
          fileExists <- myCtx.exists(entry.path)
          resTransfer <-
            if (!fileExists) {
              IO.pure(
                ReplicationResult(
                  success = false,
                  error = Some(FILE_NOT_FOUND_ERROR),
                  stats = Some(
                    ReplicationStats(
                      entry.path,
                      src = myMid,
                      dst,
                      start,
                      end = start,
                      bytesTransferred = 0L
                    )
                  )
                )
              )
            } else {
              val data: Stream[IO, Byte] = myCtx.read(entry.path)

              network.getClient(dst).use { dstCtx =>
                val transfer: IO[Either[Throwable, Unit]] =
                  network
                    .writeFile(dstCtx, entry.path, dst, data)
                    .attempt
                transfer.flatMap {
                  case Left(_) =>
                    // TODO also log the specific error?
                    IO(System.currentTimeMillis())
                      .map { end =>
                        ReplicationResult(
                          success = false,
                          error = Some(CONNECTION_ERROR),
                          stats = Some(
                            ReplicationStats(
                              entry.path,
                              src = myMid,
                              dst,
                              start,
                              end,
                              bytesTransferred = 0L
                            )
                          )
                        )
                      }
                  case Right(_) =>
                    IO(System.currentTimeMillis())
                      .map { end =>
                        ReplicationResult(
                          success = true,
                          error = None,
                          stats = Some(
                            ReplicationStats(
                              entry.path,
                              src = myMid,
                              dst,
                              start,
                              end,
                              bytesTransferred = 0L
                            )
                          )
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

  def pull(entry: FileEntry, src: Mid): IO[ReplicationResult] = {
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
}
