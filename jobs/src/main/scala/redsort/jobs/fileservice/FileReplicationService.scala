package redsort.jobs.fileservice

import cats.effect.{Async, IO, Resource}
import fs2.Stream
import redsort.jobs.Common.{FileEntry, Mid}
import redsort.jobs.context.ReplicatorCtx
import redsort.jobs.context.interface.FileStorage
import redsort.jobs.messages.ReplicationErrorKind.{CONNECTION_ERROR, FILE_NOT_FOUND_ERROR}
import redsort.jobs.messages.{ReplicationResult, ReplicationStats}

class FileReplicationService(
    network: NetworkAlgebra[IO],
    myMid: Mid,
    storage: FileStorage // TODO : change to ref/resource ??? or this is ok?
) {
  // FIXME -- clean+probably rework
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
    for {
      start <- IO(System.currentTimeMillis())

      // try to receive the data from src
      readAttempt <- network.getClient(src).use { srcClient =>
          // read the file
          network.readFile(srcClient, entry.path, src).attempt
        }

      // check if transfer worked
      res <- readAttempt match {
        case Left(_) => // didn't work --> connection error
          // TODO also log the specific error?
          IO(System.currentTimeMillis())
            .map { end =>
              ReplicationResult(
                success = false,
                error = Some(CONNECTION_ERROR),
                stats = Some(
                  ReplicationStats(
                    entry.path,
                    src,
                    dst = myMid,
                    start,
                    end,
                    bytesTransferred = 0L
                  )
                )
              )
            }
        case Right(dataStream) => {
          for {
            // read the bytes
            bytes <- dataStream.compile.to(Array)

            // write to storage
            _ <- storage.writeAll(entry.path, bytes)

            end <- IO(System.currentTimeMillis())
          } yield ReplicationResult(
            success = true,
            error = None,
            stats = Some(
              ReplicationStats(
                entry.path,
                src,
                dst = myMid,
                start,
                end,
                bytesTransferred = bytes.length.toLong
              )
            )
          )
        }
      }
    } yield res
  }
}
