package redsort.jobs.worker

import cats._
import cats.effect._
import cats.syntax._
import redsort.jobs.Common._
import redsort.jobs.messages._

import redsort.jobs.worker.filestorage.{FileStorage, AppContext}

trait Worker {
  def start: IO[Unit]
}

object Worker {
  def apply(
      handlerMap: Map[String, JobSpecMsg => IO[JobResult]],
      masterIP: String,
      masterPort: Int,
      inputDirectories: Seq[String],
      outputDirectory: String,
      wtid: Int,
      port: Int = 5000,
      ctx: AppContext = AppContext.Production
  ): IO[Worker] =
    IO.pure(new Worker {
      override def start: IO[Unit] = for {
        _ <- IO.println(s"[Worker] Worker started, connecting at $masterIP:$masterPort")
        fileStorage <- FileStorage.create(ctx)
        _ <- (
          WorkerServerFiber.start(port, handlerMap, fileStorage),
          WorkerClientFiber.start(
            wtid,
            masterIP,
            masterPort,
            inputDirectories,
            outputDirectory,
            fileStorage
          )
        ).parMapN((_, _) => ())
      } yield ()
    })
}
