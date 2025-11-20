package redsort.jobs.worker

import cats._
import cats.effect._
import cats.syntax._
import redsort.jobs.Common._
import redsort.jobs.messages._

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
      port: Int = 5000
  ): IO[Worker] =
    IO.pure(new Worker {
      override def start: IO[Unit] = for {
        _ <- IO.println(s"[Worker] Worker started, connecting at $masterIP:$masterPort")
        _ <- (
          WorkerServerFiber.start(port, handlerMap),
          WorkerClientFiber.start(wtid, masterIP, masterPort)
        ).parMapN((_, _) => ())
      } yield ()
    })
}
