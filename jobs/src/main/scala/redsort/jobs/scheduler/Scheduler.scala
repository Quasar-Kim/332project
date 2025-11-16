package redsort.jobs.scheduler

import cats._
import cats.effect._
import cats.syntax._
import redsort.jobs.Common._

trait Scheduler {
  def start: IO[Unit]
}

object Scheduler {
  def apply(workers: Seq[NetAddr]): IO[Scheduler] =
    IO.pure(new Scheduler {
      override def start: IO[Unit] = for {
        _ <- (RpcServerFiber.start, SchedulerFiber.start, WorkerRpcClientFiber.start(Wid(0, 0)))
          .parMapN((_, _, _) => IO.unit)
      } yield ()
    })
}
