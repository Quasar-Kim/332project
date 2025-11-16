package redsort.jobs.scheduler

import cats._
import cats.effect._
import cats.syntax._
import scala.concurrent.duration._

object SchedulerFiber {
  def start: IO[Unit] = for {
    _ <- IO.println("scheduler fiber running")
    _ <- IO.sleep(1.second)
    _ <- start
  } yield ()
}
