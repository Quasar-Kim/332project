package redsort.jobs.scheduler

import cats._
import cats.effect._
import cats.syntax._
import redsort.jobs.Common._
import scala.concurrent.duration._

object WorkerRpcClientFiber {
  def start(state: Ref[IO, SharedState], wid: Wid): Resource[IO, Unit] =
    main(state).background.evalMap(_ => IO.unit)

  private def main(state: Ref[IO, SharedState]): IO[Unit] = for {
    _ <- IO.println("worker fiber running")
    _ <- IO.sleep(1.second)
    _ <- main(state)
  } yield ()
}
