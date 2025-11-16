package redsort.jobs.scheduler

import cats._
import cats.effect._
import cats.syntax._
import redsort.jobs.Common._
import scala.concurrent.duration._

object WorkerRpcClientFiber {
  def start(wid: Wid): IO[Unit] = for {
    _ <- IO.println(s"hello from rpc client $wid")
    _ <- IO.sleep(1.second)
    _ <- start(wid)
  } yield ()
}
