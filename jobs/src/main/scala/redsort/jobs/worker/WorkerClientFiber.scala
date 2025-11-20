package redsort.jobs.worker

import cats._
import cats.effect._
import cats.syntax._
import redsort.jobs.Common._
import scala.concurrent.duration._

object WorkerClientFiber {
  def start(wid: Int, masterIP: String, masterPort: Int): IO[Unit] = for {
    _ <- IO.println(s"[WorkerClientFiber] hello from rpc client $wid")
    _ <- IO.sleep(20.second)
    _ <- start(wid, masterIP, masterPort)
  } yield ()
}
