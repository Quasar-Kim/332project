package redsort.master

import cats.effect._
import redsort.jobs.scheduler.Scheduler
import scala.concurrent.duration._

object Main extends IOApp {
  override def run(args: List[String]): IO[ExitCode] = for {
    _ <- Scheduler(Seq()).use { scheduler =>
      IO.println("launched scheduler")
      IO.sleep(1.hours)
    }
  } yield ExitCode.Success
}
