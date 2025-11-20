package redsort.worker.handlers

import cats._
import cats.effect._
import cats.syntax._
import redsort.jobs.Common._
import redsort.jobs.messages._

import scala.concurrent.duration._
import redsort.jobs.worker.filestorage.{FileStorage, AppContext}

class JobMerger(fileStorage: FileStorage[AppContext]) {
  def run(job: JobSpecMsg): IO[JobResult] = {
    for {
      _ <- IO.println(s"[Merging] Merging job ${job.name} is started")
      // TODO
      _ <- IO.sleep(10.second)
      _ <- IO.println(s"[Merging] Merging job ${job.name} is done")
    } yield JobResult(
      success = true
      // TODO
    )
  }
}
