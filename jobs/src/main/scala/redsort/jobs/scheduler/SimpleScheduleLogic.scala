package redsort.jobs.scheduler

import cats._
import cats.syntax._
import cats.effect._
import redsort.jobs.scheduler.ScheduleLogic
import redsort.jobs.Common._

// TODO: currently just a placeholder to run tests, needs to implement
object SimpleScheduleLogic extends ScheduleLogic {
  override def schedule(
      workerStates: Map[Wid, WorkerState],
      specs: Seq[JobSpec]
  ): IO[Map[Wid, WorkerState]] = IO.raiseError(new NotImplementedError)

  override def evaluate(spec: JobSpec, wid: Wid, workerState: WorkerState): ScheduleEvaluation =
    ???
}
