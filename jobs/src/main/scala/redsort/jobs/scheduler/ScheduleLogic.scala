package redsort.jobs.scheduler

import redsort.jobs.Common._
import cats.effect._

trait ScheduleLogic {
  def schedule(workerStates: Map[Wid, WorkerState], specs: Seq[JobSpec]): IO[Map[Wid, WorkerState]]

  def evaluate(spec: JobSpec, wid: Wid, workerState: WorkerState): ScheduleEvaluation
}

sealed abstract class ScheduleEvaluation
object ScheduleEvaluation {
  final case class Runnable(time: Int) extends ScheduleEvaluation
  final case object NotRunnable extends ScheduleEvaluation
}
