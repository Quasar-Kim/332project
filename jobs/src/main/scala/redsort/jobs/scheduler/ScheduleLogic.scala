package redsort.jobs.scheduler

import redsort.jobs.Common._
import cats.effect._

/** Pluggable job scheduling logic of scheduler.
  */
trait ScheduleLogic {

  /** Given `specs`, create and move `Job` objects into desired worker's `pendingJobs` queue.
    *
    * @param workerStates
    *   map of worker states.
    * @param specs
    *   list of `JobSpec`s to schedule.
    * @return
    *   a new worker state.
    */
  def schedule(workerStates: Map[Wid, WorkerState], specs: Seq[JobSpec]): Map[Wid, WorkerState]

  /** Given one of worker, check whether job is runnable on that worker.
    *
    * @param spec
    *   a job spec.
    * @param wid
    *   wid of a worker.
    * @param workerState
    *   scheduling state of a worker.
    * @return
    *   an evaluation result.
    */
  def evaluate(spec: JobSpec, wid: Wid, workerState: WorkerState): ScheduleEvaluation
}

/** Indicate whether job can be run on a given worker.
  */
sealed abstract class ScheduleEvaluation
object ScheduleEvaluation {

  /** Job is runnable on a worker.
    *
    * @param time
    *   estimated time required for job execution.
    */
  final case class Runnable(time: Int) extends ScheduleEvaluation

  /** Job is not runnable on a worker.
    */
  final case object NotRunnable extends ScheduleEvaluation
}
