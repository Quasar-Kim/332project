package redsort.jobs.scheduler

import cats._
import cats.syntax.all._
import cats.effect._
import redsort.jobs.scheduler.ScheduleLogic
import redsort.jobs.Common._
import monocle.syntax.all._
import redsort.jobs.Unreachable

/** A simple scheduler. This scheduler does not actually do "schedlue" - all informations must be
  * supplied by the user.
  *
  * Scheduling procedure:
  *   1. If JobSpec (or simply "job") has one or more output files, then schedule the job to first
  *      element of `replica` field of first output file spec.
  *   2. If job does not have output file, then schedule the job to first element of `replica` field
  *      of input file.
  *   3. If job also does not have input file, schedule it to worker with least jobs.
  *
  * This scheduler is not fault tolerant.
  */
object SimpleScheduleLogic extends ScheduleLogic {
  override def schedule(
      workerStates: Map[Wid, WorkerState],
      specs: Seq[JobSpec]
  ): Map[Wid, WorkerState] = {
    specs.foldLeft(workerStates)(scheduleSingle _)
  }

  def scheduleSingle(workerStates: Map[Wid, WorkerState], spec: JobSpec): Map[Wid, WorkerState] = {
    val mid = determineMid(spec)
    val wid = workerWithLeastJob(workerStates, mid)
    val job = new Job(state = JobState.Pending, ttl = 0, spec = spec, result = None)
    workerStates.updatedWith(wid) {
      case Some(state) =>
        Some(state.focus(_.pendingJobs).modify(_.enqueue(job)))
      case None => throw new Unreachable
    }
  }

  def determineMid(spec: JobSpec): Option[Mid] =
    if (spec.outputs.length > 0) Some(spec.outputs(0).replicas(0))
    else if (spec.inputs.length > 0) Some(spec.inputs(0).replicas(0))
    else None

  def workerWithLeastJob(workerState: Map[Wid, WorkerState], mid: Option[Mid]): Wid = {
    val filtered = mid match {
      case Some(m) => workerState.filter(_._1.mid == m)
      case None    => workerState
    }
    filtered.view.mapValues(_.pendingJobs.length).minBy(_._2)._1
  }

  override def evaluate(spec: JobSpec, wid: Wid, workerState: WorkerState): ScheduleEvaluation =
    ???
}
