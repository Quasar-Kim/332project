package redsort.jobs.scheduler

import redsort.jobs.Common.Wid
import redsort.jobs.Common.FileEntry
import scala.collection.immutable.Queue

/**
  * Shared states among all fibers.
  * Some fields are owned by a specific fiber, which can be read by anyone
  * but only written by owner. Remaining fields are input queue of fibers.
  *
  * @param schedulerFiber states owned by scheduler fiber.
  * @param rpcClientFibers map of states, each owned by respective RPC client fibers.
  * @param rpcServerFiber states owned by RPC server fiber.
  * @param schedulerFiberQueue event queue of scheduler fiber.
  * @param rpcClientFiberQueues: map of RPC client fibers' event queue.
  */
final case class SharedState(
  schedulerFiber: SchedulerFiberState,
  rpcClientFibers: Map[Wid, RpcCleintFiberState],
  rpcServerFiber: RpcServerFiberState,
  schedulerFiberQueue: Queue[SchedulerFiberEvents],
  rpcClientFiberQueues: Map[Wid, WorkerFiberEvents],
)

/**
  * States owned by scheduler fiber.
  *
  * @param workers states of each workers.
  * @param files files available on each machines.
  */
final case class SchedulerFiberState(
  workers: Map[Wid, WorkerState],
  files: Map[Mid, Map[String, FileEntry]],
)

/**
  * State associated to each workers.
  *
  * @param wid worker ID.
  * @param files map of files available on this worker's machine.
  * @param status availability of worker.
  * @param pendingJobs queue of jobs in pending state.
  * @param runningJobs a job in running state.
  * @param completedJobs queue of jobs in completed state.
  */
final case class WorkerState(
  wid: Wid,
  status: WorkerStatus,
  pendingJobs: Queue[Job],
  runningJobs: Option[Job],
  completedJobs: Queue[Job],
)

/**
  * Availability of worker, either `Up` or `Down`.
  */
sealed abstract class WorkerStatus
final case object Up extends WorkerStatus
final case object Down extends WorkerStatus

/**
  * A job specification plus states associated with the job.
  *
  * @param state job state, either pending, running, or completed.
  * @param ttl remaining retry counts.
  * @param spec job specification.
  */
final case class Job(
  state: JobState,
  ttl: Int,
  spec: JobSpec,
)

/**
  * Job state, either pending, running, or completed.
  */
sealed abstract class JobState
final case object Pending extends JobState
final case object Running extends JobState
final case object Completed extends JobState

/**
  * A specification of a job that can be sent between worker and scheduler.
  *
  * @param name job name.
  * @param args arguments passed to body of a job.
  * @param inputs list of files required by job.
  * @param outputs list of files created by job.
  * @param outputSize estimated size of all output files.
  */
final case class JobSpec(
  name: String,
  args: Seq[Any],
  inputs: Seq[FileEntry],
  outputs: Seq[FileEntry],
  outputSize: Int,
)
