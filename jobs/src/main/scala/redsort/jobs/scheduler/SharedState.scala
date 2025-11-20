package redsort.jobs.scheduler

import scala.collection.immutable.Queue
import cats.effect._
import cats.syntax.all._

import redsort.jobs.Common._
import redsort.jobs.scheduler
import redsort.jobs.{messages => msg}
import com.google.protobuf.any

/** Shared states among all fibers. Some fields are owned by a specific fiber, which can be read by
  * anyone but only written by owner. Remaining fields are input queue of fibers.
  *
  * @param schedulerFiber
  *   states owned by scheduler fiber.
  * @param rpcClientFibers
  *   map of states, each owned by respective RPC client fibers.
  * @param rpcServerFiber
  *   states owned by RPC server fiber.
  * @param schedulerFiberQueue
  *   event queue of scheduler fiber.
  * @param rpcClientFiberQueues:
  *   map of RPC client fibers' event queue.
  */
final case class SharedState(
    schedulerFiber: SchedulerFiberState,
    rpcClientFibers: Map[Wid, RpcClientFiberState],
    rpcServerFiber: RpcServerFiberState
)

object SharedState {
  def init(workers: Map[Wid, NetAddr]) = {
    val initialSchedulerFiber = SchedulerFiberState.init(workers)
    val initialRpcClientFibers = workers.map { case (wid, _) => (wid, RpcClientFiberState.init) }
    val initialRpcServerFiber = RpcServerFiberState.init

    val initialState = SharedState(
      schedulerFiber = initialSchedulerFiber,
      rpcClientFibers = initialRpcClientFibers,
      rpcServerFiber = initialRpcServerFiber
    )

    Ref.of[IO, SharedState](initialState)
  }
}

/** States owned by scheduler fiber.
  *
  * @param workers
  *   states of each workers.
  * @param files
  *   files available on each machines.
  */
final case class SchedulerFiberState(
    state: SchedulerState,
    workers: Map[Wid, WorkerState],
    files: Map[Mid, Map[String, FileEntry]]
)

object SchedulerFiberState {
  def init(workers: Map[Wid, NetAddr]) = new SchedulerFiberState(
    state = SchedulerState.Initializing,
    workers = workers.map { case (wid, netAddr) => (wid, WorkerState.init(wid, netAddr)) },
    files = Map()
  )
}

/** Enum indicating state of scheduler.
  */
sealed abstract class SchedulerState
object SchedulerState {
  final case object Initializing extends SchedulerState
  final case object Idle extends SchedulerState
  final case object Running extends SchedulerState
}

/** State associated to each workers.
  *
  * @param wid
  *   worker ID.
  * @param files
  *   map of files available on this worker's machine.
  * @param status
  *   availability of worker.
  * @param pendingJobs
  *   queue of jobs in pending state.
  * @param runningJobs
  *   a job in running state.
  * @param completedJobs
  *   queue of jobs in completed state.
  */
final case class WorkerState(
    wid: Wid,
    netAddr: NetAddr,
    status: WorkerStatus,
    pendingJobs: Queue[Job],
    runningJob: Option[Job],
    completedJobs: Queue[Job],
    initialized: Boolean
)

object WorkerState {
  def init(wid: Wid, netAddr: NetAddr) = new WorkerState(
    wid = wid,
    netAddr = netAddr,
    status = WorkerStatus.Down,
    pendingJobs = Queue(),
    runningJob = None,
    completedJobs = Queue(),
    initialized = false
  )
}

/** Availability of worker, either `Up` or `Down`.
  */
sealed abstract class WorkerStatus
object WorkerStatus {
  final case object Up extends WorkerStatus
  final case object Down extends WorkerStatus
}

/** A job specification plus states associated with the job.
  *
  * @param state
  *   job state, either pending, running, or completed.
  * @param ttl
  *   remaining retry counts.
  * @param spec
  *   job specification.
  */
final case class Job(
    state: JobState,
    ttl: Int,
    spec: JobSpec,
    result: Option[msg.JobResult]
)

/** Job state, either pending, running, or completed.
  */
sealed abstract class JobState
object JobState {
  final case object Pending extends JobState
  final case object Running extends JobState
  final case object Completed extends JobState
}

/** A specification of a job that can be sent between worker and scheduler.
  *
  * @param name
  *   job name.
  * @param args
  *   arguments passed to body of a job.
  * @param inputs
  *   list of files required by job.
  * @param outputs
  *   list of files created by job.
  * @param outputSize
  *   estimated size of all output files.
  */
final case class JobSpec(
    name: String,
    args: Seq[Any],
    inputs: Seq[FileEntry],
    outputs: Seq[FileEntry]
)
object JobSpec {
  def toMsg(spec: JobSpec): msg.JobSpecMsg =
    new msg.JobSpecMsg(
      name = spec.name,
      // XXX: how to convert Any to protobuf Any?
      args = Seq(),
      inputs = spec.inputs.map(FileEntry.toMsg),
      outputs = spec.outputs.map(FileEntry.toMsg)
    )

  def fromMsg(m: msg.JobSpecMsg): JobSpec =
    new JobSpec(
      name = m.name,
      args = m.args,
      inputs = m.inputs.map(FileEntry.fromMsg),
      outputs = m.outputs.map(FileEntry.fromMsg)
    )
}

final case class RpcClientFiberState()

object RpcClientFiberState {
  def init = new RpcClientFiberState
}

final case class RpcServerFiberState()

object RpcServerFiberState {
  def init = new RpcServerFiberState
}

sealed abstract class SchedulerFiberEvents
object SchedulerFiberEvents {
  // == sent by `runJobs`:

  /** Distributed program requested execution of jobs.
    *
    * @param specs
    *   list of job specs.
    */
  final case class Jobs(specs: Seq[JobSpec]) extends SchedulerFiberEvents

  // == sent by RPC server fiber:

  /** WorkerHello RPC method has been called with argument `hello`.
    */
  final case class WorkerRegistration(hello: msg.WorkerHello, from: Wid)
      extends SchedulerFiberEvents

  /** Worker did not called Heartbeat RPC method in timeout.
    *
    * @param from
    */
  final case class HeartbeatTimeout(from: Wid) extends SchedulerFiberEvents

  /** Worker requested system halt.
    */
  final case class Halt(err: msg.JobSystemError, from: Wid) extends SchedulerFiberEvents

  // == sent by RPC client fiber:

  final case class JobCompleted(result: msg.JobResult, from: Wid) extends SchedulerFiberEvents
  final case class JobFailed(result: msg.JobResult, from: Wid) extends SchedulerFiberEvents
  final case class WorkerNotResponding(from: Wid) extends SchedulerFiberEvents

  // == can be sent by anyone:

  final case class FatalError(error: Throwable) extends SchedulerFiberEvents
}

sealed abstract class WorkerFiberEvents
object WorkerFiberEvents {
  final case class Job(spec: JobSpec) extends WorkerFiberEvents
  final case object WorkerDown extends WorkerFiberEvents
  final case object WorkerUp extends WorkerFiberEvents
}

sealed abstract class MainFiberEvents
object MainFiberEvents {
  final case object Initialized extends MainFiberEvents
  final case class JobCompleted(results: Seq[Tuple2[JobSpec, msg.JobResult]])
      extends MainFiberEvents
  final case class JobFailed(error: Throwable) extends MainFiberEvents
}
