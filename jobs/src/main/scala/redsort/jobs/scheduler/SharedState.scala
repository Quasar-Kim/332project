package redsort.jobs.scheduler

import scala.collection.immutable.Queue
import cats.effect._
import cats.syntax.all._

import redsort.jobs.Common._
import redsort.jobs.scheduler
import redsort.jobs.{messages => msg}
import com.google.protobuf.any.{Any => ProtobufAny}
import scalapb.GeneratedMessage

/** Shared states among all fibers. Some fields are owned by a specific fiber, which can be read by
  * anyone but only written by owner. Remaining fields are input queue of fibers.
  *
  * @param schedulerFiber
  *   states owned by scheduler fiber.
  * @param rpcClientFibers
  *   map of states, each owned by respective RPC client fibers.
  * @param rpcServerFiber
  *   states owned by RPC server fiber.
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
  * @param state
  *   state of scheduler automata.
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

/** Enum indicating state of scheduler automata.
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
  * @param netAddr
  *   network address of worker.
  * @param status
  *   availability of worker.
  * @param pendingJobs
  *   queue of jobs in pending state.
  * @param runningJobs
  *   a job in running state.
  * @param completedJobs
  *   queue of jobs in completed state.
  * @param initialized
  *   indicate whether worker called RegisterWorker() RPC method.
  */
final case class WorkerState(
    wid: Wid,
    netAddr: NetAddr,
    status: WorkerStatus,
    pendingJobs: Queue[Job],
    runningJob: Option[Job],
    completedJobs: Queue[Job],
    initialized: Boolean,
    completed: Boolean
)

object WorkerState {
  def init(wid: Wid, netAddr: NetAddr) = new WorkerState(
    wid = wid,
    netAddr = netAddr,
    status = WorkerStatus.Down,
    pendingJobs = Queue(),
    runningJob = None,
    completedJobs = Queue(),
    initialized = false,
    completed = false
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
  * @param result
  *   execution result, only available for completed jobs.
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
  */
final case class JobSpec(
    name: String,
    args: Seq[GeneratedMessage],
    inputs: Seq[FileEntry],
    outputs: Seq[FileEntry]
)
object JobSpec {
  def toMsg(spec: JobSpec): msg.JobSpecMsg =
    new msg.JobSpecMsg(
      name = spec.name,
      args = spec.args.map(ProtobufAny.pack(_)),
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

/** Events that scheduler fiber receives.
  */
sealed abstract class SchedulerFiberEvents
object SchedulerFiberEvents {
  // == sent by main fiber:

  /** Distributed program requested execution of jobs.
    *
    * @param specs
    *   list of job specs.
    */
  final case class Jobs(specs: Seq[JobSpec]) extends SchedulerFiberEvents

  /** Shut down workers.
    */
  final case class Complete() extends SchedulerFiberEvents

  // == sent by RPC server fiber:

  /** WorkerHello RPC method has been called with argument `hello`.
    *
    * @param hello
    *   hello message sent by worker.
    * @param from
    *   sender of worker hello.
    */
  final case class WorkerRegistration(hello: msg.WorkerHello, from: Wid)
      extends SchedulerFiberEvents

  /** Worker did not called Heartbeat RPC method in timeout.
    *
    * @param from
    *   worker that timed out.
    */
  final case class HeartbeatTimeout(from: Wid) extends SchedulerFiberEvents

  /** Worker requested system halt.
    *
    * @param err
    *   error detail.
    * @param from
    *   worker that requested system halt.
    */
  final case class Halt(err: msg.JobSystemError, from: Wid) extends SchedulerFiberEvents

  // == sent by RPC client fiber:

  /** A job was completed without fatal error.
    *
    * @param result
    *   job execution result.
    * @param from
    *   worker that has run the job.
    */
  final case class JobCompleted(result: msg.JobResult, from: Wid) extends SchedulerFiberEvents

  /** A job failed to run, due to wrong job specification or machine fault.
    *
    * @param result
    *   job execution result.
    * @param from
    *   worker that has run the job.
    */
  final case class JobFailed(result: msg.JobResult, from: Wid) extends SchedulerFiberEvents

  /** Cannot send RPC request to a worker.
    *
    * @param from
    *   ID of worker that is not responding.
    */
  final case class WorkerNotResponding(from: Wid) extends SchedulerFiberEvents

  /** Successfully called Complete() RPC method of worker.
    */
  final case class WorkerCompleted(from: Wid) extends SchedulerFiberEvents

  // == can be sent by anyone:

  /** Some component of scheduler system failed.
    *
    * @param error
    *   an error.
    */
  final case class FatalError(error: Throwable) extends SchedulerFiberEvents
}

/** Events that worker RPC client fibers receive.
  */
sealed abstract class WorkerFiberEvents
object WorkerFiberEvents {
  final case class Job(spec: JobSpec) extends WorkerFiberEvents
  final case object WorkerDown extends WorkerFiberEvents
  final case object WorkerUp extends WorkerFiberEvents
  final case object Complete extends WorkerFiberEvents
}

/** Events that main fiber (where `Scheduler` object is) receives.
  */
sealed abstract class MainFiberEvents
object MainFiberEvents {
  final case class Initialized(files: Map[Mid, Map[String, FileEntry]]) extends MainFiberEvents
  final case class JobCompleted(
      results: Seq[Tuple2[JobSpec, msg.JobResult]],
      files: Map[Mid, Map[String, FileEntry]]
  ) extends MainFiberEvents
  final case class JobFailed(spec: JobSpec, result: msg.JobResult) extends MainFiberEvents
  final case class SystemException(error: Throwable) extends MainFiberEvents
  final case object CompleteDone extends MainFiberEvents
}
