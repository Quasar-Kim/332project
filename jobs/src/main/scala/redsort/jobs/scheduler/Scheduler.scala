package redsort.jobs.scheduler

import cats._
import cats.effect._
import cats.effect.std.{Supervisor, Queue}
import cats.syntax.all._
import redsort.jobs.Common._
import redsort.jobs.context.SchedulerCtx
import redsort.jobs.messages.JobResult
import org.log4s._
import redsort.jobs.scheduler.MainFiberEvents.Initialized
import redsort.jobs.scheduler.MainFiberEvents.JobCompleted
import redsort.jobs.scheduler.MainFiberEvents.JobFailed
import redsort.jobs.scheduler.MainFiberEvents.SystemException
import redsort.jobs.Unreachable

/** A frontend of job scheduling system.
  */
trait Scheduler {

  /** Wait for all workers to be initialized.
    * @return
    *   a map of files on each machines.
    */
  def waitInit: IO[Map[Int, Map[String, FileEntry]]]

  /** Run requested jobs in worker cluster.
    *
    * @param specs
    *   a sequence of job specifications to launch.
    *
    * @return
    *   a sequence of tuple of each job specifications and its result.
    */
  def runJobs(specs: Seq[JobSpec], sync: Boolean = true): IO[JobExecutionResult]
}

final case class JobExecutionResult(
    results: Seq[Tuple2[JobSpec, JobResult]],
    files: Map[Int, Map[String, FileEntry]]
)

/** Entry point to job scheduling system.
  */
object Scheduler {
  private[this] val logger = getLogger

  /** Start a job system. Launches background fibers consisting scheduler system.
    *
    * @param port
    *   port on which scheduler RPC service will run.
    * @param workers
    *   Sequence of sequence of network address of each workers.
    * @param ctx
    *   Context object providing dependencies.
    * @param scheduleLogic
    *   scheduling logic used by scheduler.
    * @return
    *   a resource wrapping `Scheduler` logic that allow user to interact with scheduler system.
    */
  def apply(
      port: Int,
      workers: Seq[Seq[NetAddr]],
      ctx: SchedulerCtx,
      scheduleLogic: ScheduleLogic
  ): Resource[IO, Scheduler] = {
    for {
      supervisor <- Supervisor[IO]

      // Initialize internal shared states.
      workerAddrs <- workerAddresses(workers).toResource
      stateR <- SharedState.init(workerAddrs).toResource

      // Create input queues of each fibers.
      schedulerFiberQueue <- Queue.unbounded[IO, SchedulerFiberEvents].toResource
      mainFiberQueue <- Queue.unbounded[IO, MainFiberEvents].toResource
      rpcClientFiberQueues <- createRpcClientFiberQueues(workerAddrs).toResource

      // Launch each fibers in background
      _ <- Resource.eval {
        for {
          _ <- (
            // Launch scheduler RPC service fiber.
            supervisor.supervise(
              RpcServerFiber.start(port, stateR, schedulerFiberQueue, ctx, workerAddrs).useForever
            ),

            // Launch scheduler fiber.
            supervisor.supervise(
              SchedulerFiber
                .start(
                  stateR,
                  mainFiberQueue,
                  schedulerFiberQueue,
                  rpcClientFiberQueues,
                  scheduleLogic
                )
                .useForever
            ),

            // Launch worker RPC service client fibers for each workers.
            workerAddrs.keys.toList.parTraverse_ { wid =>
              supervisor.supervise(
                WorkerRpcClientFiber
                  .start(stateR, wid, rpcClientFiberQueues(wid), schedulerFiberQueue, ctx)
                  .useForever
              )
            }
          ).parTupled.void
        } yield ()
      }
    } yield new Scheduler {
      override def waitInit: IO[Map[Int, Map[String, FileEntry]]] = for {
        // wait for Initialized event from scheduler fiber
        evt <- mainFiberQueue.take
        _ <- assertIO(
          evt.isInstanceOf[Initialized],
          "event other than initialized is received while waiting for scheduler initialization"
        )
        _ <- IO(logger.info("cluster initialized"))
      } yield evt match {
        case Initialized(files) => files
        case _                  =>
          throw new Unreachable
      }

      override def runJobs(specs: Seq[JobSpec], sync: Boolean = true): IO[JobExecutionResult] =
        for {
          // send jobs to scheduler fiber and wait for result
          _ <- schedulerFiberQueue.offer(new SchedulerFiberEvents.Jobs(specs))
          evt <- mainFiberQueue.take
          result <- evt match {
            case JobCompleted(results, files) =>
              IO.whenA(sync)(runJobs(syncJobSpecs(files), false).void) >>
                IO.pure(new JobExecutionResult(results, files))
            case JobFailed(spec, result) =>
              IO.raiseError[JobExecutionResult](
                new RuntimeException(s"Job execution failed: spec=$spec, result=$result")
              )
            case SystemException(error) => IO.raiseError[JobExecutionResult](error)
            case _                      => unreachableIO[JobExecutionResult]
          }
        } yield result
    }
  }

  /** Convert sequence of sequence of network addresses into map from worker ID to network address.
    *
    * @param workers
    *   sequence of sequence of network addresses.
    * @return
    *   map from worker ID to network address.
    */
  def workerAddresses(workers: Seq[Seq[NetAddr]]): IO[Map[Wid, NetAddr]] = {
    val entries = for {
      (inner, mid) <- workers.zipWithIndex
      (addr, wtid) <- inner.zipWithIndex
    } yield (new Wid(mid, wtid), addr)
    IO.pure(entries.toMap)
  }

  /** Create input queue of each worker RPC client fibers.
    *
    * @param workers
    *   map from worker ID to network address.
    * @return
    *   map from worker ID to queue of each worker RPC client fibers.
    */
  def createRpcClientFiberQueues(
      workers: Map[Wid, NetAddr]
  ): IO[Map[Wid, Queue[IO, WorkerFiberEvents]]] =
    workers.foldLeft(IO.pure(Map[Wid, Queue[IO, WorkerFiberEvents]]())) { case (io, (wid, _)) =>
      for {
        map <- io
        queue <- Queue.unbounded[IO, WorkerFiberEvents]
      } yield map + (wid -> queue)
    }

  /** Create synchronization jobs.
    *
    * @param files
    *   map of files on each machiens.
    * @return
    *   sequence of synchornization jobs to be scheduled.
    */
  def syncJobSpecs(files: Map[Int, Map[String, FileEntry]]): Seq[JobSpec] =
    files.toSeq.map { case (mid, entries) =>
      new JobSpec(name = SYNC_JOB_NAME, args = Seq(entries), inputs = Seq(), outputs = Seq())
    }

  val SYNC_JOB_NAME = "__sync__"
}
