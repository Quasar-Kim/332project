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
import redsort.jobs.SourceLogger
import redsort.jobs.messages.FileEntryMsg
import redsort.jobs.JobSystemException
import redsort.jobs.messages.WorkerErrorKind.BODY_ERROR
import redsort.jobs.scheduler
import scala.concurrent.duration._

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

  /** Signal workers to shutdown. Call this method right before exiting your program. This method
    * does NOT wait for workers to shutdown.
    *
    * @return
    */
  def complete: IO[Unit]

  /** Returns configured number of machines.
    */
  def getNumMachines: Int

  /** Returns network address of scheduler RPC server.
    */
  def netAddr: IO[NetAddr]

  /** Returns sequence of worker machine IP addresses. IMPORTANT: This method is only valid after
    * `waitInit`ed.
    */
  def machineAddrs: IO[Seq[String]]

  /** Returns addresses of all workers. */
  def workerAddrs: IO[Map[Wid, NetAddr]]
}

final case class JobExecutionResult(
    results: Seq[Tuple2[JobSpec, JobResult]],
    files: Map[Int, Map[String, FileEntry]]
)

/** Entry point to job scheduling system.
  */
object Scheduler {
  private[this] val logger = new SourceLogger(getLogger, "scheduler")

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
    * @param program
    *   distributed program that uses scheduler.
    * @return
    *   a resource wrapping `Scheduler` logic that allow user to interact with scheduler system.
    */
  def apply[T](
      port: Int,
      numMachines: Int,
      numWorkersPerMachine: Int,
      ctx: SchedulerCtx,
      scheduleLogic: ScheduleLogic = SimpleScheduleLogic
  )(program: Scheduler => IO[T]): IO[T] = {
    for {
      // Initialize internal shared states.
      wids <- workerIds(numMachines, numWorkersPerMachine)
      stateR <- SharedState.init(wids)

      // Create input queues of each fibers.
      schedulerFiberQueue <- Queue.unbounded[IO, SchedulerFiberEvents]
      mainFiberQueue <- Queue.unbounded[IO, MainFiberEvents]
      rpcClientFiberQueues <- createRpcClientFiberQueues(wids)
      rpcServerFiberQueue <- Queue.unbounded[IO, RpcServerFiberEvents]

      // Launch fibers in background
      backgroundFibers = (
        RpcServerFiber
          .start(port, stateR, schedulerFiberQueue, ctx, rpcServerFiberQueue)
          .useForever,
        SchedulerFiber
          .start(
            stateR,
            mainFiberQueue,
            schedulerFiberQueue,
            rpcClientFiberQueues,
            rpcServerFiberQueue,
            scheduleLogic
          )
          .useForever,
        wids.parTraverse_ { wid =>
          WorkerRpcClientFiber
            .start(stateR, wid, rpcClientFiberQueues(wid), schedulerFiberQueue, ctx)
            .useForever
        }
        // REMOVEME
        // IO.raiseError(new RuntimeException("let's see how error is handled here"))
        //   .delayBy(100.millis)
      ).parTupled

      // actual scheduler definition here, bound with arguments passed to apply().
      scheduler = new Scheduler {
        override def waitInit: IO[Map[Int, Map[String, FileEntry]]] = for {
          // wait for Initialized event from scheduler fiber
          evt <- mainFiberQueue.take
          _ <- assertIO(
            evt.isInstanceOf[Initialized],
            "event other than initialized is received while waiting for scheduler initialization"
          )
          _ <- logger.info("cluster initialized")
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
                  new RuntimeException(
                    s"Job execution failed: kind= ${result.error.get.kind}, spec=$spec",
                    result.error.get.inner match {
                      case Some(inner) => JobSystemException.fromMsg(inner, source = "worker")
                      case None        => null
                    }
                  )
                )
              case SystemException(error) => IO.raiseError[JobExecutionResult](error)
              case _                      => unreachableIO[JobExecutionResult]
            }
          } yield result

        override def complete: IO[Unit] =
          for {
            _ <- schedulerFiberQueue.offer(new SchedulerFiberEvents.Complete)
            evt <- mainFiberQueue.take
          } yield {
            assert(evt == MainFiberEvents.CompleteDone)
            ()
          }

        override def getNumMachines: Int = numMachines

        override def netAddr: IO[NetAddr] =
          ctx.getIP.map(ip => new NetAddr(ip, port))

        override def machineAddrs: IO[Seq[String]] =
          stateR.get.map(state =>
            state.schedulerFiber.workers
              .filter { case (wid, _) => wid.wtid == 0 }
              .map { case (wid, workerState) => (wid, workerState.netAddr.get.ip) }
              .toList
              .sortBy { case (wid, _) => wid.mid }
              .map { case (_, ip) => ip }
          )

        override def workerAddrs: IO[Map[Wid, NetAddr]] =
          stateR.get.map(state =>
            state.schedulerFiber.workers.map { case (wid, workerState) =>
              (wid, workerState.netAddr.get)
            }
          )
      }

      // run program with scheduler
      result <- backgroundFibers.race(program(scheduler))
    } yield result match {
      case Left(_)      => throw new Unreachable
      case Right(value) => value
    }
  }

  /** Generate sequence of worker IDs.
    *
    * @param numMachines
    *   number of machines.
    * @param numWorkersPerMachine
    *   number of workers per machines.
    * @return
    */
  def workerIds(numMachines: Int, numWorkersPerMachine: Int): IO[Seq[Wid]] = {
    val wids = for {
      mid <- (0 until numMachines)
      wtid <- (0 until numWorkersPerMachine)
    } yield new Wid(mid, wtid)
    IO.pure(wids.toSeq)
  }

  /** Create input queue of each worker RPC client fibers.
    *
    * @param workers
    *   map from worker ID to network address.
    * @return
    *   map from worker ID to queue of each worker RPC client fibers.
    */
  def createRpcClientFiberQueues(
      wids: Seq[Wid]
  ): IO[Map[Wid, Queue[IO, WorkerFiberEvents]]] =
    wids
      .traverse { wid =>
        for {
          queue <- Queue.unbounded[IO, WorkerFiberEvents]
        } yield (wid, queue)
      }
      .map(_.toMap)

  /** Create synchronization jobs.
    *
    * @param files
    *   map of files on each machiens.
    * @return
    *   sequence of synchornization jobs to be scheduled.
    */
  def syncJobSpecs(files: Map[Int, Map[String, FileEntry]]): Seq[JobSpec] =
    files.toSeq.map { case (mid, entries) =>
      new JobSpec(
        name = SYNC_JOB_NAME,
        args = entries.values.map(FileEntry.toMsg(_)).toSeq,
        inputs = Seq(),
        outputs = Seq()
      )
    }

  val SYNC_JOB_NAME = "__sync__"
}
