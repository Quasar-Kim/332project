package redsort.jobs.scheduler

import cats._
import cats.effect._
import cats.effect.std.{Supervisor, Queue}
import cats.syntax.all._
import redsort.jobs.Common._
import redsort.jobs.context.SchedulerCtx
import redsort.jobs.messages.JobResult

/** A frontend of job scheduling system.
  */
trait Scheduler {

  /** Wait for all workers to be initialized.
    */
  def waitInit: IO[Unit]

  /** Run requested jobs in worker cluster.
    *
    * @param specs
    *   a sequence of job specifications to launch.
    *
    * @return
    *   a sequence of tuple of each job specifications and its result.
    */
  def runJobs(specs: Seq[JobSpec]): IO[Seq[Tuple2[JobSpec, JobResult]]]
}

/** Entry point to job scheduling system.
  */
object Scheduler {

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
      override def waitInit: IO[Unit] = for {
        state <- stateR.get
        _ <- assertIO(
          state.schedulerFiber.state == SchedulerState.Initializing,
          "state must be initializing"
        )
        evt <- mainFiberQueue.take
        _ <- assertIO(
          evt == MainFiberEvents.Initialized,
          "initialized event must be emitted"
        )
      } yield ()

      def runJobs(specs: Seq[JobSpec]): IO[Seq[Tuple2[JobSpec, JobResult]]] =
        IO.raiseError(new NotImplementedError)
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
}
