package redsort.jobs.scheduler

import cats._
import cats.effect._
import cats.effect.std.{Supervisor, Queue}
import cats.syntax.all._
import redsort.jobs.Common._
import redsort.jobs.context.SchedulerCtx

trait Scheduler {
  def waitInit: IO[Unit]
  def runJobs: IO[Unit]
}

object Scheduler {
  def apply(
      workers: Seq[Seq[NetAddr]],
      ctx: SchedulerCtx,
      scheduleLogic: ScheduleLogic
  ): Resource[IO, Scheduler] = {
    for {
      workerAddrs <- workerAddresses(workers).toResource
      supervisor <- Supervisor[IO]
      schedulerFiberQueue <- Queue.unbounded[IO, SchedulerFiberEvents].toResource
      rpcClientFiberQueues <- createRpcClientFiberQueues(workerAddrs).toResource
      mainFiberQueue <- Queue.unbounded[IO, MainFiberEvents].toResource
      stateR <- SharedState.init(workerAddrs).toResource
      _ <- Resource.eval {
        for {
          _ <- (
            // TODO: we need mechanism to allocate port numbers without overlapping one
            // to allow local testing
            supervisor.supervise(
              RpcServerFiber.start(5000, stateR, schedulerFiberQueue, ctx, workerAddrs).useForever
            ),
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

      override def runJobs: IO[Unit] =
        IO.raiseError(new NotImplementedError)
    }
  }

  def workerAddresses(workers: Seq[Seq[NetAddr]]): IO[Map[Wid, NetAddr]] = {
    val entries = for {
      (inner, mid) <- workers.zipWithIndex
      (addr, wtid) <- inner.zipWithIndex
    } yield (new Wid(mid, wtid), addr)
    IO.pure(entries.toMap)
  }

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
