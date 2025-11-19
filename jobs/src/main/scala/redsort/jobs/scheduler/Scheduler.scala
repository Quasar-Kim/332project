package redsort.jobs.scheduler

import cats._
import cats.effect._
import cats.effect.std.{Supervisor, Queue}
import cats.syntax.all._
import redsort.jobs.Common._
import redsort.jobs.context.SchedulerCtx

trait Scheduler {}

object Scheduler {
  def apply(workers: Seq[Seq[NetAddr]], ctx: SchedulerCtx): Resource[IO, Scheduler] = {
    for {
      workerAddrs <- workerAddresses(workers).toResource
      supervisor <- Supervisor[IO]
      schedulerFiberQueue <- Queue.unbounded[IO, SchedulerFiberEvents].toResource
      rpcClientFiberQueues <- createRpcClientFiberQueues(workerAddrs).toResource
      _ <- Resource.eval {
        for {
          stateR <- SharedState.init(workerAddrs)
          _ <- (
            supervisor.supervise(RpcServerFiber.start(stateR).useForever),
            supervisor.supervise(SchedulerFiber.start(stateR).useForever),
            workerAddrs.keys.toList.parTraverse_ { wid =>
              supervisor.supervise(
                WorkerRpcClientFiber.start(stateR, wid, rpcClientFiberQueues(wid), ctx).useForever
              )
            }
          ).parTupled.void
        } yield ()
      }
    } yield new Scheduler {}
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
