package redsort.jobs.scheduler

import cats._
import cats.effect._
import cats.effect.std.Supervisor
import cats.syntax.all._
import redsort.jobs.Common._
import scala.collection.immutable.Queue

trait Scheduler {}

object Scheduler {
  def apply(workers: Seq[Seq[NetAddr]]): Resource[IO, Scheduler] =
    for {
      supervisor <- Supervisor[IO]
      _ <- Resource.eval {
        for {
          workerAddrs <- workerAddresses(workers)
          stateR <- SharedState.init(workerAddrs)
          _ <- (
            supervisor.supervise(RpcServerFiber.start(stateR).useForever),
            supervisor.supervise(SchedulerFiber.start(stateR).useForever),
            workerAddrs.keys.toList.parTraverse_ { wid =>
              supervisor.supervise(WorkerRpcClientFiber.start(stateR, wid).useForever)
            }
          ).parTupled.void
        } yield ()
      }
    } yield new Scheduler {}

  def workerAddresses(workers: Seq[Seq[NetAddr]]): IO[Map[Wid, NetAddr]] = {
    val entries = for {
      (inner, mid) <- workers.zipWithIndex
      (addr, wtid) <- inner.zipWithIndex
    } yield (new Wid(mid, wtid), addr)
    IO.pure(entries.toMap)
  }
}
