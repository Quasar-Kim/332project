package redsort.jobs.scheduler

import cats._
import cats.effect._
import cats.effect.std.Queue
import cats.syntax._
import redsort.jobs.Common._
import scala.concurrent.duration._
import redsort.jobs.messages.WorkerFs2Grpc
import io.grpc.Metadata
import redsort.jobs.context.interface.WorkerRpcClient
import redsort.jobs.scheduler
import org.log4s._
import redsort.jobs.SourceLogger
import com.google.protobuf.empty.Empty

object WorkerRpcClientFiber {
  private[this] val logger = new SourceLogger(getLogger, "scheduler")

  def start(
      stateR: Ref[IO, SharedState],
      wid: Wid,
      inputQueue: Queue[IO, WorkerFiberEvents],
      schedulerFiberQueue: Queue[IO, SchedulerFiberEvents],
      ctx: WorkerRpcClient
  ): Resource[IO, Unit] =
    for {
      serverAddr <- stateR.get.map(_.schedulerFiber.workers(wid).netAddr).toResource
      _ <- logger.debug(s"RPC client fiber for $wid started").toResource
      _ <- ctx
        .workerRpcClient(serverAddr)
        .flatMap(rpcClient =>
          main(stateR, wid, inputQueue, schedulerFiberQueue, rpcClient).background
        )
    } yield ()

  private def main(
      stateR: Ref[IO, SharedState],
      wid: Wid,
      queue: Queue[IO, WorkerFiberEvents],
      schedulerFiberQueue: Queue[IO, SchedulerFiberEvents],
      rpcClient: WorkerFs2Grpc[IO, Metadata]
  ): IO[Unit] = for {
    event <- queue.take
    _ <- logger.debug(s"received event $event")
    _ <- handleEvent(stateR, wid, event, schedulerFiberQueue, rpcClient)
    _ <- main(stateR, wid, queue, schedulerFiberQueue, rpcClient)
  } yield ()

  private def handleEvent(
      stateR: Ref[IO, SharedState],
      wid: Wid,
      event: WorkerFiberEvents,
      schedulerFiberQueue: Queue[IO, SchedulerFiberEvents],
      rpcClient: WorkerFs2Grpc[IO, Metadata]
  ): IO[Unit] = event match {
    case WorkerFiberEvents.Job(spec) =>
      for {
        _ <- logger.debug(s"got job spec $spec")
        result <- rpcClient.runJob(JobSpec.toMsg(spec), new Metadata)
        _ <- schedulerFiberQueue.offer(
          if (result.success) new SchedulerFiberEvents.JobCompleted(result, wid)
          else new SchedulerFiberEvents.JobFailed(result, wid)
        )
      } yield ()

    case WorkerFiberEvents.Complete =>
      for {
        _ <- logger.debug(s"shutting down worker $wid")
        _ <- rpcClient.complete(new Empty, new Metadata)
        _ <- schedulerFiberQueue.offer(new SchedulerFiberEvents.WorkerCompleted(from = wid))
      } yield ()

    case _ => notImplmenetedIO
  }
}
