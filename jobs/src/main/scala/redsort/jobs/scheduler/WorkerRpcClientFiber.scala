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

object WorkerRpcClientFiber {
  def start(
      stateR: Ref[IO, SharedState],
      wid: Wid,
      queue: Queue[IO, WorkerFiberEvents],
      ctx: WorkerRpcClient
  ): Resource[IO, Unit] =
    ctx
      .workerRpcClient(5000)
      .flatMap(rpcClient => main(stateR, wid, queue, rpcClient).background)
      .evalMap(_ => IO.unit)

  private def main(
      stateR: Ref[IO, SharedState],
      wid: Wid,
      queue: Queue[IO, WorkerFiberEvents],
      rpcClient: WorkerFs2Grpc[IO, Metadata]
  ): IO[Unit] = for {
    event <- queue.take
    _ <- handleEvent(stateR, wid, event, rpcClient)
    _ <- main(stateR, wid, queue, rpcClient)
  } yield ()

  private def handleEvent(
      stateR: Ref[IO, SharedState],
      wid: Wid,
      event: WorkerFiberEvents,
      rpcClient: WorkerFs2Grpc[IO, Metadata]
  ): IO[Unit] = event match {
    case WorkerFiberEvents.Job(spec) =>
      rpcClient.runJob(JobSpec.toMsg(spec), new Metadata) >> IO.unit
    case _ => ???
  }
}
