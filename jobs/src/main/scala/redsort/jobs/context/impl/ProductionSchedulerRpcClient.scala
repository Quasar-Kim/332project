package redsort.jobs.context.impl

import cats.effect._
import cats.syntax.all._
import fs2.grpc.syntax.all._
import redsort.jobs.messages.SchedulerFs2Grpc
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder
import com.google.protobuf.empty.Empty
import redsort.jobs.context.interface.SchedulerRpcClient
import io.grpc.Metadata
import redsort.jobs.Common.NetAddr
import io.grpc.ManagedChannel
import io.grpc.ConnectivityState
import scala.concurrent.duration._

trait ProductionSchedulerRpcClient extends SchedulerRpcClient {
  override def schedulerRpcClient(addr: NetAddr): Resource[IO, SchedulerFs2Grpc[IO, Metadata]] =
    for {
      channel <- NettyChannelBuilder
        .forAddress(addr.ip, addr.port)
        .usePlaintext()
        .resource[IO]
      _ <- awaitConnection(channel).toResource
      grpc <- SchedulerFs2Grpc.stubResource[IO](channel)
    } yield grpc

  /** Repeatedly try connection for every 1 second until conneciton is established.
    */
  def awaitConnection(channel: ManagedChannel): IO[Unit] = {
    def waitUntilReady(currentState: ConnectivityState): IO[Unit] =
      if (currentState == ConnectivityState.READY) IO.unit
      else
        for {
          _ <- IO.async_((cb: Either[Throwable, Unit] => Unit) =>
            channel.notifyWhenStateChanged(currentState, () => cb(Right(())))
          )
          nextState <- IO(channel.getState(true))
          _ <- waitUntilReady(nextState)
        } yield ()

    IO(channel.getState(true))
      .flatMap(waitUntilReady)
      .timeoutTo(
        5.minute,
        IO.raiseError(new RuntimeException("scheduler gRPC connection timed out"))
      )
  }
}
