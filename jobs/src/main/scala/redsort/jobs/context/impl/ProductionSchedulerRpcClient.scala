package redsort.jobs.context.impl

import cats.effect._
import cats.syntax._
import fs2.grpc.syntax.all._
import redsort.jobs.messages.SchedulerFs2Grpc
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder
import com.google.protobuf.empty.Empty
import redsort.jobs.context.interface.SchedulerRpcClient
import io.grpc.Metadata
import redsort.jobs.Common.NetAddr

trait ProductionSchedulerRpcClient extends SchedulerRpcClient {
  override def schedulerRpcClient(addr: NetAddr): Resource[IO, SchedulerFs2Grpc[IO, Metadata]] =
    NettyChannelBuilder
      .forAddress(addr.ip, addr.port)
      .usePlaintext()
      .resource[IO]
      .flatMap(channel => SchedulerFs2Grpc.stubResource(channel))
}
