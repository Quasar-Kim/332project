package redsort.jobs.context.impl

import cats.effect.{IO, Resource}
import io.grpc.Metadata
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder
import fs2.grpc.syntax.all._
import redsort.jobs.Common.NetAddr
import redsort.jobs.context.interface.ReplicatorRemoteRpcClient
import redsort.jobs.messages.ReplicatorRemoteServiceFs2Grpc

trait ProductionReplicatorRemoteRpcClient extends ReplicatorRemoteRpcClient {
  override def replicatorRemoteRpcClient(
      addr: NetAddr
  ): Resource[IO, ReplicatorRemoteServiceFs2Grpc[IO, Metadata]] =
    NettyChannelBuilder
      .forAddress(addr.ip, addr.port)
      .usePlaintext()
      .resource[IO]
      .flatMap(channel => ReplicatorRemoteServiceFs2Grpc.stubResource(channel))
}
