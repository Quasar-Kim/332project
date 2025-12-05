package redsort.jobs.context.impl

import redsort.jobs.context.interface.ReplicatorRemoteRpcClient
import cats.effect._
import io.grpc.Metadata
import redsort.jobs.Common
import redsort.jobs.messages.ReplicatorRemoteServiceFs2Grpc
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder
import fs2.grpc.syntax.all._
import java.util.concurrent.TimeUnit

trait ProductionReplicatorRemoteRpcClient extends ReplicatorRemoteRpcClient {
  def replicatorRemoteRpcClient(
      addr: Common.NetAddr
  ): Resource[IO, ReplicatorRemoteServiceFs2Grpc[IO, Metadata]] =
    NettyChannelBuilder
      .forAddress(addr.ip, addr.port)
      .usePlaintext()
      .keepAliveTime(15, TimeUnit.SECONDS)
      .keepAliveTimeout(5, TimeUnit.SECONDS)
      .keepAliveWithoutCalls(false)
      .resource[IO]
      .flatMap(channel => ReplicatorRemoteServiceFs2Grpc.stubResource[IO](channel))
}
