package redsort.jobs.context.impl

import cats.effect._
import cats.syntax.all._
import fs2.grpc.syntax.all._
import redsort.jobs.messages.ReplicatorLocalServiceFs2Grpc
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder
import com.google.protobuf.empty.Empty
import redsort.jobs.context.interface.ReplicatorLocalRpcClient
import io.grpc.Metadata
import redsort.jobs.Common.NetAddr

trait ProductionReplicatorLocalRpcClient extends ReplicatorLocalRpcClient {
  override def replicatorLocalRpcClient(
      addr: NetAddr
  ): Resource[IO, ReplicatorLocalServiceFs2Grpc[IO, Metadata]] =
    NettyChannelBuilder
      .forAddress(addr.ip, addr.port)
      .usePlaintext()
      .resource[IO]
      .flatMap(channel => ReplicatorLocalServiceFs2Grpc.stubResource(channel))
}
