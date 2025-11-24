package redsort.jobs.context.impl

import redsort.jobs.context.interface.ReplicatorLocalRpcServer
import cats.effect.{IO, Resource}
import io.grpc.{Metadata, Server}
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
import fs2.grpc.syntax.all._
import redsort.jobs.Common.NetAddr
import redsort.jobs.messages.ReplicatorLocalServiceFs2Grpc

trait ProductionReplicatorLocalRpcServer extends ReplicatorLocalRpcServer {
  override def replicatorLocalRpcServer(
      grpc: ReplicatorLocalServiceFs2Grpc[IO, Metadata],
      addr: NetAddr
  ): Resource[IO, Server] =
    ReplicatorLocalServiceFs2Grpc
      .bindServiceResource[IO](grpc)
      .flatMap(service =>
        NettyServerBuilder
          .forPort(addr.port)
          .addService(service)
          .resource[IO]
      )
}
