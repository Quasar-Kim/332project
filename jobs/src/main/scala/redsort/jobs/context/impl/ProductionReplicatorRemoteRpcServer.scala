package redsort.jobs.context.impl

import redsort.jobs.context.interface.ReplicatorRemoteRpcServer
import cats.effect.{IO, Resource}
import io.grpc.{Metadata, Server}
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
import fs2.grpc.syntax.all._
import redsort.jobs.Common.NetAddr
import redsort.jobs.messages.ReplicatorRemoteServiceFs2Grpc

trait ProductionReplicatorRemoteRpcServer extends ReplicatorRemoteRpcServer {
  def replicatorRemoteRpcServer(
      grpc: ReplicatorRemoteServiceFs2Grpc[IO, Metadata],
      addr: NetAddr
  ): Resource[IO, Server] =
    ReplicatorRemoteServiceFs2Grpc
      .bindServiceResource[IO](grpc)
      .flatMap(service =>
        NettyServerBuilder
          .forPort(addr.port)
          .addService(service)
          .resource[IO]
      )
}
