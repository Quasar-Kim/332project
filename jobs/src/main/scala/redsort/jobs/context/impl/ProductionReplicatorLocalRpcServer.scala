package redsort.jobs.context.impl

import cats.effect._
import fs2.grpc.syntax.all._
import io.grpc.Server
import redsort.jobs.context.interface.FileStorage
import io.grpc.Metadata
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
import redsort.jobs.replicator.ReplicatorLocalService
import redsort.jobs.context.interface.ReplicatorLocalRpcServer
import redsort.jobs.messages.ReplicatorLocalServiceFs2Grpc

trait ProductionReplicatorLocalRpcServer extends ReplicatorLocalRpcServer {
  override def replicatorLocalRpcServer(
      grpc: ReplicatorLocalServiceFs2Grpc[IO, Metadata],
      port: Int
  ): Resource[IO, Server] =
    ReplicatorLocalServiceFs2Grpc
      .bindServiceResource[IO](grpc)
      .flatMap(service =>
        NettyServerBuilder
          .forPort(port)
          .addService(service)
          .resource[IO]
      )
}
