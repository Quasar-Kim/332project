package redsort.jobs.context.impl

import redsort.jobs.context.interface.ReplicatorRemoteRpcServer
import cats.effect._
import fs2.grpc.syntax.all._
import io.grpc.Server
import redsort.jobs.context.interface.FileStorage
import io.grpc.Metadata
import redsort.jobs.messages.ReplicatorRemoteServiceFs2Grpc
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
import java.util.concurrent.TimeUnit

trait ProductionReplicatorRemoteRpcServer extends ReplicatorRemoteRpcServer {
  override def replicatorRemoteRpcServer(
      grpc: ReplicatorRemoteServiceFs2Grpc[IO, Metadata],
      port: Int
  ): Resource[IO, Server] =
    ReplicatorRemoteServiceFs2Grpc
      .bindServiceResource[IO](grpc)
      .flatMap(service =>
        NettyServerBuilder
          .forPort(port)
          .permitKeepAliveTime(1, TimeUnit.SECONDS)
          .permitKeepAliveWithoutCalls(true)
          .addService(service)
          .resource[IO]
      )

}
