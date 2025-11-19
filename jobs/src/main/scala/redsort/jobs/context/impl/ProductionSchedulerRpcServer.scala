package redsort.jobs.context.impl

import redsort.jobs.context.interface.SchedulerRpcServer
import cats.effect._
import cats.syntax._
import fs2.grpc.syntax.all._
import cats.effect.Resource
import io.grpc.{Server, ServerServiceDefinition}
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
import redsort.jobs.messages.SchedulerFs2Grpc
import io.grpc.Metadata

trait ProductionSchedulerRpcServer extends SchedulerRpcServer {
  def schedulerRpcServer(grpc: SchedulerFs2Grpc[IO, Metadata], port: Int): Resource[IO, Server] =
    SchedulerFs2Grpc
      .bindServiceResource[IO](grpc)
      .flatMap(service =>
        NettyServerBuilder
          .forPort(port)
          .addService(service)
          .resource[IO]
      )
}
