package redsort.jobs.context.interface

import cats.effect._
import io.grpc.ServerServiceDefinition
import io.grpc.Server
import redsort.jobs.messages.SchedulerFs2Grpc
import io.grpc.Metadata

trait SchedulerRpcServer {
  def schedulerRpcServer(grpc: SchedulerFs2Grpc[IO, Metadata], port: Int): Resource[IO, Server]
}
