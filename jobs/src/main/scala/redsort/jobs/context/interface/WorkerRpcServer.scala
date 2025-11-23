package redsort.jobs.context.interface

import cats.effect._
import io.grpc.ServerServiceDefinition
import io.grpc.Server
import redsort.jobs.messages.WorkerFs2Grpc
import io.grpc.Metadata

trait WorkerRpcServer {
  def workerRpcServer(grpc: WorkerFs2Grpc[IO, Metadata], port: Int): Resource[IO, Server]
}
