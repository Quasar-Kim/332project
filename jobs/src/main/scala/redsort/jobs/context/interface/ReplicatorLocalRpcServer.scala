package redsort.jobs.context.interface

import cats.effect._
import redsort.jobs.Common._
import io.grpc.Metadata
import io.grpc.Server
import redsort.jobs.messages.ReplicatorLocalServiceFs2Grpc

trait ReplicatorLocalRpcServer {
  def replicatorLocalRpcServer(
      grpc: ReplicatorLocalServiceFs2Grpc[IO, Metadata],
      port: Int
  ): Resource[IO, Server]
}
