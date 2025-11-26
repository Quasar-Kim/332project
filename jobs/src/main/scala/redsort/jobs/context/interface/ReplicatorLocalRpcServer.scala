package redsort.jobs.context.interface

import cats.effect.{IO, Resource}
import io.grpc.{Metadata, Server}
import redsort.jobs.Common.NetAddr
import redsort.jobs.messages.ReplicatorLocalServiceFs2Grpc

trait ReplicatorLocalRpcServer {
  def replicatorLocalRpcServer(
      grpc: ReplicatorLocalServiceFs2Grpc[IO, Metadata],
      port: Int
  ): Resource[IO, Server]
}
