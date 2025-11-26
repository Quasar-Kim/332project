package redsort.jobs.context.interface

import cats.effect.{IO, Resource}
import io.grpc.{Metadata, Server}
import redsort.jobs.Common.NetAddr
import redsort.jobs.messages.ReplicatorRemoteServiceFs2Grpc

trait ReplicatorRemoteRpcServer {
  def replicatorRemoteRpcServer(
      grpc: ReplicatorRemoteServiceFs2Grpc[IO, Metadata],
      port: Int
  ): Resource[IO, Server]
}
