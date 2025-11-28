package redsort.jobs.context.interface

import cats.effect._
import redsort.jobs.Common._
import redsort.jobs.messages.ReplicatorLocalServiceFs2Grpc
import io.grpc.Metadata
import io.grpc.Server
import redsort.jobs.messages.ReplicatorRemoteServiceFs2Grpc

trait ReplicatorRemoteRpcServer {
  def replicatorRemoteRpcServer(
      grpc: ReplicatorRemoteServiceFs2Grpc[IO, Metadata],
      port: Int
  ): Resource[IO, Server]
}
