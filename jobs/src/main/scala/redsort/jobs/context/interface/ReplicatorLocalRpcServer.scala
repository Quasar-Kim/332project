package redsort.jobs.context.interface

import cats.effect._
import redsort.jobs.Common._
import redsort.jobs.messages.ReplicatorLocalServiceFs2Grpc
import io.grpc.Metadata
import io.grpc.Server

trait ReplicatorLocalRpcServer {
  def replicatorLocalRpcServer(
      replicatorAddrs: Map[Mid, NetAddr]
  ): Resource[IO, Server]
}
