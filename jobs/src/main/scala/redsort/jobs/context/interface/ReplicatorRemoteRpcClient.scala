package redsort.jobs.context.interface

import cats.effect._
import redsort.jobs.messages.ReplicatorRemoteServiceFs2Grpc
import redsort.jobs.Common._
import io.grpc.Metadata

trait ReplicatorRemoteRpcClient {
  def replicatorRemoteRpcClient(
      addr: NetAddr
  ): Resource[IO, ReplicatorRemoteServiceFs2Grpc[IO, Metadata]]
}
