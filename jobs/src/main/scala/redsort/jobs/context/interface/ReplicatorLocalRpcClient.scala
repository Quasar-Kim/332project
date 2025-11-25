package redsort.jobs.context.interface

import cats._
import cats.effect._
import cats.syntax.all._
import redsort.jobs.messages.ReplicatorLocalServiceFs2Grpc
import io.grpc.Metadata
import redsort.jobs.Common.NetAddr

trait ReplicatorLocalRpcClient {
  def replicatorLocalRpcClient(
      addr: NetAddr
  ): Resource[IO, ReplicatorLocalServiceFs2Grpc[IO, Metadata]]
}
