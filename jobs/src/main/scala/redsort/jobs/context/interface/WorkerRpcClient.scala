package redsort.jobs.context.interface

import cats._
import cats.effect._
import cats.syntax.all._
import redsort.jobs.messages.WorkerFs2Grpc
import io.grpc.Metadata
import redsort.jobs.Common.NetAddr

trait WorkerRpcClient {
  def workerRpcClient(addr: NetAddr): Resource[IO, WorkerFs2Grpc[IO, Metadata]]
}
