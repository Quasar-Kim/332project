package redsort.jobs.context.interface

import cats._
import cats.effect._
import cats.syntax.all._
import redsort.jobs.messages.SchedulerFs2Grpc
import io.grpc.Metadata
import redsort.jobs.Common.NetAddr

trait SchedulerRpcClient {
  def schedulerRpcClient(addr: NetAddr): Resource[IO, SchedulerFs2Grpc[IO, Metadata]]
}
