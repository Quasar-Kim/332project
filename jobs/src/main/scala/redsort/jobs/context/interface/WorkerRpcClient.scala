package redsort.jobs.context.interface

import cats._
import cats.effect._
import cats.syntax._
import redsort.jobs.messages.WorkerFs2Grpc
import io.grpc.Metadata

trait WorkerRpcClient {
  def workerRpcClient(port: Int): Resource[IO, WorkerFs2Grpc[IO, Metadata]]
}
