package redsort.jobs.context.impl

import cats.effect._
import cats.syntax._
import fs2.grpc.syntax.all._
import redsort.jobs.messages.WorkerFs2Grpc
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder
import com.google.protobuf.empty.Empty
import redsort.jobs.messages.WorkerHaltRequest
import redsort.jobs.context.interface.WorkerRpcClient
import io.grpc.Metadata

trait ProductionWorkerRpcClient extends WorkerRpcClient {
  def workerRpcClient(port: Int): Resource[IO, WorkerFs2Grpc[IO, Metadata]] =
    NettyChannelBuilder
      .forAddress("0.0.0.0", port)
      .usePlaintext()
      .resource[IO]
      .flatMap(channel => WorkerFs2Grpc.stubResource(channel))
}
