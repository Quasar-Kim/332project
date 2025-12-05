package redsort.jobs.context.impl

import cats.effect._
import cats.syntax.all._
import fs2.grpc.syntax.all._
import redsort.jobs.messages.WorkerFs2Grpc
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder
import com.google.protobuf.empty.Empty
import redsort.jobs.context.interface.WorkerRpcClient
import io.grpc.Metadata
import redsort.jobs.Common.NetAddr
import java.util.concurrent.TimeUnit

trait ProductionWorkerRpcClient extends WorkerRpcClient {
  def workerRpcClient(addr: NetAddr): Resource[IO, WorkerFs2Grpc[IO, Metadata]] =
    NettyChannelBuilder
      .forAddress(addr.ip, addr.port)
      .usePlaintext()
      .keepAliveTime(10, TimeUnit.SECONDS)
      .keepAliveTimeout(5, TimeUnit.SECONDS)
      .keepAliveWithoutCalls(true)
      .resource[IO]
      .flatMap(channel => WorkerFs2Grpc.stubResource(channel))
}
