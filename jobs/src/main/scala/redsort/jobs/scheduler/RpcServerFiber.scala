package redsort.jobs.scheduler

import cats._
import cats.effect._
import cats.syntax._
import io.grpc._
import fs2.grpc.syntax.all._
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
import redsort.jobs.messages._
import com.google.protobuf.empty.Empty

object SchedulerRpcService {
  def init(state: Ref[IO, SharedState]): SchedulerFs2Grpc[IO, Metadata] =
    new SchedulerFs2Grpc[IO, Metadata] {
      override def haltOnError(request: JobSystemError, ctx: Metadata): IO[Empty] =
        IO.pure(new Empty())

      override def notifyUp(request: Empty, ctx: Metadata): IO[Empty] = ???

      override def registerWorker(request: WorkerHello, ctx: Metadata): IO[SchedulerHello] = ???
    }
}

object RpcServerFiber {
  private def grpcService(state: Ref[IO, SharedState]): Resource[IO, ServerServiceDefinition] =
    SchedulerFs2Grpc.bindServiceResource[IO](SchedulerRpcService.init(state))

  def start(state: Ref[IO, SharedState]): Resource[IO, Server] =
    grpcService(state)
      .flatMap(service =>
        NettyServerBuilder
          .forPort(5000)
          .addService(service)
          .resource[IO]
      )
      .evalMap(server => IO(server.start()))
}
