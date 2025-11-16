package redsort.jobs.scheduler

import cats._
import cats.effect._
import cats.syntax._
import io.grpc._
import fs2.grpc.syntax.all._
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
import redsort.jobs.messages._
import com.google.protobuf.empty.Empty

class SchedulerRpcService extends SchedulerFs2Grpc[IO, Metadata] {
  override def haltOnError(request: JobSystemError, ctx: Metadata): IO[Empty] =
    IO.pure(new Empty())

  override def notifyUp(request: Empty, ctx: Metadata): IO[Empty] = ???

  override def registerWorker(request: WorkerHello, ctx: Metadata): IO[SchedulerHello] = ???
}

object RpcServerFiber {
  private def grpcService: Resource[IO, ServerServiceDefinition] =
    SchedulerFs2Grpc.bindServiceResource[IO](new SchedulerRpcService)

  def start: IO[Unit] = grpcService.use(service =>
    NettyServerBuilder
      .forPort(5000)
      .addService(service)
      .resource[IO]
      .evalMap(server => IO(server.start()))
      .useForever
  ) >> IO.unit
}
