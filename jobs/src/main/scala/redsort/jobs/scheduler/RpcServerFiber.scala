package redsort.jobs.scheduler

import cats._
import cats.effect._
import cats.syntax._
import io.grpc._
import fs2.grpc.syntax.all._
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
import redsort.jobs.messages._
import com.google.protobuf.empty.Empty
import cats.effect.std.Queue
import redsort.jobs.context.SchedulerCtx
import redsort.jobs.context.interface.SchedulerRpcServer
import redsort.jobs.Common._
import redsort.jobs.scheduler

object SchedulerRpcService {
  def init(
      stateR: Ref[IO, SharedState],
      schedulerFiberQueue: Queue[IO, SchedulerFiberEvents],
      ctx: SchedulerRpcServer,
      workerAddrs: Map[Wid, NetAddr]
  ): SchedulerFs2Grpc[IO, Metadata] =
    new SchedulerFs2Grpc[IO, Metadata] {
      override def haltOnError(req: HaltRequest, meta: Metadata): IO[Empty] = for {
        _ <- schedulerFiberQueue.offer(
          new SchedulerFiberEvents.Halt(req.err, Wid.fromMsg(req.source))
        )
      } yield new Empty()

      override def notifyUp(request: Empty, meta: Metadata): IO[Empty] = IO.pure(new Empty())

      override def registerWorker(hello: WorkerHello, meta: Metadata): IO[SchedulerHello] = for {
        _ <- schedulerFiberQueue.offer(new SchedulerFiberEvents.WorkerRegistration(hello))
      } yield {
        val mid = workerAddrs.find(_._2.ip == hello.ip).get._1.mid
        new SchedulerHello(
          mid = mid
        )
      }
    }
}

object RpcServerFiber {
  def start(
      port: Int,
      stateR: Ref[IO, SharedState],
      schedulerFiberQueue: Queue[IO, SchedulerFiberEvents],
      ctx: SchedulerRpcServer,
      workerAddrs: Map[Wid, NetAddr]
  ): Resource[IO, Server] =
    ctx
      .schedulerRpcServer(
        SchedulerRpcService.init(stateR, schedulerFiberQueue, ctx, workerAddrs),
        5000
      )
      .evalMap(server => IO(server.start()))
}
