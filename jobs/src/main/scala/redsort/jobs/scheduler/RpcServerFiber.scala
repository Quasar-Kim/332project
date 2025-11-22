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
import org.log4s._

/* Implementation of scheduler RPC service. */
object SchedulerRpcService {
  private[this] val logger = getLogger

  /** Return a new object that implements `SchedulerFs2Grpc` trait.
    *
    * @param stateR
    *   reference to shared state.
    * @param schedulerFiberQueue
    *   input queue of scheduler fiber.
    * @param ctx
    *   context object that provide server on which RPC server runs.
    * @param workerAddrs
    *   map of worker network addresses.
    * @return
    *   a new `SchedulerFs2Grpc` object.
    */
  def init(
      stateR: Ref[IO, SharedState],
      schedulerFiberQueue: Queue[IO, SchedulerFiberEvents],
      ctx: SchedulerRpcServer,
      workerAddrs: Map[Wid, NetAddr]
  ): SchedulerFs2Grpc[IO, Metadata] =
    new SchedulerFs2Grpc[IO, Metadata] {
      override def haltOnError(req: HaltRequest, meta: Metadata): IO[Empty] = for {
        _ <- IO(logger.error(s"haltOnError called by ${req.source}: ${req.err}"))
        _ <- schedulerFiberQueue.offer(
          new SchedulerFiberEvents.Halt(req.err, Wid.fromMsg(req.source))
        )
      } yield new Empty()

      override def notifyUp(request: NotifyUpRequest, ctx: Metadata): IO[Empty] = ???

      // NOTE: this method should be ALSO considered as heartbeat message,
      // otherwise in case where some event that triggers reschedule arrives later than this message
      // can corrupt worker state.
      override def registerWorker(hello: WorkerHello, meta: Metadata): IO[SchedulerHello] = for {
        wid <- IO.pure(resolveWidFromNetAddr(workerAddrs, new NetAddr(hello.ip, hello.port)))
        _ <- IO(logger.info(s"Worker $wid registered"))
        _ <- schedulerFiberQueue.offer(new SchedulerFiberEvents.WorkerRegistration(hello, wid))
      } yield {
        new SchedulerHello(
          mid = wid.mid,
          replicatorAddrs = resolveReplicatorNetAddrs(workerAddrs)
        )
      }
    }

  def resolveWidFromNetAddr(workerAddrs: Map[Wid, NetAddr], netAddr: NetAddr): Wid =
    workerAddrs.find { case (_, addr) => addr == netAddr }.get._1

  def resolveReplicatorNetAddrs(workerAddrs: Map[Wid, NetAddr]): Map[Int, NetAddrMsg] =
    workerAddrs.filter { case (wid, _) => wid.wtid == 0 }.map { case (wid, netAddr) =>
      (wid.mid, new NetAddrMsg(netAddr.ip, netAddr.port - 1))
    }

}

/* Fiber serving scheduler RPC service. */
object RpcServerFiber {
  private[this] val logger = getLogger

  /** Start a RPC server.
    *
    * @param port
    *   server port.
    * @param stateR
    *   reference to shared states.
    * @param schedulerFiberQueue
    *   input queue of scheduler fiber.
    * @param ctx
    *   map of worker network addresses.
    * @param workerAddrs
    *   map of worker network addresses.
    * @return
    *   `Resource` wrapping a RPC server.
    */
  def start(
      port: Int,
      stateR: Ref[IO, SharedState],
      schedulerFiberQueue: Queue[IO, SchedulerFiberEvents],
      ctx: SchedulerRpcServer,
      workerAddrs: Map[Wid, NetAddr]
  ): Resource[IO, Server] =
    IO(logger.debug("scheduler RPC server fiber started")).toResource.flatMap { _ =>
      ctx
        .schedulerRpcServer(
          SchedulerRpcService.init(stateR, schedulerFiberQueue, ctx, workerAddrs),
          port
        )
        .evalMap(server => IO(server.start()))
    }
}
