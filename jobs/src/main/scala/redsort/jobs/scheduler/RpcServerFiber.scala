package redsort.jobs.scheduler

import cats._
import cats.effect._
import cats.syntax.all._
import io.grpc._
import fs2.grpc.syntax.all._
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
import redsort.jobs.messages._
import com.google.protobuf.empty.Empty
import cats.effect.std.Queue
import redsort.jobs.context.SchedulerCtx
import redsort.jobs.context.interface.SchedulerRpcServer
import redsort.jobs.Common._
import redsort.jobs.scheduler.RpcServerFiberEvents._
import org.log4s._
import redsort.jobs.SourceLogger

/* Implementation of scheduler RPC service. */
object SchedulerRpcService {
  private[this] val logger = new SourceLogger(getLogger, "scheduler")

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
      inputQueue: Queue[IO, RpcServerFiberEvents]
  ): SchedulerFs2Grpc[IO, Metadata] =
    new SchedulerFs2Grpc[IO, Metadata] {
      override def haltOnError(req: HaltRequest, meta: Metadata): IO[Empty] = for {
        _ <- logger.error(s"haltOnError called by ${req.source}: ${req.err}")
        _ <- schedulerFiberQueue.offer(
          new SchedulerFiberEvents.Halt(req.err, Wid.fromMsg(req.source))
        )
      } yield new Empty()

      override def notifyUp(request: NotifyUpRequest, ctx: Metadata): IO[Empty] = ???

      // NOTE: this method should be ALSO considered as heartbeat message,
      // otherwise in case where some event that triggers reschedule arrives later than this message
      // can corrupt worker state.
      override def registerWorker(hello: WorkerHello, meta: Metadata): IO[SchedulerHello] =
        for {
          netAddr <- IO.pure(new NetAddr(hello.ip, hello.port))
          _ <- logger.info(s"new worker registration from $netAddr")
          _ <- schedulerFiberQueue.offer(new SchedulerFiberEvents.WorkerRegistration(hello))

          // wait for scheduler tells us that all workers are initialized
          // then create SchedulerHello
          evt <- inputQueue.take
          state <- stateR.get
          schedulerHello <- IO(evt match {
            case AllWorkersInitialized(replictorAddrs) =>
              buildSchedulerHello(state, replictorAddrs, netAddr)
          })
        } yield schedulerHello
    }

  def buildSchedulerHello(
      state: SharedState,
      replictorAddrs: Map[Mid, NetAddr],
      netAddr: NetAddr
  ): SchedulerHello = {
    state.schedulerFiber.workers.find { case (wid, state) => state.netAddr.get == netAddr } match {
      case Some((wid, _)) =>
        new SchedulerHello(
          mid = wid.mid,
          replicatorAddrs = replictorAddrs.view.mapValues(NetAddr.toMsg(_)).toMap,
          success = true
        )
      case None =>
        new SchedulerHello(
          success = false,
          failReason =
            "invalid network address - maybe there are more workers than scheduler knows?"
        )
    }

  }

  def badSchedulerHello(reason: String): SchedulerHello =
    new SchedulerHello(
      mid = -1,
      replicatorAddrs = Map(),
      success = false,
      failReason = reason
    )
}

/* Fiber serving scheduler RPC service. */
object RpcServerFiber {
  private[this] val logger = new SourceLogger(getLogger, "scheduler")

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
      inputQueue: Queue[IO, RpcServerFiberEvents]
  ): Resource[IO, Server] =
    logger.debug("scheduler RPC server fiber started").toResource.flatMap { _ =>
      ctx
        .schedulerRpcServer(
          SchedulerRpcService.init(stateR, schedulerFiberQueue, ctx, inputQueue),
          port
        )
        .evalMap(server => IO(server.start()))
    }
}
