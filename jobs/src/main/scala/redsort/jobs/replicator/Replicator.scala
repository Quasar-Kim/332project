package redsort.jobs.replicator

import cats._
import cats.effect._
import cats.syntax.all._
import redsort.jobs.Common.Mid
import redsort.jobs.context.interface.FileStorage
import redsort.jobs.replicator.ReplicatorLocalService.ClientType
import redsort.jobs.worker.Directories
import redsort.jobs.Common.NetAddr
import redsort.jobs.context.ReplicatorCtx
import redsort.jobs.context.interface.ReplicatorLocalRpcServer
import redsort.jobs.SourceLogger
import org.log4s._

object Replicator {
  private[this] val logger = new SourceLogger(getLogger, "replicator")

  /** Start file replicator server. This method does not return unless internal error happenes.
    */
  def start(
      replicatorAddrs: Map[Mid, NetAddr],
      ctx: ReplicatorCtx,
      dirs: Directories,
      localPort: Int,
      remotePort: Int
  ): IO[Unit] = {
    val startLocal = startLocalRpcServer(
      replicatorAddrs = replicatorAddrs,
      ctx = ctx,
      dirs = dirs,
      port = localPort
    )
    val startRemote = startRemoteRpcServer(ctx = ctx, dirs = dirs, port = remotePort)

    (
      startLocal,
      startRemote
    ).parMapN((_, _) => IO.unit)
  }

  def startLocalRpcServer(
      replicatorAddrs: Map[Mid, NetAddr],
      ctx: ReplicatorCtx,
      dirs: Directories,
      port: Int
  ): IO[Unit] = {
    val entries = replicatorAddrs.toList
    val clientsRes =
      entries.map(_._2).toList.parTraverse(addr => ctx.replicatorRemoteRpcClient(addr))
    val mids = entries.map(_._1)

    for {
      _ <- logger.info(s"starting local replicator server at port $port")
      _ <- clientsRes.use { clients =>
        val clientsMap = mids.lazyZip(clients).toMap

        val grpc = ReplicatorLocalService.init(
          replicatorAddrs = replicatorAddrs,
          clients = clientsMap,
          ctx = ctx,
          dirs = dirs
        )
        ctx
          .replicatorLocalRpcServer(grpc, port)
          .evalMap(server => IO(server.start()))
          .useForever
          .flatMap(_ => IO.unit)
      }
    } yield ()
  }

  def startRemoteRpcServer(
      ctx: ReplicatorCtx,
      dirs: Directories,
      port: Int
  ): IO[Unit] = {
    val grpc = ReplicatorRemoteService.init(ctx = ctx, dirs = dirs)

    for {
      _ <- logger.info(s"starting remote replicator server at port $port")
      _ <- ctx
        .replicatorRemoteRpcServer(grpc, port)
        .evalMap(server => IO(server.start()))
        .useForever
        .flatMap(_ => IO.unit)
    } yield ()
  }
}
