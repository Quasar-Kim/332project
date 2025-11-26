package redsort.jobs.fileservice

import cats.effect._
import cats.syntax.all._
import fs2.grpc.syntax.all._
import io.grpc.{Metadata, Server}
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
import org.log4s.getLogger
import redsort.jobs.Common
import redsort.jobs.Common.NetAddr
import redsort.jobs.context.ReplicatorCtx
import redsort.jobs.context.impl.ReplicatorLocalServerImpl
import redsort.jobs.context.interface.ReplicatorLocalRpcServer
import redsort.jobs.messages.ReplicatorLocalServiceFs2Grpc

import java.net.InetAddress

object ReplicatorLocalServerFiber {
  private[this] val logger = getLogger

  def start(
    port: Int,
    fileService: FileReplicationService,
    ctx: ReplicatorLocalRpcServer
  ): Resource[IO, Server] = {
    IO(logger.debug(s"replicator RPC server fiber")).toResource.flatMap { _ =>
      val grpc = new ReplicatorLocalServerImpl(fileService)

      ctx.replicatorLocalRpcServer(grpc, port)
        .evalMap { server =>
          IO(logger.info(s"replicator local server started on port $port")) *>
            IO(server.start())
        }
    }
  }
}
