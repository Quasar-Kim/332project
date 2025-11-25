package redsort.jobs.fileservice

import cats.effect._
import fs2.Pipe
import io.grpc.{Metadata, Server}
import redsort.jobs.Common
import redsort.jobs.Common.{Mid, NetAddr}
import redsort.jobs.context.ReplicatorCtx
import redsort.jobs.context.interface.FileStorage
import redsort.jobs.messages.{ReplicatorLocalServiceFs2Grpc, ReplicatorRemoteServiceFs2Grpc}

import scala.collection.immutable.Queue
import scala.concurrent.duration._

case class ConnectionPoolConfig(
    connectionsPerMachine: Int = 4,
    connectionTimeout: FiniteDuration = 10.seconds
)

trait ConnectionPoolAlgebra[F[_]] {
  def borrow(mid: Mid): Resource[F, ReplicatorCtx]
  def invalidateAll(mid: Mid): F[Unit]
  def getRegistry: Map[Mid, NetAddr]
}

class ConnectionPoolAlgebraImpl(
    registry: Map[Mid, NetAddr],
    config: ConnectionPoolConfig = ConnectionPoolConfig(),
    grpcClientFactory: NetAddr => Resource[
      IO,
      ReplicatorRemoteServiceFs2Grpc[IO, Metadata]
    ], // TODO : simplify?
    fileStorageFactory: NetAddr => Resource[IO, FileStorage] // TODO : simplify?
) extends ConnectionPoolAlgebra[IO] {
  private val pools: Ref[IO, Map[Mid, Queue[ReplicatorCtx]]] = Ref.unsafe(Map.empty)

  def getRegistry: Map[Mid, NetAddr] = registry

  def borrow(mid: Mid): Resource[IO, ReplicatorCtx] =
    Resource.make(acquireClient(mid))(ctx => releaseClient(mid, ctx))

  private def acquireClient(mid: Mid): IO[ReplicatorCtx] = {
    pools.modify { poolsMap =>
      poolsMap.get(mid) match {
        case Some(queue) =>
          if (queue.nonEmpty) {
            val (client, rem) = queue.dequeue
            (poolsMap + (mid -> rem), IO.pure(client))
          } else (poolsMap, createNewClient(mid))
        case None => (poolsMap + (mid -> Queue.empty), createNewClient(mid))
      }
    }.flatten
  }

  private def createNewClient(mid: Mid): IO[ReplicatorCtx] = {
    registry.get(mid) match {
      case Some(addr) => createReplicatorCtx(addr).allocated.map(_._1)
      case None       =>
        Async[IO].raiseError(
          new NoSuchElementException(
            s"Cannot find the NetAddr of machine with ID $mid -- key not in registry. Available machine IDs: ${registry.keys.mkString("; ")}"
          )
        )
    }
  }
  private def createReplicatorCtx(addr: NetAddr): Resource[IO, ReplicatorCtx] = {
    fileStorageFactory(addr).map { fileStorage =>
      new ReplicatorCtx {
        override def replicatorRemoteRpcClient(
            someAddr: NetAddr
        ): Resource[IO, ReplicatorRemoteServiceFs2Grpc[IO, Metadata]] =
          grpcClientFactory(someAddr)

        override def read(path: String): fs2.Stream[IO, Byte] = fileStorage.read(path)
        override def write(path: String): Pipe[IO, Byte, Unit] = fileStorage.write(path)
        override def rename(before: String, after: String): IO[Unit] =
          fileStorage.rename(before, after)
        override def delete(path: String): IO[Unit] = fileStorage.delete(path)
        override def exists(path: String): IO[Boolean] = fileStorage.exists(path)
        override def list(path: String): IO[Map[String, Common.FileEntry]] = fileStorage.list(path)
        override def fileSize(path: String): IO[Long] = fileStorage.fileSize(path)
        override def mkDir(path: String): IO[String] = fileStorage.mkDir(path)

        override def replicatorRemoteRpcServer(
            grpc: ReplicatorRemoteServiceFs2Grpc[IO, Metadata],
            someAddr: NetAddr
        ): Resource[IO, Server] = Resource.eval(
          Async[IO].raiseError(
            new UnsupportedOperationException(
              "Trying to invoke a Remote Server method on a Client context"
            )
          )
        )
        override def replicatorLocalRpcServer(
            grpc: ReplicatorLocalServiceFs2Grpc[IO, Metadata],
            someAddr: NetAddr
        ): Resource[IO, Server] = Resource.eval(
          Async[IO].raiseError(
            new UnsupportedOperationException(
              "Trying to invoke a Local Server method on a Client context"
            )
          )
        )
      }
    }
  }

  private def releaseClient(mid: Mid, ctx: ReplicatorCtx): IO[Unit] = {
    pools.update { poolsMap =>
      val queue = poolsMap.getOrElse(mid, Queue.empty)
      if (queue.size < config.connectionsPerMachine) poolsMap + (mid -> queue.enqueue(ctx))
      else {
        closeCtx(ctx)
        poolsMap
      }
    }
  }

  def invalidateAll(mid: Mid): IO[Unit] = {
    pools.modify { poolsMap =>
      poolsMap.get(mid) match {
        case Some(queue) =>
          queue.foreach(ctx => closeCtx(ctx))
          (poolsMap - mid, ())
        case None => (poolsMap, ())
      }
    }
  }

  private def closeCtx(ctx: ReplicatorCtx): IO[Unit] = ???
}
