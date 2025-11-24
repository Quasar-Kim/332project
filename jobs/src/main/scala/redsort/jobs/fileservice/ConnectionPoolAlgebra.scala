package redsort.jobs.fileservice

import cats.effect._
import cats.implicits._
import redsort.jobs.Common.{Mid, NetAddr}
import redsort.jobs.context.ReplicatorCtx
import redsort.jobs.context.impl.ProductionReplicatorRemoteRpcClient

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

class ConnectionPoolAlgebraImpl[F[_]: Async](
    registry: Map[Mid, NetAddr],
    config: ConnectionPoolConfig = ConnectionPoolConfig()
) extends ConnectionPoolAlgebra[F] {
  private val pools: Ref[F, Map[Mid, Queue[ReplicatorCtx]]] = Ref.unsafe(Map.empty)

  def getRegistry: Map[Mid, NetAddr] = registry

  def borrow(mid: Mid): Resource[F, ReplicatorCtx] =
    Resource.make(acquireClient(mid))(ctx => releaseClient(mid, ctx))

  private def acquireClient(mid: Mid): F[ReplicatorCtx] = {
    pools.modify { poolsMap =>
      poolsMap.get(mid) match {
        case Some(queue) => {
          if (queue.nonEmpty) {
            val (client, rem) = queue.dequeue
            (poolsMap + (mid -> rem), Async[F].pure(client))
          } else (poolsMap, createNewClient(mid))
        }
        case None => (poolsMap + (mid -> Queue.empty), createNewClient(mid))
      }
    }.flatten
  }

  private def createNewClient(mid: Mid): F[ReplicatorCtx] = {
    registry.get(mid) match {
      case Some(addr) => createReplicatorCtx(addr)
      case None       => {
        Async[F].raiseError(
          new NoSuchElementException(
            s"Cannot find the NetAddr of machine with ID $mid -- key not in registry. Available machine IDs: ${registry.keys.mkString("; ")}"
          )
        )
      }
    }
  }
  private def createReplicatorCtx(addr: NetAddr): F[ReplicatorCtx] = ???

  private def releaseClient(mid: Mid, ctx: ReplicatorCtx): F[Unit] = {
    pools.update { poolsMap =>
      val queue = poolsMap.getOrElse(mid, Queue.empty)
      if (queue.size < config.connectionsPerMachine) poolsMap + (mid -> queue.enqueue(ctx))
      else {
        closeCtx(ctx)
        poolsMap
      }
    }
  }

  def invalidateAll(mid: Mid): F[Unit] = {
    pools.modify { poolsMap =>
      poolsMap.get(mid) match {
        case Some(queue) => {
          queue.foreach(ctx => closeCtx(ctx))
          (poolsMap - mid, ())
        }
        case None => (poolsMap, ())
      }
    }
  }

  private def closeCtx(ctx: ReplicatorCtx): F[Unit] = ???
}
