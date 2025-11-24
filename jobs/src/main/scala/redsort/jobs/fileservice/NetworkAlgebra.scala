package redsort.jobs.fileservice

import cats.effect.{Async, Resource}
import fs2.{Chunk, Stream}
import com.google.protobuf.ByteString
import io.grpc.Metadata
import redsort.jobs.Common.{Mid, NetAddr}
import redsort.jobs.context.ReplicatorCtx
import redsort.jobs.fileservice.ConnectionPoolAlgebra
import redsort.jobs.messages.{PushRequest, ReadRequest, WriteRequest}

trait NetworkAlgebra[F[_]] {
  def getClient(mid: Mid): Resource[F, ReplicatorCtx]
  def writeFile(ctx: ReplicatorCtx, path: String, dst: Mid, data: Stream[F, Byte]): F[Unit]
  def readFile(ctx: ReplicatorCtx, path: String, src: Mid): F[Stream[F, Byte]]
}

class NetworkAlgebraImpl[F[_]: Async](connectionPool: ConnectionPoolAlgebra[F])
    extends NetworkAlgebra[F] {
  def getClient(mid: Mid): Resource[F, ReplicatorCtx] = connectionPool.borrow(mid)

  def writeFile(ctx: ReplicatorCtx, path: String, dst: Mid, data: Stream[F, Byte]): F[Unit] = {
//    connectionPool.getRegistry.get(dst) match {
//      case Some(addr) => {
//        val writeRequests = data.chunks.map { chunk =>
//          WriteRequest(path = path, dst = dst, data = ByteString.copyFrom(chunk.toArray))
//        }
//        ctx.replicatorRemoteRpcClient(addr).use { grpc => grpc.write(writeRequests, new Metadata()) }
//      }
//    }
    ???
  }

  def readFile(ctx: ReplicatorCtx, path: String, src: Mid): F[Stream[F, Byte]] = {
    //    connectionPool.getRegistry.get(src) match {
    //      case Some(addr) => {
    //        val readRequest = ReadRequest(path = path, src = src)
    //        ctx.replicatorRemoteRpcClient(addr).use { grpc => grpc.read(readRequest, new Metadata()) }
    //      }
    //    }
    //  }
    ???
  }
}
