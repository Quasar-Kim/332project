package redsort.jobs.fileservice

import cats.effect.{Async, Resource}
import com.google.protobuf.ByteString
import fs2.Stream
import io.grpc.Metadata
import redsort.jobs.Common.Mid
import redsort.jobs.context.ReplicatorCtx
import redsort.jobs.messages.{Packet, ReadRequest, WriteRequest}

trait NetworkAlgebra[F[_]] {
  def getClient(mid: Mid): Resource[F, ReplicatorCtx]
  def writeFile(ctx: ReplicatorCtx, path: String, dst: Mid, data: Stream[F, Byte]): F[Unit]
  def readFile(ctx: ReplicatorCtx, path: String, src: Mid): F[Stream[F, Byte]]
}

class NetworkAlgebraImpl[F[_]: Async](connectionPool: ConnectionPoolAlgebra[F])
    extends NetworkAlgebra[F] {
  def getClient(mid: Mid): Resource[F, ReplicatorCtx] = connectionPool.borrow(mid)

  def writeFile(ctx: ReplicatorCtx, path: String, dst: Mid, data: Stream[F, Byte]): F[Unit] = {
    connectionPool.getRegistry.get(dst) match {
      case Some(addr) =>
        val writeRequests = data.chunks.map { chunk =>
          WriteRequest(path = path, dst = dst, data = ByteString.copyFrom(chunk.toArray))
        }
        ctx
          .replicatorRemoteRpcClient(addr)
          .use { grpc =>
            grpc.write(writeRequests, new Metadata()) // FIXME type mismatch (IO/F)
          }()
      case None =>
        Async[F].raiseError(
          new NoSuchElementException((s"Destination machine ID $dst not in registry"))
        )
    }
  }

  def readFile(ctx: ReplicatorCtx, path: String, src: Mid): F[Stream[F, Byte]] = {
    connectionPool.getRegistry.get(src) match {
      case Some(addr) =>
//        val readRequest = ReadRequest(path = path, src = src)
//        ctx.replicatorRemoteRpcClient(addr).use { grpc =>
//          grpc.read(readRequest, new Metadata())
//        }()

//        val readRequest = ReadRequest(path = path, src = src)
//        Async[F].pure { // read returns stream of Packet
//          Stream.resource(ctx.replicatorRemoteRpcClient(addr)).flatMap {
//            grpc => grpc.read(readRequest, new Metadata()).flatMap {
//              packet => Stream.chunk(Chunk.array(packet.data.toByteArray))
//            }
//          }
//        }

        val readRequest = ReadRequest(path = path, src = src)
        ctx
          .replicatorRemoteRpcClient(addr)
          .use { grpc =>
            val packets: Stream[F, Packet] =
              grpc.read(readRequest, new Metadata()) // FIXME type mismatch (IO/F)
            packets.flatMap { packet => Stream.eval(packet.data) }
          }()
      case None =>
        Async[F].raiseError(new NoSuchElementException((s"Source machine ID $src not in registry")))
    }
  }
}
