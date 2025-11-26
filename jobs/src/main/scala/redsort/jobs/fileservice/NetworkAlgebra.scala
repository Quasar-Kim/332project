package redsort.jobs.fileservice

import cats.effect.{IO, Resource}
import com.google.protobuf.ByteString
import fs2.{Chunk, Stream}
import io.grpc.Metadata
import redsort.jobs.Common.Mid
import redsort.jobs.context.ReplicatorCtx
import redsort.jobs.messages.{Packet, ReadRequest, WriteRequest}

trait NetworkAlgebra[F[_]] {
  def getClient(mid: Mid): Resource[F, ReplicatorCtx]
  def writeFile(ctx: ReplicatorCtx, path: String, dst: Mid, data: Stream[F, Byte]): F[Unit]
  def readFile(ctx: ReplicatorCtx, path: String, src: Mid): F[Stream[F, Byte]]
}

class NetworkAlgebraImpl(connectionPool: ConnectionPoolAlgebra[IO]) extends NetworkAlgebra[IO] {
  def getClient(mid: Mid): Resource[IO, ReplicatorCtx] = connectionPool.borrow(mid)

  def writeFile(ctx: ReplicatorCtx, path: String, dst: Mid, data: Stream[IO, Byte]): IO[Unit] = {
    connectionPool.getRegistry.get(dst) match {
      case Some(addr) =>
        val writeRequests = data.chunks.map { chunk =>
          WriteRequest(path = path, data = ByteString.copyFrom(chunk.toArray))
        }
        ctx.replicatorRemoteRpcClient(addr).use { grpc =>
          grpc.write(writeRequests, new Metadata()).void // void for IO[Empty] -> IO[Unit]
        }
      case None =>
        IO.raiseError(
          new NoSuchElementException((s"Destination machine ID $dst not in registry"))
        )
    }
  }

  def readFile(ctx: ReplicatorCtx, path: String, src: Mid): IO[Stream[IO, Byte]] = {
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

        val readRequest = ReadRequest(path = path)
        ctx.replicatorRemoteRpcClient(addr).use { grpc =>
          val packets: Stream[IO, Packet] = grpc.read(readRequest, new Metadata())
          val bytes: Stream[IO, Byte] = packets.flatMap { packet =>
            Stream.chunk(Chunk.array(packet.data.toByteArray))
          }
          IO.pure(bytes)
        }
      case None =>
        IO.raiseError(new NoSuchElementException((s"Source machine ID $src not in registry")))
    }
  }
}
