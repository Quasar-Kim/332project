package redsort.jobs.context.impl

import cats.effect.IO
import com.google.protobuf.ByteString
import com.google.protobuf.empty.Empty
import io.grpc.Metadata
import fs2.Stream
import redsort.jobs.context.interface.FileStorage
import redsort.jobs.messages.{Packet, ReadRequest, ReplicatorRemoteServiceFs2Grpc, WriteRequest}

class ReplicatorRemoteServerImpl(fileStorage: FileStorage)
    extends ReplicatorRemoteServiceFs2Grpc[IO, Metadata] {
  override def read(request: ReadRequest, ctx: Metadata): fs2.Stream[IO, Packet] = {
    val path = request.path

    fileStorage.read(path).chunks.map(bytes => Packet(ByteString.copyFrom(bytes.toArray)))
  }

  override def write(request: fs2.Stream[IO, WriteRequest], ctx: Metadata): IO[Empty] = {
    // TODO : fix how file is written (whole file is resource-heavy)
    // TODO : multiple different paths error?
    // TODO : file already exists?
    val fromAllRequests: Stream[IO, Either[(String, Array[Byte]), Array[Byte]]] =
      request.evalMap { writeRequest =>
        // array of bytes from the request
        val bytes = writeRequest.data.toByteArray

        writeRequest.path match {
          case Some(path) => IO.pure(Left((path, bytes))) // both the path and the bytes
          case None => IO.pure(Right(bytes)) // only the bytes
        }
    }

    // consume the resulting stream
    fromAllRequests
      .compile
      .toList
      .flatMap { items =>
        // try to extract the path
        val maybePath = items.collectFirst { case Left((path, _)) => path }

        maybePath match {
          case None =>
            IO.raiseError(new RuntimeException("no path in the stream of write requests")) // TODO : specify error?
          case Some(path) => {
            // concatenate the data (should preserve order)
            val data: Array[Byte] = items.flatMap {
              case Left((_, bytes)) => bytes
              case Right(bytes) => bytes
            }.toArray

            // write the file
            fileStorage.writeAll(path, data)
          }
        }
      }
      .as(Empty())
  }
}
