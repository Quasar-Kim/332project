package redsort.jobs.context.impl

import cats.effect.IO
import com.google.protobuf.ByteString
import com.google.protobuf.empty.Empty
import io.grpc.Metadata
import redsort.jobs.context.interface.FileStorage
import redsort.jobs.messages.{Packet, ReadRequest, ReplicatorRemoteServiceFs2Grpc, WriteRequest}

class ReplicatorRemoteServerImpl(fileStorage: FileStorage)
    extends ReplicatorRemoteServiceFs2Grpc[IO, Metadata] {
  override def read(request: ReadRequest, ctx: Metadata): fs2.Stream[IO, Packet] = {
    val path = request.path

    fileStorage.read(path).chunks.map(bytes => Packet(ByteString.copyFrom(bytes.toArray)))
  }

  override def write(request: fs2.Stream[IO, WriteRequest], ctx: Metadata): IO[Empty] = {
    var path = ""
    request
      .evalMap { writeRequest =>
        path = writeRequest.path match {
          case Some(p) => p
          case _ => ""
      }
        IO.pure(writeRequest.data.toByteArray)
      }
      .compile
      .toList
      .map { bytesList =>
        bytesList.flatten.toArray
      }
      .flatMap { fullData =>
        fileStorage.writeAll(path, fullData)
      }
      .as(Empty())
  }
}
