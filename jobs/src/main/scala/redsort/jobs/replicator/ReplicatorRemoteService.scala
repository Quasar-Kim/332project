package redsort.jobs.replicator

import cats._
import cats.effect._
import cats.syntax.all._
import org.log4s._
import redsort.jobs.Common._
import redsort.jobs.SourceLogger
import java.nio.file.FileStore
import redsort.jobs.context.interface.FileStorage
import redsort.jobs.worker.Directories
import redsort.jobs.messages.ReplicatorRemoteServiceFs2Grpc
import io.grpc.Metadata
import redsort.jobs.messages.{Packet, ReadRequest}
import com.google.protobuf.empty.Empty
import redsort.jobs.messages.WriteRequest
import fs2.io.file.Path
import com.google.protobuf.ByteString
import fs2._

object ReplicatorRemoteService {
  type ServiceType = ReplicatorRemoteServiceFs2Grpc[IO, Metadata]

  private[this] val logger = new SourceLogger(getLogger, "replicator-remote")
  private val CHUNK_SIZE = 1 * 1000 * 1000 // 1MB

  def init(
      ctx: FileStorage,
      dirs: Directories
  ): ServiceType =
    new ServiceType {
      override def read(request: ReadRequest, _ctx: Metadata): fs2.Stream[IO, Packet] = {
        val localPath = Directories.resolvePath(dirs, Path(request.path)).toString
        ctx
          .read(localPath)
          .chunkN(CHUNK_SIZE)
          .map(chunk => new Packet(ByteString.copyFrom(chunk.toByteBuffer)))
      }

      override def write(request: fs2.Stream[IO, WriteRequest], _ctx: Metadata): IO[Empty] = {
        request.pull.uncons1
          .flatMap {
            case None => Pull.raiseError[IO](new IllegalArgumentException("Empty stream"))

            case Some((metadata, dataStream)) =>
              metadata.payload.path match {
                case None => Pull.raiseError[IO](new IllegalArgumentException("Missing path"))

                case Some(path) => {
                  val writeStream =
                    dataStream.flatMap(msg =>
                      msg.payload.data match {
                        case Some(data) => Stream.chunk(Chunk.byteBuffer(data.asReadOnlyByteBuffer))
                        case None       =>
                          Stream.raiseError[IO](new IllegalArgumentException("Missing data"))
                      }
                    )
                  val localPath = Directories.resolvePath(dirs, Path(path))
                  Pull.eval(ctx.save(localPath.toString, writeStream)) >> Pull.output1(new Empty)
                }
              }
          }
          .stream
          .compile
          .lastOrError
      }
    }
}
