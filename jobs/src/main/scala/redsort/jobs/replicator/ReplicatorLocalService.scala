package redsort.jobs.replicator

import cats._
import cats.effect._
import cats.syntax.all._
import org.log4s._

import redsort.jobs.SourceLogger
import redsort.jobs.messages.ReplicatorLocalServiceFs2Grpc
import io.grpc.Metadata
import redsort.jobs.messages.{PullRequest, ReplicationResult}
import redsort.jobs.messages.PushRequest
import redsort.jobs.Common._
import redsort.jobs.context.interface.ReplicatorLocalRpcServer
import io.grpc.Server
import redsort.jobs.messages.ReplicatorRemoteServiceFs2Grpc
import redsort.jobs.context.interface.FileStorage
import redsort.jobs.worker.Directories
import redsort.jobs.messages.ReadRequest
import fs2._
import fs2.io.file.Path
import redsort.jobs.messages.WriteRequest
import com.google.protobuf.ByteString

object ReplicatorLocalService {
  type ServiceType = ReplicatorLocalServiceFs2Grpc[IO, Metadata]
  type ClientType = ReplicatorRemoteServiceFs2Grpc[IO, Metadata]

  private[this] val logger = new SourceLogger(getLogger, "replicator-local")
  private val CHUNK_SIZE = 1 * 1000 * 1000 // 1MB

  def init(
      replicatorAddrs: Map[Mid, NetAddr],
      clients: Map[Mid, ClientType],
      ctx: FileStorage,
      dirs: Directories
  ): ServiceType =
    new ServiceType {
      override def pull(pullRequest: PullRequest, _ctx: Metadata): IO[ReplicationResult] = {
        val readRequest = new ReadRequest(
          path = pullRequest.path
        )

        // create stream
        val client = clients(pullRequest.src)
        val source = client.read(readRequest, new Metadata)
        val stream = source
          .map(msg =>
            Chunk.byteBuffer(msg.data.asReadOnlyByteBuffer())
          ) // convert `Packet` message into chunk
          .unchunks

        // save stream into local path
        val localPath = Directories.resolvePath(dirs, Path(readRequest.path))
        for {
          _ <- logger.debug(s"pulling file ${pullRequest.path} from machine ${pullRequest.src}")
          _ <- ctx.save(localPath.toString, stream)
          _ <- logger.debug(s"pulled ${pullRequest.path}")
        } yield new ReplicationResult(
          success = true,
          error = None,
          stats = None
        )
      }

      override def push(request: PushRequest, _ctx: Metadata): IO[ReplicationResult] = {
        // build head (metadata) of stream
        val metadata = new WriteRequest(WriteRequest.Payload.Path(request.path))
        val streamHead = Stream.emit(metadata).covary[IO]

        // build tail (data) of stream
        val localPath = Directories.resolvePath(dirs, Path(request.path))
        val streamTail = ctx
          .read(localPath.toString)
          .chunkN(CHUNK_SIZE)
          .map(c => WriteRequest(WriteRequest.Payload.Data(ByteString.copyFrom(c.toByteBuffer))))

        // call remote client's write() method
        val client = clients(request.dst)
        val stream = streamHead ++ streamTail
        client.write(stream, new Metadata).map(_ => ReplicationResult(success = true))
      }
    }
}
