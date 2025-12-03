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
import redsort.jobs.RPChelper
import scala.concurrent.duration._

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

        // call read() RPC method of remote replicator service, retrying max. 3 times
        // when transport error happenes
        val client = clients(pullRequest.src)
        tryRead(client, readRequest, retriesLeft = 2) { stream =>
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

        tryWrite(client, stream, retriesLeft = 2).map(_ => ReplicationResult(success = true))
      }
    }

  /** Call `read()` method of remote replicator RPC service with retries upon transport error.
    */
  def tryRead[T](rpcClient: ClientType, request: ReadRequest, retriesLeft: Int)(
      callback: Stream[IO, Byte] => IO[T]
  ): IO[T] = {
    val stream = rpcClient
      .read(request, new Metadata)
      .map(msg =>
        Chunk.byteBuffer(msg.data.asReadOnlyByteBuffer())
      ) // convert `Packet` message into chunk
      .unchunks
    callback(stream).handleErrorWith {
      case err if RPChelper.isTransportError(err) =>
        for {
          _ <- logger.error(s"replicator transport error (retry count: $retriesLeft): $err")
          result <-
            if (retriesLeft > 0)
              IO.sleep(1.second) >> tryRead(rpcClient, request, retriesLeft - 1)(callback)
            else IO.raiseError[T](err)
        } yield result
      case err =>
        logger.error(s"replicator read() failed with fatal exception: $err") >> IO.raiseError[T](
          err
        )
    }
  }

  def tryWrite(
      rpcClient: ClientType,
      stream: Stream[IO, WriteRequest],
      retriesLeft: Int
  ): IO[Unit] = {
    rpcClient
      .write(stream, new Metadata)
      .handleErrorWith { err =>
        if (retriesLeft > 0 && RPChelper.isTransportError(err)) {
          RPChelper.handleRpcErrorWithRetry(tryWrite(rpcClient, stream, retriesLeft - 1))(err)
        } else {
          logger.error(s"replicator write() failed with fatal exception: $err") >>
            IO.raiseError(err)
        }
      }
      .flatMap(_ => IO.unit)
  }
}
