package redsort.jobs.replicator

import cats._
import cats.effect._
import cats.syntax.all._
import redsort.jobs.Common._
import redsort.AsyncSpec
import redsort.jobs.context.impl.InMemoryFileStorage
import redsort.jobs.worker.Directories
import fs2.io.file.Path
import redsort.jobs.messages.ReadRequest
import io.grpc.Metadata
import fs2.{Chunk, Stream}
import com.google.protobuf.ByteString
import redsort.jobs.messages.WriteRequest

class ReplicatorRemoteServiceSpec extends AsyncSpec {
  val fixture = new {
    val dirs = new Directories(
      inputDirectories = Seq(Path("/input")),
      outputDirectory = Path("/output"),
      workingDirectory = Path("/working")
    )

    def withStorageAndService(
        testCode: (ReplicatorRemoteService.ServiceType, InMemoryFileStorage) => IO[Unit]
    ) = {
      for {
        ref <- IO.ref[Map[String, Array[Byte]]](Map.empty)
        storage = new InMemoryFileStorage(ref)
        service = ReplicatorRemoteService.init(storage, dirs)
        _ <- testCode(service, storage)
      } yield ()
    }
  }

  behavior of "ReplicatorRemoteService.read"

  it should "send contents of requested file as stream" in {
    val f = fixture
    val request = new ReadRequest(
      path = "@{working}/hello"
    )

    f.withStorageAndService { case (service, storage) =>
      for {
        // prepare file
        contents <- IO.pure("hello".getBytes)
        _ <- storage.writeAll("/working/hello", contents)

        // gather output stream of read as byte string
        output <- service
          .read(request, new Metadata)
          .map(msg => Chunk.byteBuffer(msg.data.asReadOnlyByteBuffer()))
          .unchunks
          .compile
          .to(Array)
      } yield {
        output shouldBe contents
      }
    }
  }

  behavior of "ReplicatorRemoteService.write"

  it should "write contents of stream into file" in {
    val f = fixture
    val contents = Array.fill(10 * 1000)(42.toByte)
    val dataStream = Stream
      .chunk(Chunk.array(contents))
      .chunkN(1000)
      .map(c => WriteRequest(WriteRequest.Payload.Data(ByteString.copyFrom(c.toArray))))
    val stream = (
      Stream.emit(WriteRequest(WriteRequest.Payload.Path("/working/hello")))
        ++ dataStream
    ).covary[IO]

    f.withStorageAndService { case (service, storage) =>
      for {
        // request service to write `contents` into file `/working/hello`
        _ <- service.write(stream, new Metadata)

        actualContents <- storage.readAll("/working/hello")
      } yield {
        actualContents shouldBe contents
      }
    }
  }
}
