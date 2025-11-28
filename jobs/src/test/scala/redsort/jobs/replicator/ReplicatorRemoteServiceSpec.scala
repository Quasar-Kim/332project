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
import fs2.Chunk
import com.google.protobuf.ByteString

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
}
