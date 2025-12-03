package redsort.jobs.replicator

import cats._
import cats.effect._
import cats.syntax.all._
import redsort.jobs.Common._
import redsort.AsyncSpec
import redsort.jobs.messages.ReplicatorRemoteServiceFs2Grpc
import io.grpc.Metadata
import redsort.jobs.context.interface.ReplicatorRemoteRpcClient
import redsort.jobs.messages.PullRequest
import redsort.jobs.messages.Packet
import com.google.protobuf.ByteString
import fs2.Stream
import redsort.jobs.context.impl.InMemoryFileStorage
import redsort.jobs.worker.Directories
import fs2.io.file.Path
import redsort.jobs.messages.PushRequest
import com.google.protobuf.empty.Empty

class ReplicatorLocalServiceSpec extends AsyncSpec {
  val fixture = new {
    val clientStub = stub[ReplicatorRemoteServiceFs2Grpc[IO, Metadata]]
    val clients = Map(1 -> clientStub)

    val replicatorAddrs = Map(
      0 -> new NetAddr("1.1.1.1", 5000),
      1 -> new NetAddr("2.2.2.2", 6000)
    )

    val dirs = new Directories(
      inputDirectories = Seq(Path("/input")),
      outputDirectory = Path("/output"),
      workingDirectory = Path("/working")
    )

    def withStorageAndService(
        testCode: (ReplicatorLocalService.ServiceType, InMemoryFileStorage) => IO[Unit]
    ) = {
      for {
        ref <- IO.ref[Map[String, Array[Byte]]](Map.empty)
        storage = new InMemoryFileStorage(ref)
        service = ReplicatorLocalService.init(replicatorAddrs, clients, storage, dirs)
        _ <- testCode(service, storage)
      } yield ()
    }
  }

  behavior of "ReplicatorLocalService.pull"

  it should "write stream from Read() method of remote replicator" in {
    val f = fixture

    // return some stream of `Packet`s when read() is called
    (f.clientStub.read _).returnsWith({
      val packets = Seq(
        new Packet(
          data = ByteString.copyFromUtf8("aaa")
        ),
        new Packet(
          data = ByteString.copyFromUtf8("bbb")
        )
      )

      Stream.emits(packets)
    })
    val request = new PullRequest(
      path = "@{working}/hello",
      src = 1
    )

    f.withStorageAndService { case (service, storage) =>
      for {
        result <- service.pull(request, new Metadata)
        data <- storage.readAll("/working/hello")
      } yield {
        (f.clientStub.read _).calls.get(0).get._1.path shouldBe "@{working}/hello"
        ByteString.copyFrom(data) shouldBe ByteString.copyFromUtf8("aaabbb")
        result.success shouldBe true
      }
    }
  }

  behavior of "ReplicatorLocalService.push"

  it should "call Write() method of remote replicator with stream of local file" in {
    val f = fixture
    val request = new PushRequest(
      path = "@{working}/hello",
      dst = 1
    )
    val data = Array.fill(5)(42.toByte)

    (f.clientStub.write _).returnsWith(IO(Empty()))

    f.withStorageAndService { case (service, storage) =>
      for {
        // prepare file
        _ <- storage.writeAll("/working/hello", data)

        result <- service.push(request, new Metadata)
        requests <- (f.clientStub.write _).calls.get(0).get._1.compile.toList
      } yield {
        requests(0).payload.path.get shouldBe "@{working}/hello"
        requests(1).payload.data.get shouldBe ByteString.copyFrom(data)
      }
    }
  }
}
