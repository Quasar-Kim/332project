package redsort.jobs.fileservice

import cats.effect.IO
import com.google.protobuf.ByteString
import com.google.protobuf.empty.Empty
import fs2.Stream
import io.grpc.Metadata
import org.scalamock.stubs.Stub
import redsort.AsyncSpec
import redsort.jobs.Common.FileEntry
import redsort.jobs.context.impl.{ReplicatorLocalServerImpl, ReplicatorRemoteServerImpl}
import redsort.jobs.context.interface.FileStorage
import redsort.jobs.messages.{Packet, PullRequest, ReadRequest, WriteRequest}

class ReplicatorRemoteServerSpec extends AsyncSpec {
  def fixture = new {
    val path = "file.txt"
    val content = "qwerty"
    val chunks: Seq[String] = List("q", "we", "rty")
    val contentBytes: Array[Byte] = content.getBytes

    val metadata = new Metadata()

    val packets: Stream[IO, Packet] = Stream.emits(
      chunks.map { item =>
        Packet(ByteString.copyFromUtf8(item))
      }
    ).covary[IO]

    val readRequest: ReadRequest = ReadRequest(path)
    val writeRequests: Stream[IO, WriteRequest] = Stream.emits(
      chunks.map { item =>
        if (chunks.indexOf(item) == 1) WriteRequest(path = Some(path), data = ByteString.copyFromUtf8(item))
        else WriteRequest(path = None, data = ByteString.copyFromUtf8(item))
      }
    ).covary[IO]

    // stub file storage
    val fileStorage: Stub[FileStorage] = stub[FileStorage]

    val server = new ReplicatorRemoteServerImpl(fileStorage)
  }

  behavior of "server.read"

  it should "convert contents of the specified file to packets" in {
    val f = fixture

    stubbed(f.fileStorage.read _).returns { path =>
      if (path == f.path) Stream.emits(f.contentBytes).covary[IO]
      else Stream.empty.covary[IO]
    }

    val resPackets = f.server.read(f.readRequest, f.metadata)

    // extract data from packets
    val resBytes: Array[Byte] =
      resPackets
        .map(packet => packet.data.toByteArray)
        .compile
        .toList
        .unsafeRunSync()
        .flatten
        .toArray

    new String(resBytes) shouldBe f.content
  }

  behavior of "server.write"

  it should "write received data to the specified path, preserving the order" in {
    val f = fixture

    var writtenPath = ""
    var writtenBytes = Array.emptyByteArray
    stubbed(f.fileStorage.writeAll _).returns { arg =>
      val path = arg._1
      val data = arg._2
      if (path == f.path) {
        writtenPath = path
        writtenBytes = data
      }
      IO.unit
    }

    val res = f.server.write(f.writeRequests, f.metadata).unsafeRunSync()
    val resContent = new String(writtenBytes)

    println(f.content + " : " + resContent)

    res shouldBe Empty()
    writtenPath shouldBe f.path
    resContent shouldBe f.content
  }

  it should "fail if no path was specified" in {
    val f = fixture

    val pathLessRequests = Stream.emits(
      List(
        WriteRequest(path = None, data = ByteString.copyFromUtf8("qwerty")),
        WriteRequest(path = None, data = ByteString.copyFromUtf8("asdfgh"))
      )
    ).covary[IO]

    val thrown = intercept[RuntimeException] {
      f.server.write(pathLessRequests, f.metadata).unsafeRunSync()
    }

    thrown.getMessage shouldBe "no path in the stream of write requests"
  }
}
