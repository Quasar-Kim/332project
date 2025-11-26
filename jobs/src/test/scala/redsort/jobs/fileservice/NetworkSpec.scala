package redsort.jobs.fileservice

import cats.effect.{IO, Resource}
import com.google.protobuf.ByteString
import com.google.protobuf.empty.Empty
import fs2.Stream
import io.grpc.Metadata
import org.scalamock.stubs.Stub
import redsort.AsyncSpec
import redsort.jobs.Common.NetAddr
import redsort.jobs.context.ReplicatorCtx
import redsort.jobs.messages.{Packet, ReplicatorRemoteServiceFs2Grpc, WriteRequest}

class NetworkSpec extends AsyncSpec {
  def fixture = new {
    val node1 = 1
    val addr1: NetAddr = NetAddr("1.1.1.1", 1111)

    // stub connection pool
    val pool: Stub[ConnectionPoolAlgebra[IO]] = stub[ConnectionPoolAlgebra[IO]]
    (() => pool.getRegistry).returnsWith(Map(node1 -> addr1))

    // stub replicator context
    val rpcClientStub: Stub[ReplicatorRemoteServiceFs2Grpc[IO, Metadata]] =
      stub[ReplicatorRemoteServiceFs2Grpc[IO, Metadata]]
    val ctx: Stub[ReplicatorCtx] = stub[ReplicatorCtx]
    stubbed(ctx.replicatorRemoteRpcClient _).returns { addr =>
      {
        if (addr == addr1)
          Resource.pure[IO, ReplicatorRemoteServiceFs2Grpc[IO, Metadata]](rpcClientStub)
        else Resource.eval(IO.raiseError(new RuntimeException(s"unexpected address -- $addr")))
      }
    }

    // borrow for pool should return the context
    stubbed(pool.borrow _).returns { mid =>
      {
        if (mid == node1) Resource.pure[IO, ReplicatorCtx](ctx)
        else Resource.eval(IO.raiseError(new RuntimeException(s"unexpected machine id -- $mid")))
      }
    }

    // for the files
    val content = "qwerty"
    val path = "file.txt"
    // chunks (ByteString) to make packets for read
    val contentChunks: Stream[IO, ByteString] = Stream
      .emits(
        List("q", "we", "rty").map { item =>
          ByteString.copyFromUtf8(item)
        }
      )
      .covary[IO]
    // stream of bytes for writeFile
    val contentBytes: Stream[IO, Byte] = Stream.emits(content.getBytes).covary[IO]

    // write for rpc client
    var receivedRequests: List[WriteRequest] = Nil
    stubbed(rpcClientStub.write _).returns { arg =>
      val stream = arg._1
      stream
        .evalMap { request =>
          IO { receivedRequests = receivedRequests :+ request }
        }
        .compile
        .drain *> IO.pure(Empty())
    }

    // read for rpc client
    val somePackets: Stream[IO, Packet] = contentChunks.map { item =>
      Packet(data = item)
    }
    stubbed(rpcClientStub.read _).returns { _ => somePackets }

    val network = new NetworkAlgebraImpl(pool)
  }

  behavior of "network.getClient"
  // fully reliant on "borrow" of connection pool

  it should "acquire the corresponding client using the conneciton pool" in {
    val f = fixture

    f.network
      .getClient(f.node1)
      .use { ctx =>
        IO.pure(ctx)
      }
      .unsafeRunSync() shouldBe f.ctx
  }

  behavior of "network.writeFile"

  it should "send correct write requests that piece together the original file content" in {
    val f = fixture

    // mimic writing file "file.txt" (containing "qwerty") to node1
    for {
      ctx <- f.network.getClient(f.node1).use(IO.pure)
      _ <- f.network.writeFile(ctx, f.path, f.node1, f.contentBytes)
    } yield {
      f.receivedRequests should not be empty
      f.receivedRequests.foreach(_.path shouldBe Some(f.path))
      new String(
        f.receivedRequests.flatMap(_.data.toByteArray).toArray
      ) shouldBe f.content
    }
  }

  // TODO error writing to existing path (?)

  behavior of "network.readFile"

  it should "convert packets into byte stream" in {
    val f = fixture

    // mimic reading file "file.txt" (containing "qwerty") from node1
    for {
      ctx <- f.network.getClient(f.node1).use(IO.pure)
      stream <- f.network.readFile(ctx, f.path, f.node1)
      bytes <- stream.compile.toList
    } yield {
      new String(bytes.toArray) shouldBe f.content
    }
  }

  // TODO error reading non-existent file (?)
}
