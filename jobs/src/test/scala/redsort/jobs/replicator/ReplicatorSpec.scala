package redsort.jobs.replicator

import cats._
import cats.effect._
import cats.syntax.all._
import redsort.jobs.context.interface.NetInfo
import redsort.jobs.context.ReplicatorCtx
import redsort.jobs.context.impl.ProductionReplicatorRemoteRpcClient
import redsort.jobs.context.impl.ProductionReplicatorRemoteRpcServer
import redsort.jobs.context.impl.ProductionReplicatorLocalRpcServer
import redsort.jobs.context.impl.InMemoryFileStorage
import redsort.AsyncFunSpec
import redsort.Logging.fileLogger
import redsort.jobs.Common._
import redsort.jobs.worker.Directories
import fs2.io.file.Path
import redsort.jobs.context.impl.ProductionReplicatorLocalRpcClient
import redsort.jobs.messages.PullRequest
import io.grpc.Metadata
import com.google.protobuf.ByteString
import redsort.NetworkTest
import scala.concurrent.duration._

trait FakeNetInfo extends NetInfo {
  override def getIP: IO[String] =
    IO.pure("127.0.0.1")
}

class ReplicatorTestCtx(ref: Ref[IO, Map[String, Array[Byte]]])
    extends InMemoryFileStorage(ref)
    with ReplicatorCtx
    with ProductionReplicatorRemoteRpcClient
    with ProductionReplicatorRemoteRpcServer
    with ProductionReplicatorLocalRpcServer

object RpcClient extends ProductionReplicatorLocalRpcClient

class ReplicatorSpec extends AsyncFunSpec {
  def fixture(portOffset: Int) = new {
    val replicatorAddrs = Map(
      0 -> new NetAddr("127.0.0.1", 3000 + portOffset),
      1 -> new NetAddr("127.0.0.1", 3050 + portOffset)
    )
    val replicatorAlocalPort = 3010 + portOffset
    val replicatorBlocalPort = 3060 + portOffset
    val clientAres =
      RpcClient.replicatorLocalRpcClient(new NetAddr("127.0.0.1", replicatorAlocalPort))

    def integrationTest(
        logName: String
    )(
        body: (ReplicatorTestCtx, ReplicatorTestCtx, ReplicatorLocalService.ServiceType) => IO[Unit]
    ): IO[Unit] =
      fileLogger(logName).use { _ =>
        clientAres.use { clientA =>
          // run replicator with body
          for {
            fsOne <- IO.ref[Map[String, Array[Byte]]](Map.empty)
            fsTwo <- IO.ref[Map[String, Array[Byte]]](Map.empty)
            ctxOne = new ReplicatorTestCtx(fsOne)
            ctxTwo = new ReplicatorTestCtx(fsTwo)
            dirs <- Directories.init(
              inputDirectories = Seq(Path("/input")),
              outputDirectory = Path("/output"),
              workingDirectory = Path("/working")
            )

            replicators = (
              Replicator.start(
                replicatorAddrs = replicatorAddrs,
                ctx = ctxOne,
                dirs = dirs,
                localPort = replicatorAlocalPort,
                remotePort = 3000 + portOffset
              ),
              Replicator.start(
                replicatorAddrs = replicatorAddrs,
                ctx = ctxTwo,
                dirs = dirs,
                localPort = replicatorBlocalPort,
                remotePort = 3050 + portOffset
              )
            ).parTupled

            _ <- replicators.race(body(ctxOne, ctxTwo, clientA))
          } yield ()
        }
      }
  }

  test("pull-1KB", NetworkTest) {
    val f = fixture(0)
    f.integrationTest("pull-1KB") { case (fsA, fsB, clientA) =>
      for {
        // create small file with 1KB of data on machine B
        contents <- IO.pure(Array.fill(1000)(42.toByte))
        _ <- fsB.writeAll("/working/hello", contents)

        // pull the file from machine B to machine A
        request = new PullRequest(path = "@{working}/hello", src = 1)
        _ <- clientA.pull(request, new Metadata)

        // read contents of replicated file
        replicatedContents <- fsA.readAll("/working/hello")
      } yield {
        ByteString.copyFrom(replicatedContents) shouldBe ByteString.copyFrom(contents)
      }
    }.timeout(5.seconds)
  }

  test("pull-parallel-10MB", NetworkTest) {
    val f = fixture(1)
    f.integrationTest("pull-10MB") { case (fsA, fsB, clientA) =>
      for {
        contents <- IO.pure(Array.fill(10 * 1000 * 1000)(42.toByte))
        _ <- fsB.writeAll("/working/hello1", contents)
        _ <- fsB.writeAll("/working/hello2", contents)

        // pull the file from machine B to machine A
        requestOne = new PullRequest(path = "@{working}/hello1", src = 1)
        requestTwo = new PullRequest(path = "@{working}/hello2", src = 1)
        _ <- (
          clientA.pull(requestOne, new Metadata),
          clientA.pull(requestTwo, new Metadata)
        ).parTupled

        // read contents of replicated file
        replicatedContentsOne <- fsA.readAll("/working/hello1")
        replicatedContentsTwo <- fsA.readAll("/working/hello2")
      } yield {
        ByteString.copyFrom(replicatedContentsOne) shouldBe ByteString.copyFrom(contents)
        ByteString.copyFrom(replicatedContentsTwo) shouldBe ByteString.copyFrom(contents)
      }
    }.timeout(5.seconds)
  }
}
