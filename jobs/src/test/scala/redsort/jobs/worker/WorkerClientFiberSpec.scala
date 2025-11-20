package redsort.jobs.worker

import redsort.FlatSpecBase
import redsort.jobs.Common._
import cats.effect._
import cats.syntax.all._
import fs2.Stream
import io.grpc.{Metadata, Server}
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
import fs2.grpc.syntax.all._
import redsort.jobs.messages._
import redsort.jobs.worker.filestorage.{AppContext, FileStorage}
import org.scalamock.stubs.Stubs
import java.net.InetAddress
import scala.concurrent.duration._
import org.scalamock.scalatest.MockFactory

import cats.effect.std.Dispatcher
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalamock.scalatest.MockFactory

import cats.effect.unsafe.implicits.global


// TODO(jaehwan): I really cannot understand how to make this mockup testcase.
// This testcase always fails (not started)
class WorkerClientFiberSpec extends AnyFlatSpec with Matchers with MockFactory {

  class Fixture {
    val masterIp = "127.0.0.1"
    val wtid = 0
    val inputDirs = Seq("/tmp/input/dir1")
    val outputDir = "/tmp/output/dir1"

    val fileStorage = stub[FileStorage[AppContext]]
    val capturedReq = IO.deferred[WorkerHello].unsafeRunSync()

    val fakeSchedulerService = new SchedulerFs2Grpc[IO, Metadata] {
      override def registerWorker(
          request: WorkerHello,
          ctx: Metadata
      ): IO[SchedulerHello] = {
        for {
          _ <- capturedReq.complete(request)
        } yield SchedulerHello(mid = 1)
      }
      override def notifyUp(
          request: com.google.protobuf.empty.Empty,
          ctx: Metadata
      ): IO[com.google.protobuf.empty.Empty] =
        IO.pure(com.google.protobuf.empty.Empty())

      override def haltOnError(
          request: HaltRequest,
          ctx: Metadata
      ): IO[com.google.protobuf.empty.Empty] =
        IO.unit.as(com.google.protobuf.empty.Empty())
    }
    def serverResource: Resource[IO, Server] =
      SchedulerFs2Grpc
        .bindServiceResource[IO](fakeSchedulerService)
        .flatMap { serviceDef =>
          NettyServerBuilder
            .forPort(0)
            .addService(serviceDef)
            .resource[IO]
        }
  }
  def fixture = new Fixture

  "WorkerClientFiber" should "register to master correctly" in {
    val f = fixture
    (f.fileStorage.list _).when(*).returns(Stream.empty)
    val test = f.serverResource.use { masterServer =>
      val masterPort = masterServer.getPort
      WorkerClientFiber
        .start(
          f.wtid,
          f.masterIp,
          masterPort,
          f.inputDirs,
          f.outputDir,
          f.fileStorage
        )
        .background
        .use { _ =>
          f.capturedReq.get.timeout(10.seconds)
        }
    }

    val result = test.unsafeRunSync()
    println(result)
    result.wtid shouldBe f.wtid
  }

  it should "register to master correctly again" in {
    val f = fixture
    (f.fileStorage.list _).when("/tmp/input/dir1").returns(Stream.emit("/tmp/input/dir1/file"))
    (f.fileStorage.fileSize _).when("/tmp/input/dir1/file").returns(IO.pure(100L))
    val test = f.serverResource.use { masterServer =>
      val masterPort = masterServer.getPort
      WorkerClientFiber
        .start(
          f.wtid,
          f.masterIp,
          masterPort,
          f.inputDirs,
          f.outputDir,
          f.fileStorage
        )
        .background
        .use { _ =>
          f.capturedReq.get.timeout(10.seconds)
        }
    }

    val result = test.unsafeRunSync()
    result.wtid shouldBe f.wtid
  }
}
