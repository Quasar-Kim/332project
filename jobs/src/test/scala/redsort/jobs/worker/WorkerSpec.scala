package redsort.jobs.worker

import cats.effect._
import redsort.AsyncSpec
import redsort.jobs.messages.SchedulerFs2Grpc
import io.grpc.Metadata
import redsort.jobs.workers.SharedState
import redsort.jobs.context.WorkerCtx
import redsort.jobs.Common.FileEntry
import redsort.jobs.messages.SchedulerHello
import redsort.jobs.messages.NetAddrMsg
import fs2.io.file.Path
import scala.concurrent.duration._
import redsort.jobs.messages.FileEntryMsg
import redsort.jobs.Common.Wid
import redsort.jobs.Common.NetAddr
import redsort.jobs.messages.WorkerFs2Grpc
import io.grpc.Server
import com.google.protobuf.empty.Empty
import scala.concurrent.TimeoutException
import redsort.jobs.messages.ReplicatorLocalServiceFs2Grpc
import redsort.jobs.messages.ReplicatorRemoteServiceFs2Grpc

class WorkerSpec extends AsyncSpec {
  def fixture = new {
    val schedulerClientStub = stub[SchedulerFs2Grpc[IO, Metadata]]
    val replicatorLocalClientStub = stub[ReplicatorLocalServiceFs2Grpc[IO, Metadata]]
    val replicatorRemoteClientStub = stub[ReplicatorRemoteServiceFs2Grpc[IO, Metadata]]
    val replicators =
      Map(0 -> new NetAddrMsg("1.2.3.3", 8000), 1 -> new NetAddrMsg("1.2.3.4", 8000))
    (schedulerClientStub.registerWorker _).returnsWith(
      IO.pure(
        new SchedulerHello(
          mid = 1,
          replicatorAddrs = replicators,
          success = true
        )
      )
    )
    val serverStub = stub[Server]
    (serverStub.start _).returnsWith(serverStub)
    val ctxStub = stub[WorkerCtx]
    (ctxStub.mkDir _).returns { arg => IO(arg) }
    (ctxStub.exists _).returnsWith(IO.pure(true))
    (ctxStub.getIP).returnsWith(IO.pure("1.2.3.4"))
    (ctxStub.list _).returnsWith(
      IO.pure(
        Map(
          "/sda/a" -> new FileEntry(path = "/sda/a", size = 1024, replicas = Seq()),
          "/sda/b" -> new FileEntry(path = "/sda/b", size = 1024, replicas = Seq())
        )
      )
    )
    (ctxStub.delete _).returnsWith(IO.unit)
    (ctxStub.deleteRecursively _).returnsWith(IO.unit)
    (ctxStub.schedulerRpcClient _).returnsWith(Resource.eval(IO(schedulerClientStub)))
    (ctxStub.replicatorLocalRpcClient _).returnsWith(Resource.eval(IO(replicatorLocalClientStub)))
    (ctxStub.replicatorRemoteRpcClient _).returnsWith(Resource.eval(IO(replicatorRemoteClientStub)))
    (ctxStub.replicatorLocalRpcServer _).returnsWith(Resource.eval(IO(serverStub)))
    (ctxStub.replicatorRemoteRpcServer _).returnsWith(Resource.eval(IO(serverStub)))

    val dirs = new Directories(
      inputDirectories = Seq(Path("/sda")),
      outputDirectory = Path("/output"),
      workingDirectory = Path("/working")
    )

    def withWorkerAndGrpc(
        body: (Worker, WorkerFs2Grpc[IO, Metadata]) => IO[Unit]
    ): IO[Unit] =
      for {
        // intercept grpc server implementation
        grpcDeferred <- IO.deferred[WorkerFs2Grpc[IO, Metadata]]
        _ <- IO((ctxStub.workerRpcServer _).returns { case (grpc, port) =>
          Resource.eval(grpcDeferred.complete(grpc) >> IO(serverStub))
        })
        _ <- Worker(
          handlerMap = Map(),
          masterAddr = new NetAddr("127.0.0.1", 6000),
          inputDirectories = Seq(Path("/sda")),
          outputDirectory = Path("/output"),
          wtid = 0,
          port = 5000,
          ctx = ctxStub,
          replicatorLocalPort = 6000,
          replicatorRemotePort = 7000
        ) { worker =>
          for {
            grpc <- grpcDeferred.get
            _ <- body(worker, grpc)
          } yield ()
        }
      } yield ()
  }

  behavior of "Worker.registerWorkerToScheduler"

  it should "call RegisterWorker RPC method and update state if registration was successful" in {
    val f = fixture

    for {
      stateR <- SharedState.init
      _ <- Worker
        .registerWorkerToScheduler(
          schedulerClient = f.schedulerClientStub,
          stateR = stateR,
          wtid = 0,
          port = 5000,
          dirs = f.dirs,
          ctx = f.ctxStub
        )
        .timeout(1.second)
      state <- stateR.get
    } yield {
      val clientHello = (f.schedulerClientStub.registerWorker _).calls(0)._1
      clientHello.ip should be("1.2.3.4")
      clientHello.port should be(5000)
      clientHello.wtid should be(0)
      clientHello.storageInfo.get.entries should be(
        Map(
          "@{input}/sda/a" -> new FileEntryMsg(
            path = "@{input}/sda/a",
            size = 1024,
            replicas = Seq()
          ),
          "@{input}/sda/b" -> new FileEntryMsg(
            path = "@{input}/sda/b",
            size = 1024,
            replicas = Seq()
          )
        )
      )

      state.replicatorAddrs should be(f.replicators.view.mapValues(NetAddr.fromMsg(_)).toMap)
      state.wid should be(Some(new Wid(1, 0)))
    }
  }

  it should "raise error if registration was not successful" in {
    val f = fixture
    (f.schedulerClientStub.registerWorker _).returnsWith(
      IO.pure(
        new SchedulerHello(
          success = false,
          failReason = "some error"
        )
      )
    )

    for {
      stateR <- SharedState.init
      result <- Worker
        .registerWorkerToScheduler(
          schedulerClient = f.schedulerClientStub,
          stateR = stateR,
          wtid = 0,
          port = 5000,
          dirs = f.dirs,
          ctx = f.ctxStub
        )
        .attempt
        .timeout(1.second)
      state <- stateR.get
    } yield {
      result match {
        case Left(err)    => err.getMessage() should include("some error")
        case Right(value) => throw new AssertionError("got Right while expecting Left")
      }
    }
  }

  "Worker.waitForComplete" should "return when scheduler call Complete() RPC method" in {
    val f = fixture

    f.withWorkerAndGrpc { (worker, grpc) =>
      for {
        _ <- worker.waitForComplete.timeout(500.millis).assertThrows[TimeoutException]
        _ <- grpc.complete(new Empty, new Metadata)
        _ <- worker.waitForComplete.timeout(500.millis)
      } yield ()
    }
  }
}
