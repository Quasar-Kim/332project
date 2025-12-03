package redsort.jobs.scheduler

import redsort.AsyncSpec
import redsort.jobs.Common._
import cats.effect._
import cats.effect.std.Queue
import org.scalamock.stubs.Stubs
import cats.effect.std.Supervisor
import scala.concurrent.duration._
import redsort.jobs.context.SchedulerCtx
import io.grpc.Server
import redsort.jobs.messages.WorkerFs2Grpc
import io.grpc.Metadata
import scala.concurrent.TimeoutException
import redsort.jobs.messages.SchedulerFs2Grpc
import io.grpc.netty.shaded.io.netty.handler.timeout
import redsort.jobs.messages.WorkerHello
import redsort.jobs.messages.LocalStorageInfo
import redsort.jobs.messages.FileEntryMsg
import redsort.jobs.messages.JobResult
import redsort.jobs.messages.WorkerError
import redsort.jobs.messages.WorkerErrorKind
import redsort.jobs.messages.JobSystemError
import redsort.jobs.messages.HaltRequest
import redsort.jobs.messages.WidMsg
import redsort.jobs.JobSystemException
import com.google.protobuf.empty.Empty

class SchedulerSpec extends AsyncSpec {
  def fixture = new {
    val workers = Seq(
      Seq(NetAddr("1.1.1.1", 5000), NetAddr("1.1.1.1", 5001)),
      Seq(NetAddr("1.1.1.2", 5000), NetAddr("1.1.1.2", 5001))
    )

    val ctxStub = stub[SchedulerCtx]
    val serverStub = stub[Server]
    (serverStub.start _).returnsWith(serverStub)
    val workerRpcClientStub = stub[WorkerFs2Grpc[IO, Metadata]]
    (ctxStub.schedulerRpcServer _).returnsWith(Resource.eval(IO(serverStub)))
    (ctxStub.workerRpcClient _).returnsWith(Resource.eval(IO(workerRpcClientStub)))

    def withSchedulerAndGrpc(
        body: (Scheduler, SchedulerFs2Grpc[IO, Metadata]) => IO[Unit]
    ): IO[Unit] =
      for {
        // intercept grpc server implementation using ctxStub.schedulerRpcServer
        grpcDeferred <- IO.deferred[SchedulerFs2Grpc[IO, Metadata]]
        _ <- IO((ctxStub.schedulerRpcServer _).returns { case (grpc, port) =>
          Resource.eval(grpcDeferred.complete(grpc) >> IO(serverStub))
        })

        _ <- Scheduler(
          port = 5000,
          numMachines = 2,
          numWorkersPerMachine = 2,
          ctx = ctxStub
        ) { scheduler =>
          for {
            grpc <- grpcDeferred.get
            _ <- body(scheduler, grpc)
          } yield ()
        }
      } yield ()

    def initAll(grpc: SchedulerFs2Grpc[IO, Metadata]) = for {
      // registration must happen in parallel
      _ <- (
        grpc.registerWorker(
          new WorkerHello(
            wtid = 0,
            storageInfo = Some(
              new LocalStorageInfo(
                mid = None,
                remainingStorage = 1024,
                entries = Map(
                  "@{input}/a" -> new FileEntryMsg(
                    path = "@{input}/a",
                    size = 1024,
                    replicas = Seq()
                  ),
                  "@{input}/b" -> new FileEntryMsg(
                    path = "@{input}/b",
                    size = 1024,
                    replicas = Seq()
                  )
                )
              )
            ),
            ip = "1.1.1.1",
            port = 5000
          ),
          new Metadata
        ),
        grpc.registerWorker(
          new WorkerHello(wtid = 1, storageInfo = None, ip = "1.1.1.1", port = 5001),
          new Metadata
        ),
        grpc.registerWorker(
          new WorkerHello(
            wtid = 0,
            storageInfo = Some(
              new LocalStorageInfo(
                mid = None,
                remainingStorage = 1024,
                entries = Map(
                  "@{input}/c" -> new FileEntryMsg(
                    path = "@{input}/c",
                    size = 1024,
                    replicas = Seq()
                  ),
                  "@{input}/d" -> new FileEntryMsg(
                    path = "@{input}/d",
                    size = 1024,
                    replicas = Seq()
                  )
                )
              )
            ),
            ip = "1.1.1.2",
            port = 5000
          ),
          new Metadata
        ),
        grpc.registerWorker(
          new WorkerHello(wtid = 1, storageInfo = None, ip = "1.1.1.2", port = 5001),
          new Metadata
        )
      ).parMapN((_, _, _, _) => IO.unit)
    } yield ()
  }

  val jobA = new JobSpec(
    name = "job",
    args = Seq(),
    inputs = Seq(new FileEntry(path = "@{input}/a", size = 1024, replicas = Seq(0))),
    outputs = Seq(new FileEntry(path = "@{working}/a.out", size = 1024, replicas = Seq(0)))
  )
  val jobB = new JobSpec(
    name = "job",
    args = Seq(),
    inputs = Seq(new FileEntry(path = "@{input}/b", size = 1024, replicas = Seq(0))),
    outputs = Seq(new FileEntry(path = "@{working}/b.out", size = 1024, replicas = Seq(0)))
  )
  val jobC = new JobSpec(
    name = "job",
    args = Seq(),
    inputs = Seq(new FileEntry(path = "@{input}/c", size = 1024, replicas = Seq(1))),
    outputs = Seq(new FileEntry(path = "@{working}/c.out", size = 1024, replicas = Seq(1)))
  )
  val jobD = new JobSpec(
    name = "job",
    args = Seq(),
    inputs = Seq(new FileEntry(path = "@{input}/d", size = 1024, replicas = Seq(1))),
    outputs = Seq(new FileEntry(path = "@{working}/d.out", size = 1024, replicas = Seq(1)))
  )

  val jobAresult = new JobResult(
    success = true,
    retval = None,
    error = None,
    stats = None,
    outputs = jobA.outputs.map(FileEntry.toMsg(_))
  )
  val jobBresult = new JobResult(
    success = true,
    retval = None,
    error = None,
    stats = None,
    outputs = jobB.outputs.map(FileEntry.toMsg(_))
  )
  val jobCresult = new JobResult(
    success = true,
    retval = None,
    error = None,
    stats = None,
    outputs = jobC.outputs.map(FileEntry.toMsg(_))
  )
  val jobDresult = new JobResult(
    success = true,
    retval = None,
    error = None,
    stats = None,
    outputs = jobD.outputs.map(FileEntry.toMsg(_))
  )

  behavior of "scheduler.waitInit"

  it should "wait until all workers are initialized and return input files" in {
    val f = fixture

    f.withSchedulerAndGrpc { case (scheduler, grpc) =>
      for {
        // no workers are initialized, so this should time out
        _ <- scheduler.waitInit.timeout(100.millis).assertThrows[TimeoutException]

        // init workers
        _ <- f.initAll(grpc)

        // now waitInit should pass
        files <- scheduler.waitInit
      } yield {
        def machineOneFiles(mid: Int) = Map(
          "@{input}/a" -> new FileEntry(
            path = "@{input}/a",
            size = 1024,
            replicas = Seq(mid)
          ),
          "@{input}/b" -> new FileEntry(path = "@{input}/b", size = 1024, replicas = Seq(mid))
        )
        def machineTwoFiles(mid: Int) = Map(
          "@{input}/c" -> new FileEntry(
            path = "@{input}/c",
            size = 1024,
            replicas = Seq(mid)
          ),
          "@{input}/d" -> new FileEntry(
            path = "@{input}/d",
            size = 1024,
            replicas = Seq(mid)
          )
        )

        files should (
          be(Map(0 -> machineOneFiles(0), 1 -> machineTwoFiles(1)))
            or be(Map(0 -> machineTwoFiles(0), 1 -> machineOneFiles(1)))
        )
      }
    }.timeout(5.second)
  }

  behavior of "scheduler.runJobs"

  it should "return job results and files on all jobs completed" in {
    val f = fixture

    // program client stub to always return successful result
    (f.workerRpcClientStub.runJob _).returns { case (specMsg, _) =>
      IO.pure {
        val spec = JobSpec.fromMsg(specMsg)
        if (spec == jobA) jobAresult
        else if (spec == jobB) jobBresult
        else if (spec == jobC) jobCresult
        else jobDresult
      }
    }

    f.withSchedulerAndGrpc { case (scheduler, grpc) =>
      for {
        // init workers
        _ <- f.initAll(grpc)
        _ <- scheduler.waitInit.timeout(100.millis)

        // enqueue 4 jobs, executed on each workers
        result <- scheduler.runJobs(Seq(jobA, jobB, jobC, jobD))
      } yield {
        val files = result.files.map { case (_, m) => m.keys }.flatten.toSet
        files shouldBe Set(
          "@{input}/a",
          "@{input}/b",
          "@{input}/c",
          "@{input}/d",
          "@{working}/a.out",
          "@{working}/b.out",
          "@{working}/c.out",
          "@{working}/d.out"
        )

        result.results.to(Set) should be(
          Set(
            (jobA, jobAresult),
            (jobB, jobBresult),
            (jobC, jobCresult),
            (jobD, jobDresult)
          )
        )
      }
    }.timeout(1.second)
  }

  it should "synchornize file entries of all machines after all jobs are completed" in {
    val f = fixture

    // program client stub to always return successful result
    val jobResult = new JobResult(
      success = true,
      retval = None,
      error = None,
      stats = None
    )
    (f.workerRpcClientStub.runJob _).returnsWith(IO.pure(jobResult))

    f.withSchedulerAndGrpc { case (scheduler, grpc) =>
      for {
        // init workers
        _ <- f.initAll(grpc)
        _ <- scheduler.waitInit.timeout(100.millis)

        // enqueue 4 jobs, executed on each workers
        result <- scheduler.runJobs(Seq(jobA, jobB, jobC, jobD))
      } yield {
        (f.workerRpcClientStub.runJob _).calls(5)._1.name should be(Scheduler.SYNC_JOB_NAME)
      }
    }.timeout(1.second)
  }

  it should "raise error if one of jobs errors" in {
    val f = fixture

    // program client stub to return error on 3rd call
    val goodJobResult = new JobResult(
      success = true,
      retval = None,
      error = None,
      stats = None
    )
    val badJobResult = new JobResult(
      success = false,
      retval = None,
      error = Some(
        new WorkerError(
          kind = WorkerErrorKind.BODY_ERROR,
          inner = Some(
            new JobSystemError(
              message = "some error",
              cause = None,
              context = Map()
            )
          )
        )
      ),
      stats = None
    )
    (f.workerRpcClientStub.runJob _).returnsOnCall {
      case 3 => IO.pure(badJobResult)
      case _ => IO.pure(goodJobResult)
    }

    f.withSchedulerAndGrpc { case (scheduler, grpc) =>
      for {
        // init workers
        _ <- f.initAll(grpc)
        _ <- scheduler.waitInit.timeout(100.millis)

        // enqueue 4 jobs, executed on each workers
        result <- scheduler.runJobs(Seq(jobA, jobB, jobC, jobD)).attempt
      } yield {
        result match {
          case Left(error) =>
          case Right(_)    => fail("runJobs did not returned error")
        }
      }
    }.timeout(1.second)
  }

  it should "raise error if one of workers request halt" in {
    val f = fixture

    // do not respond to runJob request
    (f.workerRpcClientStub.runJob _).returnsWith(
      IO.sleep(100.second) >> IO(
        new JobResult(success = false, retval = None, error = None, stats = None)
      )
    )

    f.withSchedulerAndGrpc { case (scheduler, grpc) =>
      for {
        // init workers
        _ <- f.initAll(grpc)
        _ <- scheduler.waitInit.timeout(100.millis)
        result <- (
          grpc.haltOnError(
            new HaltRequest(
              source = new WidMsg(0, 0),
              err = new JobSystemError(message = "some error", cause = None, context = Map())
            ),
            new Metadata()
          ),
          scheduler.runJobs(Seq(jobA, jobB, jobC, jobD)).attempt
        ).parMapN((_, result) => result)
      } yield {
        result match {
          case Left(error) => error shouldBe a[JobSystemException]
          case Right(_)    => fail("runJobs did not returned error")
        }
      }
    }.timeout(1.second)
  }

  behavior of "scheduler.complete"

  it should "call complete RPC method of worker" in {
    val f = fixture
    (f.workerRpcClientStub.complete _).returnsWith(IO(new Empty))

    f.withSchedulerAndGrpc { case (scheduler, grpc) =>
      for {
        // init workers
        _ <- f.initAll(grpc)
        _ <- scheduler.waitInit
        _ <- scheduler.complete
      } yield {
        (f.workerRpcClientStub.complete _).calls.length should be(4)
      }
    }.timeout(1.second)
  }

  behavior of "Scheduler.syncJobSpecs"

  it should "create sync jobs whose argument contains all files present in the machine" in {
    val files = Map(
      0 -> Map(
        "@{working}/a" -> new FileEntry(path = "@{working}/a", size = 1024, replicas = Seq(0, 1))
      ),
      1 -> Map(
        "@{working}/b" -> new FileEntry(path = "@{working}/b", size = 1024, replicas = Seq(0, 1))
      )
    )

    val specs = Scheduler.syncJobSpecs(files)
    IO {
      val machineOneSpec = specs.find(_.outputs(0).replicas == Seq(0)).get
      val machineOneFiles = machineOneSpec.args.map { case msg: FileEntryMsg =>
        FileEntry.fromMsg(msg).path
      }
      machineOneFiles.toSet shouldBe Set("@{working}/a", "@{working}/b")

      val machineTwoSpec = specs.find(_.outputs(0).replicas == Seq(1)).get
      val machineTwoFiles = machineTwoSpec.args.map { case msg: FileEntryMsg =>
        FileEntry.fromMsg(msg).path
      }.toSet
      machineTwoFiles.toSet shouldBe Set("@{working}/a", "@{working}/b")
    }
  }
}
