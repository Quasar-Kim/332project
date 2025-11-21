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

    val schedulerAndServer = for {
      // intercept grpc server implementation using ctxStub.schedulerRpcServer
      grpcDeferred <- Resource.eval(IO.deferred[SchedulerFs2Grpc[IO, Metadata]])
      _ <- Resource.eval(IO((ctxStub.schedulerRpcServer _).returns { case (grpc, port) =>
        Resource.eval(grpcDeferred.complete(grpc) >> IO(serverStub))
      }))

      // start scheduler
      scheduler <- Scheduler(5000, workers, ctxStub, SimpleScheduleLogic)

      // get intercepted server implementation
      grpc <- Resource.eval(grpcDeferred.get)
    } yield (scheduler, grpc)

    def initAll(grpc: SchedulerFs2Grpc[IO, Metadata]) = for {
      _ <- grpc.registerWorker(
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
                "@{input}/b" -> new FileEntryMsg(path = "@{input}/b", size = 1024, replicas = Seq())
              )
            )
          ),
          ip = "1.1.1.1",
          port = 5000
        ),
        new Metadata
      )
      _ <- grpc.registerWorker(
        new WorkerHello(wtid = 1, storageInfo = None, ip = "1.1.1.1", port = 5001),
        new Metadata
      )
      _ <- grpc.registerWorker(
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
                "@{input}/d" -> new FileEntryMsg(path = "@{input}/d", size = 1024, replicas = Seq())
              )
            )
          ),
          ip = "1.1.1.2",
          port = 5000
        ),
        new Metadata
      )
      _ <- grpc.registerWorker(
        new WorkerHello(wtid = 1, storageInfo = None, ip = "1.1.1.2", port = 5001),
        new Metadata
      )
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

  behavior of "scheduler.waitInit"

  it should "wait until all workers are initialized and return input files" in {
    val f = fixture

    f.schedulerAndServer
      .use { case (scheduler, grpc) =>
        for {
          // no workers are initialized, so this should time out
          _ <- scheduler.waitInit.timeout(100.millis).assertThrows[TimeoutException]

          // init workers
          _ <- f.initAll(grpc)

          // now waitWorkers should pass
          files <- scheduler.waitInit.timeout(100.millis)
        } yield {
          files should be(
            Map(
              0 -> Map(
                "@{input}/a" -> new FileEntry(
                  path = "@{input}/a",
                  size = 1024,
                  replicas = Seq(0)
                ),
                "@{input}/b" -> new FileEntry(path = "@{input}/b", size = 1024, replicas = Seq(0))
              ),
              1 -> Map(
                "@{input}/c" -> new FileEntry(
                  path = "@{input}/c",
                  size = 1024,
                  replicas = Seq(1)
                ),
                "@{input}/d" -> new FileEntry(
                  path = "@{input}/d",
                  size = 1024,
                  replicas = Seq(1)
                )
              )
            )
          )
        }
      }
      .timeout(1.second)
  }

  behavior of "scheduler.runJobs"

  it should "return job results and files on all jobs completed" in {
    val f = fixture

    // program client stub to always return successful result
    val jobResult = new JobResult(
      success = true,
      retval = None,
      error = None,
      stats = None
    )
    (f.workerRpcClientStub.runJob _).returnsWith(IO.pure(jobResult))

    f.schedulerAndServer
      .use { case (scheduler, grpc) =>
        for {
          // init workers
          _ <- f.initAll(grpc)
          _ <- scheduler.waitInit.timeout(100.millis)

          // enqueue 4 jobs, executed on each workers
          result <- scheduler.runJobs(Seq(jobA, jobB, jobC, jobD))
        } yield {
          result.files should be(
            Map(
              0 -> Map(
                "@{input}/a" -> new FileEntry(
                  path = "@{input}/a",
                  size = 1024,
                  replicas = Seq(0)
                ),
                "@{input}/b" -> new FileEntry(path = "@{input}/b", size = 1024, replicas = Seq(0)),
                "@{working}/a.out" -> new FileEntry(
                  path = "@{working}/a.out",
                  size = 1024,
                  replicas = Seq(0)
                ),
                "@{working}/b.out" -> new FileEntry(
                  path = "@{working}/b.out",
                  size = 1024,
                  replicas = Seq(0)
                )
              ),
              1 -> Map(
                "@{input}/c" -> new FileEntry(
                  path = "@{input}/c",
                  size = 1024,
                  replicas = Seq(1)
                ),
                "@{input}/d" -> new FileEntry(
                  path = "@{input}/d",
                  size = 1024,
                  replicas = Seq(1)
                ),
                "@{working}/c.out" -> new FileEntry(
                  path = "@{working}/c.out",
                  size = 1024,
                  replicas = Seq(1)
                ),
                "@{working}/d.out" -> new FileEntry(
                  path = "@{working}/d.out",
                  size = 1024,
                  replicas = Seq(1)
                )
              )
            )
          )

          result.results.to(Set) should be(
            Set(
              (jobA, jobResult),
              (jobB, jobResult),
              (jobC, jobResult),
              (jobD, jobResult)
            )
          )
        }
      }
      .timeout(1.second)
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

    f.schedulerAndServer
      .use { case (scheduler, grpc) =>
        for {
          // init workers
          _ <- f.initAll(grpc)
          _ <- scheduler.waitInit.timeout(100.millis)

          // enqueue 4 jobs, executed on each workers
          result <- scheduler.runJobs(Seq(jobA, jobB, jobC, jobD))
        } yield {
          (f.workerRpcClientStub.runJob _).calls(5)._1.name should be(Scheduler.SYNC_JOB_NAME)
        }
      }
      .timeout(1.second)
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

    f.schedulerAndServer
      .use { case (scheduler, grpc) =>
        for {
          // init workers
          _ <- f.initAll(grpc)
          _ <- scheduler.waitInit.timeout(100.millis)

          // enqueue 4 jobs, executed on each workers
          result <- scheduler.runJobs(Seq(jobA, jobB, jobC, jobD)).attempt
        } yield {
          result match {
            case Left(error) => ()
            case Right(_)    => fail("runJobs did not returned error")
          }
        }
      }
      .timeout(1.second)
  }

  it should "raise error if one of workers request halt" in {
    val f = fixture

    // do not respond to runJob request
    (f.workerRpcClientStub.runJob _).returnsWith(
      IO.sleep(100.second) >> IO(
        new JobResult(success = false, retval = None, error = None, stats = None)
      )
    )

    f.schedulerAndServer
      .use { case (scheduler, grpc) =>
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
      }
      .timeout(1.second)
  }
}
