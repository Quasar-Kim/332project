package redsort.jobs.scheduler

import redsort.AsyncSpec
import redsort.jobs.Common._
import cats._
import cats.effect._
import cats.syntax.all._
import cats.effect.std.Queue
import org.scalamock.stubs.Stubs
import cats.effect.std.Supervisor
import scala.concurrent.duration._
import redsort.jobs.messages.WorkerHello
import redsort.jobs.JobSystemException
import redsort.jobs.messages.JobSystemError
import monocle.syntax.all._
import redsort.jobs.messages.JobResult
import redsort.jobs.Unreachable
import redsort.jobs.messages.WorkerError
import redsort.jobs.messages.WorkerErrorKind
import redsort.jobs.messages.LocalStorageInfo
import redsort.jobs.messages.FileEntryMsg

class SchedulerFiberSpec extends AsyncSpec {
  def fixture = new {
    val workerAddrs = Map(
      new Wid(0, 0) -> new NetAddr("1.1.1.1", 5000),
      new Wid(0, 1) -> new NetAddr("1.1.1.1", 5001),
      new Wid(1, 0) -> new NetAddr("1.1.1.2", 5000),
      new Wid(1, 1) -> new NetAddr("1.1.1.2", 5001)
    )

    val getSharedState = SharedState.init(workerAddrs)
    val getMainFiberQueue = Queue.unbounded[IO, MainFiberEvents]
    val getSchedulerFiberQueue = Queue.unbounded[IO, SchedulerFiberEvents]
    val getRpcClientFiberQueues = Scheduler.createRpcClientFiberQueues(workerAddrs)

    val scheduleLogic = new ScheduleLogic {
      override def schedule(
          workerStates: Map[Wid, WorkerState],
          specs: Seq[JobSpec]
      ): Map[Wid, WorkerState] = {
        // schedule each job to Wid((i / 2) % 2, i % 2)
        val updatedWorkerStates = specs.zipWithIndex.foldLeft(workerStates) {
          case (acc, (spec, i)) =>
            val wid = new Wid((i / 2) % 2, i % 2)
            val job = new Job(
              state = JobState.Pending,
              ttl = 0,
              spec = spec,
              result = None
            )
            val updatedWorkerState = acc(wid).focus(_.pendingJobs).modify(q => q.enqueue(job))
            acc.updated(wid, updatedWorkerState)
        }
        updatedWorkerStates
      }

      override def evaluate(spec: JobSpec, wid: Wid, workerState: WorkerState): ScheduleEvaluation =
        ???
    }

    val startSchedulerFiber = Supervisor[IO].evalMap(sv =>
      for {
        stateR <- getSharedState
        mainFiberQueue <- getMainFiberQueue
        schedulerFiberQueue <- getSchedulerFiberQueue
        rpcClientFiberQueues <- getRpcClientFiberQueues
        _ <- sv
          .supervise(
            SchedulerFiber
              .start(
                stateR,
                mainFiberQueue,
                schedulerFiberQueue,
                rpcClientFiberQueues,
                scheduleLogic
              )
              .useForever
          )
          .void
      } yield (stateR, mainFiberQueue, schedulerFiberQueue, rpcClientFiberQueues)
    )

    def initAll(
        stateR: Ref[IO, SharedState],
        mainFiberQueue: Queue[IO, MainFiberEvents],
        schedulerFiberQueue: Queue[IO, SchedulerFiberEvents]
    ) = for {
      _ <- schedulerFiberQueue.tryOfferN(
        List(
          new SchedulerFiberEvents.WorkerRegistration(
            new WorkerHello(
              wtid = 0,
              storageInfo = Some(
                new LocalStorageInfo(
                  mid = None,
                  remainingStorage = 1024,
                  entries = Map(
                    "@{working}/a.in" -> new FileEntryMsg(
                      path = "@{working}/a.in",
                      size = 1024,
                      replicas = Seq()
                    ),
                    "@{working}/b.in" -> new FileEntryMsg(
                      path = "@{working}/b.in",
                      size = 1024,
                      replicas = Seq()
                    )
                  )
                )
              ),
              ip = "1.1.1.1"
            ),
            new Wid(0, 0)
          ),
          new SchedulerFiberEvents.WorkerRegistration(
            new WorkerHello(
              wtid = 1,
              storageInfo = None,
              ip = "1.1.1.1"
            ),
            new Wid(0, 1)
          ),
          new SchedulerFiberEvents.WorkerRegistration(
            new WorkerHello(
              wtid = 0,
              storageInfo = Some(
                new LocalStorageInfo(
                  mid = None,
                  remainingStorage = 1024,
                  entries = Map(
                    "@{input}/c.in" -> new FileEntryMsg(
                      path = "@{input}/c.in",
                      size = 1024,
                      replicas = Seq()
                    ),
                    "@{input}/d.in" -> new FileEntryMsg(
                      path = "@{input}/d.in",
                      size = 1024,
                      replicas = Seq()
                    )
                  )
                )
              ),
              ip = "1.1.1.2"
            ),
            new Wid(1, 0)
          ),
          new SchedulerFiberEvents.WorkerRegistration(
            new WorkerHello(
              wtid = 1,
              storageInfo = None,
              ip = "1.1.1.2"
            ),
            new Wid(1, 1)
          )
        )
      )
      evt <- mainFiberQueue.take
    } yield {
      assume(evt.isInstanceOf[MainFiberEvents.Initialized])
    }
  }

  val wid = new Wid(1, 0)

  val jobSpecA = new JobSpec(name = "a", args = Seq(), inputs = Seq(), outputs = Seq())
  val jobSpecB = new JobSpec(name = "b", args = Seq(), inputs = Seq(), outputs = Seq())
  val jobSpecC = new JobSpec(name = "c", args = Seq(), inputs = Seq(), outputs = Seq())
  val jobSpecD = new JobSpec(name = "d", args = Seq(), inputs = Seq(), outputs = Seq())
  val jobSpecE = new JobSpec(name = "e", args = Seq(), inputs = Seq(), outputs = Seq())
  val jobSpecs = Seq(
    jobSpecA,
    jobSpecB,
    jobSpecC,
    jobSpecD
  )

  behavior of "SchedulerFiber (upon receiving WorkerRegistration)"

  it should "initialize worker state if not initialized" in {
    val f = fixture
    val workerHello = new WorkerHello(
      wtid = 0,
      storageInfo = Some(
        new LocalStorageInfo(
          mid = Some(1),
          remainingStorage = 1024,
          entries = Map(
            "@{working}/a" -> new FileEntryMsg(
              path = "@{working}/a",
              size = 1024,
              replicas = Seq(1)
            ),
            "@{working}/b" -> new FileEntryMsg(
              path = "@{working}/b",
              size = 1024,
              replicas = Seq(1)
            )
          )
        )
      ),
      ip = "1.1.1.2"
    )

    f.startSchedulerFiber
      .use { case (stateR, mainFiberQueue, schedulerFiberQueue, rpcClientFiberQueues) =>
        for {
          state <- stateR.get
          _ <- IO(state.schedulerFiber.workers(wid).initialized should be(false))
          _ <- schedulerFiberQueue.offer(
            new SchedulerFiberEvents.WorkerRegistration(workerHello, wid)
          )
          _ <- IO.sleep(100.millis)
          state <- stateR.get
        } yield {
          val s = state.schedulerFiber.workers(wid)
          s.initialized should be(true)
          s.status should be(WorkerStatus.Up)
          state.schedulerFiber.files(wid.mid) should be(
            Map(
              "@{working}/a" -> new FileEntry(
                path = "@{working}/a",
                size = 1024,
                replicas = Seq(1)
              ),
              "@{working}/b" -> new FileEntry(path = "@{working}/b", size = 1024, replicas = Seq(1))
            )
          )
        }
      }
      .timeout(1.second)
  }

  it should "emit Initialized event when all workers are registered and change state to Idle" in {
    val f = fixture

    f.startSchedulerFiber
      .use { case (stateR, mainFiberQueue, schedulerFiberQueue, rpcClientFiberQueues) =>
        for {
          _ <- f.initAll(stateR, mainFiberQueue, schedulerFiberQueue)
          state <- stateR.get
        } yield {
          state.schedulerFiber.state should be(SchedulerState.Idle)
        }
      }
      .timeout(1.second)
  }

  // it should "initialize worker state and enqueue WorkerUp if worker status is initialized but DOWN"

  // it should "trigger reschedule if worker status is UP"

  // behavior of "SchedulerFiber (upon receiving HeartbeatTimeout)"

  // it should "detect fault and reschedule jobs"

  behavior of "SchedulerFiber (upon receiving Halt)"

  it should "raise received error" in {
    val f = fixture
    val err = new JobSystemError(
      message = "some error",
      cause = None,
      context = Map()
    )

    f.startSchedulerFiber
      .use { case (stateR, mainFiberQueue, schedulerFiberQueue, rpcClientFiberQueues) =>
        for {
          _ <- schedulerFiberQueue.offer(new SchedulerFiberEvents.Halt(err, wid))
          evt <- mainFiberQueue.take
        } yield {
          inside(evt) {
            case MainFiberEvents.SystemException(
                  JobSystemException(message, source, context, cause)
                ) =>
              cause should be(None)
              source should be("<worker 1,0>")
          }
        }
      }
      .timeout(1.second)
  }

  behavior of "SchedulerFiber (upon receiving FatalError)"

  it should "raise received error" in {
    val f = fixture
    val err = new AssertionError("you miserably failed")

    f.startSchedulerFiber
      .use { case (stateR, mainFiberQueue, schedulerFiberQueue, rpcClientFiberQueues) =>
        for {
          _ <- schedulerFiberQueue.offer(new SchedulerFiberEvents.FatalError(err))
          evt <- mainFiberQueue.take
        } yield {
          inside(evt) { case MainFiberEvents.SystemException(e) =>
            e shouldEqual (err)
          }
        }
      }
      .timeout(1.second)
  }

  behavior of "SchedulerFiber (upon receiving Jobs)"

  it should "schedule jobs and change state to Running if state is Idle" in {
    val f = fixture

    f.startSchedulerFiber
      .use { case (stateR, mainFiberQueue, schedulerFiberQueue, rpcClientFiberQueues) =>
        for {
          _ <- f.initAll(stateR, mainFiberQueue, schedulerFiberQueue)
          _ <- schedulerFiberQueue.offer(new SchedulerFiberEvents.Jobs(jobSpecs))
          a <- rpcClientFiberQueues(new Wid(0, 0)).take
          b <- rpcClientFiberQueues(new Wid(0, 1)).take
          c <- rpcClientFiberQueues(new Wid(1, 0)).take
          d <- rpcClientFiberQueues(new Wid(1, 1)).take
          state <- stateR.get
        } yield {
          val spec = Seq(a, b, c, d).map {
            case WorkerFiberEvents.Job(spec) => spec
            case _                           => throw new AssertionError
          }
          spec should equal(jobSpecs)
          state.schedulerFiber.state should equal(SchedulerState.Running)
        }
      }
      .timeout(1.second)
  }

  it should "raise error if state is not Idle" in {
    val f = fixture

    f.startSchedulerFiber
      .use { case (stateR, mainFiberQueue, schedulerFiberQueue, rpcClientFiberQueues) =>
        for {
          // no init
          _ <- schedulerFiberQueue.offer(new SchedulerFiberEvents.Jobs(jobSpecs))
          evt <- mainFiberQueue.take
        } yield {
          evt shouldBe a[MainFiberEvents.SystemException]
        }
      }
      .timeout(1.second)
  }

  behavior of "SchedulerFiber (upon receiving JobCompleted)"

  it should "run another job if there are remaining jobs" in {
    val f = fixture
    val result = new JobResult(
      success = true,
      retval = None,
      error = None,
      stats = None
    )
    val wid = new Wid(0, 0)

    f.startSchedulerFiber
      .use { case (stateR, mainFiberQueue, schedulerFiberQueue, rpcClientFiberQueues) =>
        for {
          _ <- f.initAll(stateR, mainFiberQueue, schedulerFiberQueue)

          // schedule four + one jobs, each scheduled to each workers
          _ <- schedulerFiberQueue.offer(new SchedulerFiberEvents.Jobs(jobSpecs :+ jobSpecE))
          a <- rpcClientFiberQueues(new Wid(0, 0)).take
          b <- rpcClientFiberQueues(new Wid(0, 1)).take
          c <- rpcClientFiberQueues(new Wid(1, 0)).take
          d <- rpcClientFiberQueues(new Wid(1, 1)).take

          // enqueue JobCompleted event
          _ <- schedulerFiberQueue.offer(new SchedulerFiberEvents.JobCompleted(result, wid))

          // now another job will be enqueued to worker 0,0
          evt <- rpcClientFiberQueues(wid).take

          // sleep briefly to allow shared states to be updated
          _ <- IO.sleep(100.millis)
          state <- stateR.get
        } yield {
          evt match {
            case WorkerFiberEvents.Job(s) => s should be(jobSpecE)
            case _                        => fail("evt is not Job")
          }
          val completedJob = state.schedulerFiber.workers(wid).completedJobs.dequeue._1
          completedJob.spec should be(jobSpecA)
          completedJob.state should be(JobState.Completed)
        }
      }
      .timeout(1.second)
  }

  it should "return job specs and files to caller and change state to Idle if all jobs are completed" in {
    val f = fixture
    val jobResult = new JobResult(
      success = true,
      retval = None,
      error = None,
      stats = None
    )
    val wid = new Wid(0, 0)

    f.startSchedulerFiber
      .use { case (stateR, mainFiberQueue, schedulerFiberQueue, rpcClientFiberQueues) =>
        for {
          _ <- f.initAll(stateR, mainFiberQueue, schedulerFiberQueue)
          _ <- schedulerFiberQueue.offer(new SchedulerFiberEvents.Jobs(jobSpecs))
          _ <- rpcClientFiberQueues(new Wid(0, 0)).take
          _ <- rpcClientFiberQueues(new Wid(0, 1)).take
          _ <- rpcClientFiberQueues(new Wid(1, 0)).take
          _ <- rpcClientFiberQueues(new Wid(1, 1)).take

          // enqueue JobCompleted event
          _ <- schedulerFiberQueue.tryOfferN(
            List(
              new SchedulerFiberEvents.JobCompleted(jobResult, new Wid(0, 0)),
              new SchedulerFiberEvents.JobCompleted(jobResult, new Wid(0, 1)),
              new SchedulerFiberEvents.JobCompleted(jobResult, new Wid(1, 0)),
              new SchedulerFiberEvents.JobCompleted(jobResult, new Wid(1, 1))
            )
          )

          // wait for job completion event
          result <- mainFiberQueue.take
        } yield {
          result match {
            case MainFiberEvents.JobCompleted(results, files) => {
              results should be(
                Seq(
                  (jobSpecA, jobResult),
                  (jobSpecB, jobResult),
                  (jobSpecC, jobResult),
                  (jobSpecD, jobResult)
                )
              )
              files should be(
                Map(
                  0 -> Map(
                    "@{working}/a.in" -> new FileEntry(
                      path = "@{working}/a.in",
                      size = 1024,
                      replicas = Seq(0)
                    ),
                    "@{working}/b.in" -> new FileEntry(
                      path = "@{working}/b.in",
                      size = 1024,
                      replicas = Seq(0)
                    )
                  ),
                  1 -> Map(
                    "@{input}/c.in" -> new FileEntry(
                      path = "@{input}/c.in",
                      size = 1024,
                      replicas = Seq(1)
                    ),
                    "@{input}/d.in" -> new FileEntry(
                      path = "@{input}/d.in",
                      size = 1024,
                      replicas = Seq(1)
                    )
                  )
                )
              )
            }
            case _ => fail("result is not JobCompleted")
          }
        }
      }
      .timeout(1.second)
  }

  it should "update worker file entries if all jobs are completed" in {
    val f = fixture
    val jobResult = new JobResult(
      success = true,
      retval = None,
      error = None,
      stats = None
    )
    val wid = new Wid(0, 0)
    val jobSpec1 = new JobSpec(
      name = "x",
      args = Seq(),
      inputs = Seq(new FileEntry(path = "@{working}/a.in", size = 1024, replicas = Seq(0))),
      outputs = Seq(new FileEntry(path = "@{working}/a.out", size = 1024, replicas = Seq(0)))
    )
    val jobSpec2 = new JobSpec(
      name = "x",
      args = Seq(),
      inputs = Seq(new FileEntry(path = "@{working}/b.in", size = 1024, replicas = Seq(0))),
      outputs = Seq(new FileEntry(path = "@{working}/b.out", size = 1024, replicas = Seq(0)))
    )
    val jobSpec3 = new JobSpec(
      name = "x",
      args = Seq(),
      inputs = Seq(new FileEntry(path = "@{input}/c.in", size = 1024, replicas = Seq(1))),
      outputs = Seq(new FileEntry(path = "@{working}/c.out", size = 1024, replicas = Seq(1)))
    )
    val jobSpec4 = new JobSpec(
      name = "x",
      args = Seq(),
      inputs = Seq(new FileEntry(path = "@{input}/d.in", size = 1024, replicas = Seq(1))),
      outputs = Seq(new FileEntry(path = "@{working}/d.out", size = 1024, replicas = Seq(1)))
    )

    f.startSchedulerFiber
      .use { case (stateR, mainFiberQueue, schedulerFiberQueue, rpcClientFiberQueues) =>
        for {
          _ <- f.initAll(stateR, mainFiberQueue, schedulerFiberQueue)
          _ <- schedulerFiberQueue.offer(
            new SchedulerFiberEvents.Jobs(Seq(jobSpec1, jobSpec2, jobSpec3, jobSpec4))
          )
          _ <- rpcClientFiberQueues(new Wid(0, 0)).take
          _ <- rpcClientFiberQueues(new Wid(0, 1)).take
          _ <- rpcClientFiberQueues(new Wid(1, 0)).take
          _ <- rpcClientFiberQueues(new Wid(1, 1)).take

          // enqueue JobCompleted event
          _ <- schedulerFiberQueue.tryOfferN(
            List(
              new SchedulerFiberEvents.JobCompleted(jobResult, new Wid(0, 0)),
              new SchedulerFiberEvents.JobCompleted(jobResult, new Wid(0, 1)),
              new SchedulerFiberEvents.JobCompleted(jobResult, new Wid(1, 0)),
              new SchedulerFiberEvents.JobCompleted(jobResult, new Wid(1, 1))
            )
          )

          // wait for job completion event
          _ <- mainFiberQueue.take
          state <- stateR.get
        } yield {
          // files on machine 0 were all in working directory - they are deleted
          state.schedulerFiber.files(0) should be(
            Map(
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
            )
          )

          // files on machine 1 were all in input directory - they are preserved
          state.schedulerFiber.files(1) should be(
            Map(
              "@{input}/c.in" -> new FileEntry(
                path = "@{input}/c.in",
                size = 1024,
                replicas = Seq(1)
              ),
              "@{input}/d.in" -> new FileEntry(
                path = "@{input}/d.in",
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
        }
      }
      .timeout(1.second)
  }

  behavior of "SchedulerFiber (upon receiving JobFailed)"

  it should "raise received error" in {
    val f = fixture
    val result = new JobResult(
      success = false,
      retval = None,
      error = Some(
        new WorkerError(
          kind = WorkerErrorKind.BODY_ERROR,
          inner = Some(
            new JobSystemError(
              message = "some error",
              cause = None
            )
          )
        )
      ),
      stats = None
    )
    val wid = new Wid(0, 0)

    f.startSchedulerFiber
      .use { case (stateR, mainFiberQueue, schedulerFiberQueue, rpcClientFiberQueues) =>
        for {
          _ <- f.initAll(stateR, mainFiberQueue, schedulerFiberQueue)

          // schedule four jobs, each scheduled to each workers
          _ <- schedulerFiberQueue.offer(new SchedulerFiberEvents.Jobs(jobSpecs :+ jobSpecE))
          a <- rpcClientFiberQueues(new Wid(0, 0)).take
          b <- rpcClientFiberQueues(new Wid(0, 1)).take
          c <- rpcClientFiberQueues(new Wid(1, 0)).take
          d <- rpcClientFiberQueues(new Wid(1, 1)).take

          // enqueue JobFailed event
          _ <- schedulerFiberQueue.offer(new SchedulerFiberEvents.JobFailed(result, wid))
          evt <- mainFiberQueue.take
        } yield {
          evt match {
            case MainFiberEvents.JobFailed(spec, result) => {
              spec should be(jobSpecA)
              result should be(result)
            }
            case _ => fail("evt is not Job")
          }
        }
      }
      .timeout(1.second)
  }

  // behavior of "SchedulerFiber (upon receiving WorkerNotResponding)"

  // it should "detect fault and reschedule job"

  behavior of "SchedulerFiber (upon receiving Complete)"

  it should "send complete event to worker RPC client fibers, wait for WorkerCompleted events, and finally send CompleteDone event to main fiber" in {
    val f = fixture

    f.startSchedulerFiber
      .use { case (stateR, mainFiberQueue, schedulerFiberQueue, rpcClientFiberQueues) =>
        for {
          _ <- f.initAll(stateR, mainFiberQueue, schedulerFiberQueue)

          // enqueue Complete event
          _ <- schedulerFiberQueue.offer(new SchedulerFiberEvents.Complete)
          evtA <- rpcClientFiberQueues(new Wid(0, 0)).take
          evtB <- rpcClientFiberQueues(new Wid(0, 1)).take
          evtC <- rpcClientFiberQueues(new Wid(1, 0)).take
          evtD <- rpcClientFiberQueues(new Wid(1, 1)).take
          _ <- IO {
            evtA should be(WorkerFiberEvents.Complete)
            evtB should be(WorkerFiberEvents.Complete)
            evtC should be(WorkerFiberEvents.Complete)
            evtD should be(WorkerFiberEvents.Complete)
          }

          // offer WorkerCompleted events back to scheduler fiber
          _ <- schedulerFiberQueue.offer(
            new SchedulerFiberEvents.WorkerCompleted(from = new Wid(0, 0))
          )
          _ <- schedulerFiberQueue.offer(
            new SchedulerFiberEvents.WorkerCompleted(from = new Wid(0, 1))
          )
          _ <- schedulerFiberQueue.offer(
            new SchedulerFiberEvents.WorkerCompleted(from = new Wid(1, 0))
          )
          _ <- schedulerFiberQueue.offer(
            new SchedulerFiberEvents.WorkerCompleted(from = new Wid(1, 1))
          )

          // wait for CompleteDone event
          evt <- mainFiberQueue.take
        } yield {
          evt should be(MainFiberEvents.CompleteDone)
        }
      }
      .timeout(1.second)
  }
}
