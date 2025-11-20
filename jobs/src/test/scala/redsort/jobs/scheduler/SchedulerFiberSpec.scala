package redsort.jobs.scheduler

import redsort.FlatSpecBase
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

class SchedulerFiberSpec extends FlatSpecBase {
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
      ): IO[Map[Wid, WorkerState]] = {
        val updatedStates = for {
          ((wid, state), i) <- workerStates.zipWithIndex
        } yield {
          val job = new Job(
            state = JobState.Pending,
            ttl = 0,
            spec = specs(i)
          )
          val updatedState =
            state.focus(_.pendingJobs).modify(q => q.enqueue(job))
          (wid, updatedState)
        }
        IO.pure(updatedStates.toMap)
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
              storageInfo = None,
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
              storageInfo = None,
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
      assume(evt == MainFiberEvents.Initialized)
    }
  }

  val wid = new Wid(1, 0)

  val jobSpecs = Seq(
    new JobSpec(name = "a", args = Seq(), inputs = Seq(), outputs = Seq()),
    new JobSpec(name = "b", args = Seq(), inputs = Seq(), outputs = Seq()),
    new JobSpec(name = "c", args = Seq(), inputs = Seq(), outputs = Seq()),
    new JobSpec(name = "d", args = Seq(), inputs = Seq(), outputs = Seq())
  )

  behavior of "SchedulerFiber (upon receiving WorkerRegistration)"

  it should "initialize worker state if not initialized" in {
    val f = fixture
    val workerHello = new WorkerHello(
      wtid = 0,
      storageInfo = None,
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
            case MainFiberEvents.JobFailed(JobSystemException(message, source, cause)) =>
              cause should be(None.orNull)
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
          inside(evt) { case MainFiberEvents.JobFailed(e) =>
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
          evt shouldBe a[MainFiberEvents.JobFailed]
        }
      }
      .timeout(1.second)
  }

  behavior of "SchedulerFiber (upon receiving JobCompleted)"

  it should "run another job if there are remaining jobs"

  it should "return job specs to caller and change state to Idle if all jobs are completed"

  behavior of "SchedulerFiber (upon receiving JobFailed)"

  it should "raise received error"

  // behavior of "SchedulerFiber (upon receiving WorkerNotResponding)"

  // it should "detect fault and reschedule job"

}
