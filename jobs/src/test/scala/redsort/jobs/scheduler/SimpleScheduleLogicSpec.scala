package redsort.jobs.scheduler

import redsort.jobs.scheduler.SimpleScheduleLogic
import redsort.SyncSpec
import redsort.jobs.Common._
import monocle.syntax.all._
import redsort.jobs.Unreachable

class SimpleScheduleLogicSpec extends SyncSpec {
  val workerAddrs = Map(
    new Wid(0, 0) -> new NetAddr("1.1.1.1", 5000),
    new Wid(0, 1) -> new NetAddr("1.1.1.1", 5001),
    new Wid(1, 0) -> new NetAddr("1.1.1.2", 5000),
    new Wid(1, 1) -> new NetAddr("1.1.1.2", 5001)
  )

  val workerState = SchedulerFiberState
    .init(workerAddrs)
    .workers
    .view
    .mapValues(_.focus(_.initialized).replace(true).focus(_.status).replace(WorkerStatus.Up))
    .to(Map)

  def schedule(specs: Seq[JobSpec]): Map[Wid, Seq[JobSpec]] =
    SimpleScheduleLogic
      .schedule(workerState, specs)
      .view
      .mapValues(_.pendingJobs.toSeq.map(_.spec))
      .to(Map)

  behavior of "SimpleScheduleLogic"

  it should "schedule job with one output file to worker on which output file will be placed" in {
    val jobA = new JobSpec(
      name = "job",
      args = Seq(),
      inputs = Seq(),
      outputs = Seq(new FileEntry(path = "@{working}/a.out", size = 1024, replicas = Seq(1)))
    )
    val jobB = new JobSpec(
      name = "job",
      args = Seq(),
      inputs = Seq(),
      outputs = Seq(new FileEntry(path = "@{working}/b.out", size = 1024, replicas = Seq(0)))
    )
    val jobC = new JobSpec(
      name = "job",
      args = Seq(),
      inputs = Seq(),
      outputs = Seq(new FileEntry(path = "@{working}/c.out", size = 1024, replicas = Seq(1)))
    )
    val jobD = new JobSpec(
      name = "job",
      args = Seq(),
      inputs = Seq(),
      outputs = Seq(new FileEntry(path = "@{working}/d.out", size = 1024, replicas = Seq(0)))
    )

    val result = schedule(Seq(jobA, jobB, jobC, jobD))
    val expected = Map(
      new Wid(0, 0) -> Seq(jobB),
      new Wid(0, 1) -> Seq(jobD),
      new Wid(1, 0) -> Seq(jobA),
      new Wid(1, 1) -> Seq(jobC)
    )
    result should equal(expected)
  }

  it should "schedule job with no output file and one input file to worker on which input file is placed" in {
    val jobA = new JobSpec(
      name = "job",
      args = Seq(),
      inputs = Seq(new FileEntry(path = "@{input}/a", size = 1024, replicas = Seq(1))),
      outputs = Seq()
    )
    val jobB = new JobSpec(
      name = "job",
      args = Seq(),
      inputs = Seq(new FileEntry(path = "@{input}/b", size = 1024, replicas = Seq(0))),
      outputs = Seq()
    )
    val jobC = new JobSpec(
      name = "job",
      args = Seq(),
      inputs = Seq(new FileEntry(path = "@{input}/c", size = 1024, replicas = Seq(1))),
      outputs = Seq()
    )
    val jobD = new JobSpec(
      name = "job",
      args = Seq(),
      inputs = Seq(new FileEntry(path = "@{input}/d", size = 1024, replicas = Seq(0))),
      outputs = Seq()
    )

    val result = schedule(Seq(jobA, jobB, jobC, jobD))
    val expected = Map(
      new Wid(0, 0) -> Seq(jobB),
      new Wid(0, 1) -> Seq(jobD),
      new Wid(1, 0) -> Seq(jobA),
      new Wid(1, 1) -> Seq(jobC)
    )
    result should equal(expected)
  }

  it should "schedule job with no output and input file to worker with least job" in {
    val jobSpec = new JobSpec(
      name = "job",
      args = Seq(),
      inputs = Seq(),
      outputs = Seq()
    )
    val job = new Job(
      state = JobState.Pending,
      ttl = 0,
      spec = jobSpec,
      result = None
    )

    // fill job to workers except (1, 1)
    val updatedWorkerState = workerState
      .updatedWith(new Wid(0, 0)) {
        case Some(state) =>
          Some(state.focus(_.pendingJobs).modify { queue => queue.enqueue(job) })
        case None => throw new Unreachable
      }
      .updatedWith(new Wid(0, 1)) {
        case Some(state) =>
          Some(state.focus(_.pendingJobs).modify { queue => queue.enqueue(job) })
        case None => throw new Unreachable
      }
      .updatedWith(new Wid(1, 0)) {
        case Some(state) =>
          Some(state.focus(_.pendingJobs).modify { queue => queue.enqueue(job) })
        case None => throw new Unreachable
      }

    val result = SimpleScheduleLogic
      .schedule(updatedWorkerState, Seq(jobSpec))
      .view
      .mapValues(_.pendingJobs.toSeq.map(_.spec))
      .to(Map)
    val expected = Map(
      new Wid(0, 0) -> Seq(jobSpec),
      new Wid(0, 1) -> Seq(jobSpec),
      new Wid(1, 0) -> Seq(jobSpec),
      new Wid(1, 1) -> Seq(jobSpec)
    )
    result should equal(expected)
  }
}
