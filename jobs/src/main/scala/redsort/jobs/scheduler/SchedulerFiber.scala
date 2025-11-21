package redsort.jobs.scheduler

import cats._
import cats.effect._
import cats.syntax.all._
import scala.concurrent.duration._
import redsort.jobs.Common._
import cats.effect.std.Queue
import monocle.syntax.all._
import redsort.jobs.scheduler.SchedulerFiberEvents.WorkerRegistration
import redsort.jobs.scheduler.SchedulerFiberEvents.Halt
import redsort.jobs.JobSystemException
import redsort.jobs.scheduler.SchedulerFiberEvents.FatalError
import redsort.jobs.scheduler.SchedulerFiberEvents.Jobs
import redsort.jobs.Unreachable
import redsort.jobs.scheduler.SchedulerFiberEvents.JobCompleted
import redsort.jobs.messages.JobResult
import redsort.jobs.scheduler.SchedulerFiberEvents.JobFailed

/** Scheduler fiber.
  */
object SchedulerFiber {
  def start(
      stateR: Ref[IO, SharedState],
      mainFiberQueue: Queue[IO, MainFiberEvents],
      schedulerFiberQueue: Queue[IO, SchedulerFiberEvents],
      rpcClientFiberQueues: Map[Wid, Queue[IO, WorkerFiberEvents]],
      scheduleLogic: ScheduleLogic
  ): Resource[IO, Unit] =
    main(
      stateR,
      mainFiberQueue,
      schedulerFiberQueue,
      rpcClientFiberQueues,
      scheduleLogic
    ).background.evalMap(_ => IO.unit)

  private def main(
      stateR: Ref[IO, SharedState],
      mainFiberQueue: Queue[IO, MainFiberEvents],
      schedulerFiberQueue: Queue[IO, SchedulerFiberEvents],
      rpcClientFiberQueues: Map[Wid, Queue[IO, WorkerFiberEvents]],
      scheduleLogic: ScheduleLogic
  ): IO[Unit] = for {
    evt <- schedulerFiberQueue.take
    _ <- handleEvent(evt, stateR, mainFiberQueue, rpcClientFiberQueues, scheduleLogic)
    _ <- main(stateR, mainFiberQueue, schedulerFiberQueue, rpcClientFiberQueues, scheduleLogic)
  } yield ()

  private def handleEvent(
      evt: SchedulerFiberEvents,
      stateR: Ref[IO, SharedState],
      mainFiberQueue: Queue[IO, MainFiberEvents],
      rpcClientFiberQueues: Map[Wid, Queue[IO, WorkerFiberEvents]],
      scheduleLogic: ScheduleLogic
  ): IO[Unit] = evt match {
    case WorkerRegistration(hello, from) =>
      for {
        workerStates <- stateR.get.map(s => s.schedulerFiber.workers)
        _ <- assertIO(workerStates(from).initialized)

        // update worker state with initalized field set to true
        // and status field to UP.
        workerStates <- stateR
          .updateAndGet { state =>
            state.focus(_.schedulerFiber.workers).at(from).modify {
              case Some(s) => {
                Some(
                  s.focus(_.initialized)
                    .replace(true)
                    .focus(_.status)
                    .replace(WorkerStatus.Up)
                )
              }
              case None => None
            }
          }
          .map(s => s.schedulerFiber.workers)

        // if all workers are initialized, emit Initialized event to main fiber,
        // and change state to Idle.
        _ <- IO.whenA(workerStates.forall { case (wid, state) => state.initialized })(
          stateR.update { s =>
            s.focus(_.schedulerFiber.state).replace(SchedulerState.Idle)
          } >> mainFiberQueue.offer(MainFiberEvents.Initialized)
        )
      } yield ()

    case Halt(errMsg, from) =>
      for {
        exception <- IO.pure(JobSystemException.fromMsg(errMsg, from.toString()))
        _ <- mainFiberQueue.offer(
          new MainFiberEvents.SystemException(exception)
        )
      } yield ()

    case FatalError(error) =>
      for {
        _ <- mainFiberQueue.offer(
          new MainFiberEvents.SystemException(error)
        )
      } yield ()

    case Jobs(specs) =>
      stateR.get.flatMap { state =>
        if (state.schedulerFiber.state != SchedulerState.Idle) {
          mainFiberQueue.offer(
            new MainFiberEvents.SystemException(
              new IllegalStateException(
                s"Scheduler cannot accept jobs in state other than `Idle`. Current state: ${state.schedulerFiber.state}"
              )
            )
          )
        } else {
          for {
            // change state to Running.
            workerStates <- stateR
              .updateAndGet(s => s.focus(_.schedulerFiber.state).replace(SchedulerState.Running))
              .map(_.schedulerFiber.workers)

            // schedule and submit jobs to each workers.
            workerStates <- scheduleLogic.schedule(workerStates, specs)
            workerStates <- submitJob(workerStates, rpcClientFiberQueues)

            // update worker states.
            _ <- stateR.update(s =>
              s
                .focus(_.schedulerFiber.workers)
                .replace(workerStates)
            )
          } yield ()
        }
      }

    case JobCompleted(result, from) =>
      stateR.get.flatMap { state =>
        if (state.schedulerFiber.state != SchedulerState.Running) {
          mainFiberQueue.offer(
            new MainFiberEvents.SystemException(
              new IllegalStateException(
                s"Scheduler got `JobCompleted` event which is only valid in state `Runninig`. Current state: ${state.schedulerFiber.state}"
              )
            )
          )
        } else {
          for {
            // move running job to completed job
            updatedState <- IO.pure({
              val workerState = state.schedulerFiber.workers(from)
              workerState.runningJob match {
                case Some(job) => {
                  val updatedJob = job
                    .focus(_.state)
                    .replace(JobState.Completed)
                    .focus(_.result)
                    .replace(Some(result))
                  val updatedWorkerState = workerState
                    .focus(_.runningJob)
                    .replace(None)
                    .focus(_.completedJobs)
                    .modify { q => q.enqueue(updatedJob) }
                  state.focus(_.schedulerFiber.workers).at(from).replace(Some(updatedWorkerState))
                }
                case None => throw new Unreachable
              }
            })

            // if all jobs are completed, send JobCompleted event to main fiber.
            // otherwise submit another job into workers.
            done <- IO.pure(updatedState.schedulerFiber.workers.forall { case (_, state) =>
              state.pendingJobs.isEmpty && state.runningJob.isEmpty
            })
            _ <-
              if (done) {
                for {
                  _ <- mainFiberQueue.offer(
                    new MainFiberEvents.JobCompleted(
                      jobResults(updatedState.schedulerFiber.workers)
                    )
                  )
                  _ <- stateR.set(
                    updatedState
                      .focus(_.schedulerFiber.state)
                      .replace(SchedulerState.Idle)
                  )
                } yield ()
              } else {
                for {
                  workerStates <- submitJob(
                    updatedState.schedulerFiber.workers,
                    rpcClientFiberQueues
                  )
                  _ <- stateR.set(
                    updatedState.focus(_.schedulerFiber.workers).replace(workerStates)
                  )
                } yield ()
              }
          } yield ()
        }
      }

    case JobFailed(result, from) => {
      for {
        spec <- stateR.get.map(_.schedulerFiber.workers(from).runningJob.get.spec)
        _ <- mainFiberQueue.offer(new MainFiberEvents.JobFailed(spec = spec, result = result))
      } yield ()
    }

    case _ => IO.raiseError(new NotImplementedError)
  }

  def submitJob(
      workerStates: Map[Wid, WorkerState],
      rpcClientFiberQueues: Map[Wid, Queue[IO, WorkerFiberEvents]]
  ): IO[Map[Wid, WorkerState]] = {
    val candidates = workerStates.filter { case (_, state) =>
      state.runningJob.isEmpty && !state.pendingJobs.isEmpty && state.status == WorkerStatus.Up
    }

    candidates.toList
      .traverse { case (wid, state) =>
        // dequeue one elemenet from pending job and put it to running job,
        // chaning its state to RUNNING.
        val (job, updatedPendingJobs) = state.pendingJobs.dequeue
        val updatedJob = job.focus(_.state).replace(JobState.Running)
        val updatedStates = state
          .focus(_.runningJob)
          .replace(Some(updatedJob))
          .focus(_.pendingJobs)
          .replace(updatedPendingJobs)

        val enqueueEffect = rpcClientFiberQueues.get(wid) match {
          case Some(queue) => queue.offer(new WorkerFiberEvents.Job(updatedJob.spec))
          case None        => IO.raiseError(new Unreachable)
        }
        enqueueEffect.as((wid, updatedStates))
      }
      .map { workerStatesUpdates =>
        workerStates ++ workerStatesUpdates
      }
  }

  def jobResults(workerStates: Map[Wid, WorkerState]): Seq[Tuple2[JobSpec, JobResult]] =
    workerStates.foldLeft(Seq[Tuple2[JobSpec, JobResult]]()) { case (acc, (_, state)) =>
      acc ++ state.completedJobs.toSeq.map(job => (job.spec, job.result.get))
    }
}
