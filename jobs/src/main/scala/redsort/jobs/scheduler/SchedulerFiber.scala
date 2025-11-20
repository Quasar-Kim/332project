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
      mainFiberQueue.offer(
        new MainFiberEvents.JobFailed(JobSystemException.fromMsg(errMsg, from.toString()))
      )

    case FatalError(error) =>
      mainFiberQueue.offer(
        new MainFiberEvents.JobFailed(error)
      )

    case Jobs(specs) =>
      stateR.get.flatMap { state =>
        if (state.schedulerFiber.state != SchedulerState.Idle) {
          for {
            _ <- mainFiberQueue.offer(
              new MainFiberEvents.JobFailed(
                new IllegalStateException(
                  s"Scheduler cannot accept jobs in state other than `Idle`. Current state: ${state.schedulerFiber.state}"
                )
              )
            )
          } yield ()
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

    case _ => IO.raiseError(new NotImplementedError)
  }

  def submitJob(
      workerStates: Map[Wid, WorkerState],
      rpcClientFiberQueues: Map[Wid, Queue[IO, WorkerFiberEvents]]
  ): IO[Map[Wid, WorkerState]] = {
    val candidates = workerStates.filter { case (_, state) =>
      state.runningJobs.isEmpty && !state.pendingJobs.isEmpty && state.status == WorkerStatus.Up
    }

    candidates.toList
      .traverse { case (wid, state) =>
        // dequeue one elemenet from pending job and put it to running job,
        // chaning its state to RUNNING.
        val (job, updatedPendingJobs) = state.pendingJobs.dequeue
        val updatedJob = job.focus(_.state).replace(JobState.Running)
        val updatedStates = state
          .focus(_.runningJobs)
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
}
