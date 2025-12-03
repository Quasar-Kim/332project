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
import org.log4s._
import redsort.jobs.SourceLogger
import redsort.jobs.scheduler.SchedulerFiberEvents.Complete
import redsort.jobs.scheduler.SchedulerFiberEvents.WorkerCompleted
import redsort.jobs.messages.WorkerHello
import redsort.jobs.messages.NetAddrMsg
import scala.collection.immutable

/** Scheduler fiber.
  */
object SchedulerFiber {
  private[this] val logger = new SourceLogger(getLogger, "scheduler")

  abstract class WorkerRegistrationCases
  object WorkerRegistrationCases {
    final case class New(wid: Wid) extends WorkerRegistrationCases
    final case object Invalid extends WorkerRegistrationCases
    final case class Existing(wid: Wid) extends WorkerRegistrationCases
  }

  def start(
      stateR: Ref[IO, SharedState],
      mainFiberQueue: Queue[IO, MainFiberEvents],
      schedulerFiberQueue: Queue[IO, SchedulerFiberEvents],
      rpcClientFiberQueues: Map[Wid, Queue[IO, WorkerFiberEvents]],
      rpcServerFiberQueue: Queue[IO, RpcServerFiberEvents],
      scheduleLogic: ScheduleLogic
  ): Resource[IO, Unit] =
    logger.debug("scheduler fiber started").toResource >>
      main(
        stateR,
        mainFiberQueue,
        schedulerFiberQueue,
        rpcClientFiberQueues,
        rpcServerFiberQueue,
        scheduleLogic
      ).toResource

  private def main(
      stateR: Ref[IO, SharedState],
      mainFiberQueue: Queue[IO, MainFiberEvents],
      schedulerFiberQueue: Queue[IO, SchedulerFiberEvents],
      rpcClientFiberQueues: Map[Wid, Queue[IO, WorkerFiberEvents]],
      rpcServerFiberQueue: Queue[IO, RpcServerFiberEvents],
      scheduleLogic: ScheduleLogic
  ): IO[Unit] = for {
    evt <- schedulerFiberQueue.take
    _ <- logger.debug(s"got event $evt")
    _ <- handleEvent(
      evt,
      stateR,
      mainFiberQueue,
      rpcClientFiberQueues,
      rpcServerFiberQueue,
      scheduleLogic
    )
    _ <- main(
      stateR,
      mainFiberQueue,
      schedulerFiberQueue,
      rpcClientFiberQueues,
      rpcServerFiberQueue,
      scheduleLogic
    )
  } yield ()

  private def handleEvent(
      evt: SchedulerFiberEvents,
      stateR: Ref[IO, SharedState],
      mainFiberQueue: Queue[IO, MainFiberEvents],
      rpcClientFiberQueues: Map[Wid, Queue[IO, WorkerFiberEvents]],
      rpcServerFiberQueue: Queue[IO, RpcServerFiberEvents],
      scheduleLogic: ScheduleLogic
  ): IO[Unit] = {
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

    /** Check if all workers are initialized.
      */
    def allInitialized(state: SharedState): Boolean =
      state.schedulerFiber.workers.forall { case (_, state) => state.initialized }

    def updateFileEntries(state: SharedState): IO[SharedState] = {
      // Collect all completed jobs.
      val completedJobs = state.schedulerFiber.workers.foldLeft(Seq.empty[Job]) {
        case (acc, (wid, state)) =>
          acc ++ state.completedJobs.to(Seq)
      }
      val completedJobsMap = state.schedulerFiber.workers.view
        .filterKeys(_.wtid == 0)
        .map { case (wid, _) => (wid.mid, completedJobs) }
        .to(Map)

      // collect files to be deleted and added intemrediate files.
      val obsoleteFiles = completedJobsMap.map { case (mid, jobs) =>
        val files = jobs
          .map(
            _.spec.inputs
              .filter(entry => entry.path.startsWith("@{working}") && entry.replicas.contains(mid))
              .map(_.path)
          )
          .flatten
          .to(Seq)
        (mid, files)
      }
      val addedFileEntries = completedJobsMap.map { case (mid, jobs) =>
        val files = jobs
          .map(
            _.result.get.outputs
              .filter(_.replicas.contains(mid))
              .map(entryMsg => (entryMsg.path, FileEntry.fromMsg(entryMsg)))
          )
          .flatten
          .filter { case (path, _) =>
            !path.startsWith("@{working}/synced.")
          } // do not track "synced" file procued by __sync__ job
          .to(Map)
        (mid, files)
      }

      // apply changes to files
      val files =
        state.schedulerFiber.files
          .map { case (mid, entries) =>
            val deletions = obsoleteFiles(mid)
            val additions = addedFileEntries(mid)
            (mid, entries.removedAll(deletions).concat(additions))
          }
          .to(Map)

      // log debug informations
      val addedFiles = addedFileEntries.view.mapValues(_.keys.toSeq).toMap
      for {
        _ <- logger.debug(s"new files: ${addedFiles}")
        _ <- logger.debug(s"obsolete files: $obsoleteFiles")
      } yield state.focus(_.schedulerFiber.files).replace(files)
    }

    def emptyCompletedJobs(state: SharedState): SharedState = {
      state.focus(_.schedulerFiber.workers).modify { workerStates =>
        workerStates.view.mapValues { case workerState =>
          workerState.focus(_.completedJobs).replace(immutable.Queue.empty)
        }.toMap
      }
    }

    def jobResults(workerStates: Map[Wid, WorkerState]): Seq[Tuple2[JobSpec, JobResult]] =
      workerStates.foldLeft(Seq[Tuple2[JobSpec, JobResult]]()) { case (acc, (_, state)) =>
        acc ++ state.completedJobs.toSeq.map(job => (job.spec, job.result.get))
      }

    /** Given workerHello `hello`, either
      *   - find existing worker ID
      *   - allocate new worker ID
      *   - or do nothing if registration is invalid.
      */
    def findOrAllocateWid(
        workerStates: Map[Wid, WorkerState],
        hello: WorkerHello
    ): WorkerRegistrationCases = {
      val x = findExistingWid(workerStates, new NetAddr(hello.ip, hello.port))
      x match {
        case Some(wid) => new WorkerRegistrationCases.Existing(wid)
        case None      => {
          allocateWid(workerStates, hello.wtid) match {
            case Some(wid) => new WorkerRegistrationCases.New(wid)
            case None      => WorkerRegistrationCases.Invalid
          }
        }
      }
    }

    /** Find worker ID with given `addr` and return `Some(wid)` if it exists. Otherwise returns
      * `None.`
      */
    def findExistingWid(workerStates: Map[Wid, WorkerState], addr: NetAddr): Option[Wid] =
      workerStates
        .find { case (_, state) =>
          state.netAddr == Some(addr)
        }
        .map(_._1)

    /** Find first uninitialized worker with `wtid` and return `Some(wid)` if it exists. Otherwise
      * returns `None`.
      */
    def allocateWid(workerStates: Map[Wid, WorkerState], wtid: Wtid): Option[Wid] =
      workerStates
        .filter(_._1.wtid == wtid)
        .toSeq
        .sortBy(_._1.mid)
        .find { case (_, state) =>
          !state.initialized
        }
        .map(_._1)

    def updateSchedulerState(state: SharedState, schedulerState: SchedulerState): SharedState =
      state.focus(_.schedulerFiber.state).replace(schedulerState)

    def registerExistingWorker(
        state: SharedState,
        wid: Wid,
        hello: WorkerHello
    ): IO[SharedState] = {
      for {
        _ <- logger.debug(s"existing worker re-registration with wid $wid")
        updatedState <- IO.pure(initializeWorkerState(state, wid, hello))
        _ <- rpcServerFiberQueue.offer(
          new RpcServerFiberEvents.AllWorkersInitialized(
            getReplicatorAddresses(updatedState.schedulerFiber.workers)
          )
        )
      } yield updatedState
    }

    def handleInvalidRegistration(state: SharedState, hello: WorkerHello): IO[SharedState] = {
      for {
        _ <- logger.error(s"invalid worker registration request: $hello")
      } yield state
    }

    def initializeWorkerState(
        sharedState: SharedState,
        wid: Wid,
        hello: WorkerHello
    ): SharedState = {
      // update worker state
      val workerState = sharedState.schedulerFiber.workers(wid)
      val updatedWorkerState = workerState
        .focus(_.netAddr)
        .replace(Some(new NetAddr(hello.ip, hello.port)))
        .focus(_.status)
        .replace(WorkerStatus.Up)
        .focus(_.initialized)
        .replace(true)
      val updatedWorkerStates = sharedState.schedulerFiber.workers.updated(wid, updatedWorkerState)

      // also update file entries
      val fileEntries = hello.storageInfo match {
        case Some(info) =>
          info.entries.view
            .mapValues(
              FileEntry.fromMsg(_).focus(_.replicas).replace(Seq(wid.mid))
            ) // use mid as only replica of input files.
            .to(Map)
        case None => sharedState.schedulerFiber.files.getOrElse(wid.mid, Map())
      }
      val updatedFiles = sharedState.schedulerFiber.files.updated(wid.mid, fileEntries)

      sharedState
        .focus(_.schedulerFiber.workers)
        .replace(updatedWorkerStates)
        .focus(_.schedulerFiber.files)
        .replace(updatedFiles)
    }

    def getReplicatorAddresses(workerStates: Map[Wid, WorkerState]): Map[Mid, NetAddr] =
      workerStates.view
        .mapValues(_.netAddr.get)
        .filter { case (wid, _) => wid.wtid == 0 }
        .map { case (wid, netAddr) =>
          (wid.mid, new NetAddr(netAddr.ip, netAddr.port - 1))
        }
        .toMap

    def registerNewWorker(state: SharedState, wid: Wid, hello: WorkerHello): IO[SharedState] = {
      for {
        updatedState <- IO.pure(initializeWorkerState(state, wid, hello))

        /*
        If this registration makes all N workers registered, emit:
          - `Initialized` event to main fiber
          - `AllWorkersInitialized` events to RPC server fiber (N times)
          - `Initialized` events to all N worker RPC client fibers.
         */
        updatedState <-
          if (allInitialized(updatedState))
            for {
              s <- stateR.updateAndGet(_ => updateSchedulerState(updatedState, SchedulerState.Idle))
              s <- emitAllRegisteredEvents(s)
            } yield s
          else IO.pure(updatedState)
        _ <- logger.debug(s"new worker registration with wid $wid")
      } yield updatedState
    }

    def emitAllRegisteredEvents(state: SharedState): IO[SharedState] = {
      for {
        _ <- mainFiberQueue
          .offer(new MainFiberEvents.Initialized(state.schedulerFiber.files))
        _ <- (0 until state.schedulerFiber.workers.size).toList.traverse { _ =>
          rpcServerFiberQueue.offer(
            new RpcServerFiberEvents.AllWorkersInitialized(
              getReplicatorAddresses(state.schedulerFiber.workers)
            )
          )
        }
        _ <- rpcClientFiberQueues.toList.traverse { case (wid, queue) =>
          val netAddr = state.schedulerFiber.workers(wid).netAddr.get
          queue.offer(
            new WorkerFiberEvents.Initialized(netAddr)
          )
        }
      } yield state
    }

    evt match {

      /*
      There are three possible cases when `WorkerRegistration` happens:
        1. New worker registers
          a) There is a remaining slot for the worker - `WorkerRegistrationCases.New`
          b) No slot for the worker (all workers are already registered) - `WorkerRegistrationCases.Invalid`

        2. Existing worker registers again - `WorkerRegistrationCases.Existing`

        In case 1-a) and 2, scheduler resolves worker ID for the worker and
        initialize its state according to workerHello from the worker.

        In case 1-b), scheduler rejects registration by sending back "bad schedulerHello"
        (schedulerHello message with `success` field set to false).

        `SchedulerFiber` sends `AllWorkersInitialized` in either case 1-b) and 2.
        `RpcServerFiber` will send back appropriate schedulerHello for each case.
       */
      case WorkerRegistration(hello) =>
        for {
          state <- stateR.get
          state <- findOrAllocateWid(state.schedulerFiber.workers, hello) match {
            case WorkerRegistrationCases.New(wid)      => registerNewWorker(state, wid, hello)
            case WorkerRegistrationCases.Existing(wid) => registerExistingWorker(state, wid, hello)
            case WorkerRegistrationCases.Invalid       => handleInvalidRegistration(state, hello)
          }
          _ <- stateR.set(state)
        } yield ()

      case Halt(errMsg, from) =>
        for {
          exception <- IO.pure(JobSystemException.fromMsg(errMsg, from.toString()))
          _ <- logger.error(s"received halt event from $from: $errMsg")
          _ <- mainFiberQueue.offer(
            new MainFiberEvents.SystemException(exception)
          )
        } yield ()

      case FatalError(error) =>
        for {
          - <- logger.error(s"received fatal error event: $error")
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
              _ <- logger.info(s"received ${specs.length} jobs")

              // change state to Running.
              workerStates <- stateR
                .updateAndGet(s => s.focus(_.schedulerFiber.state).replace(SchedulerState.Running))
                .map(_.schedulerFiber.workers)

              // schedule and submit jobs to each workers.
              workerStates <- IO(
                scheduleLogic.schedule(workerStates, specs)
              ) // this is IO (not IO.pure) to allow logging inside scheduleLogic
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
              // move running job to completed job, chaning its state
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
              _ <- logger.info(
                s"$from completed job. pending: ${updatedState.schedulerFiber.workers(from).pendingJobs.length}, completed: ${updatedState.schedulerFiber.workers(from).completedJobs.length}"
              )

              // if all jobs are completed,
              //   - update file entries of each machine.
              //   - send JobCompleted event to main fiber.
              //   - empty completed job list.
              // otherwise submit another job into workers.
              done <- IO.pure(updatedState.schedulerFiber.workers.forall { case (_, state) =>
                state.pendingJobs.isEmpty && state.runningJob.isEmpty
              })
              _ <-
                if (done) {
                  for {
                    _ <- logger.info("all jobs completed")
                    updatedState <- updateFileEntries(updatedState)
                    _ <- stateR.set(updatedState)
                    _ <- mainFiberQueue.offer(
                      new MainFiberEvents.JobCompleted(
                        jobResults(updatedState.schedulerFiber.workers),
                        updatedState.schedulerFiber.files
                      )
                    )
                    updatedState <- IO.pure(emptyCompletedJobs(updatedState))
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
          _ <- logger.error(s"$from failed to run job, reason: ${result.error}")
          spec <- stateR.get.map(_.schedulerFiber.workers(from).runningJob.get.spec)
          _ <- mainFiberQueue.offer(new MainFiberEvents.JobFailed(spec = spec, result = result))
        } yield ()
      }

      case Complete() =>
        for {
          _ <- logger.info("shutting down cluster...")

          // send Complete events to all worker fibers
          _ <- rpcClientFiberQueues.values.toList.traverse { queue =>
            queue.offer(WorkerFiberEvents.Complete)
          }
        } yield ()

      case WorkerCompleted(from) =>
        for {
          _ <- logger.debug(s"worker $from shutted down")

          // record worker as completed
          state <- stateR.updateAndGet { s =>
            s.focus(_.schedulerFiber.workers).at(from).modify {
              case Some(ws) => Some(ws.focus(_.completed).replace(true))
              case None     => None
            }
          }

          // if all workers are completed, then send CopmleteDone event to main fiber
          _ <- IO.whenA(state.schedulerFiber.workers.forall(_._2.completed))(
            mainFiberQueue.offer(MainFiberEvents.CompleteDone)
          )
        } yield ()

      case _ => IO.raiseError(new NotImplementedError)
    }
  }
}
