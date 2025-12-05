package redsort.jobs.worker.jobrunner

import cats._
import cats.effect._
import cats.syntax.all._
import redsort.jobs.Common._
import redsort.jobs.messages._

import redsort.jobs.scheduler.JobSpec
import redsort.jobs.worker.JobHandler
import fs2.io.file.Path
import redsort.jobs.worker.Directories
import redsort.jobs.context.interface.FileStorage
import redsort.jobs.JobSystemException
import com.google.protobuf.ByteString
import redsort.jobs.Unreachable
import redsort.jobs.SourceLogger
import monocle.syntax.all._
import io.grpc.Metadata
import redsort.jobs.workers.SharedState
import redsort.jobs.RPChelper
import cats.effect.std.Random

trait JobRunner {
  def addHandler(entry: Tuple2[String, JobHandler]): IO[JobRunner]
  def runJob(spec: JobSpecMsg): IO[JobResult]
  def getHandlers: Map[String, JobHandler]
}

final case class WorkerErrorWrapper(inner: WorkerError) extends Exception("worker error", null)

object JobRunner {
  def apply(
      handlers: Map[String, JobHandler],
      dirs: Directories,
      ctx: FileStorage,
      logger: SourceLogger,
      replicatorClient: ReplicatorLocalServiceFs2Grpc[IO, Metadata],
      stateR: Ref[IO, SharedState]
  ): IO[JobRunner] =
    IO.pure(new JobRunner {
      override def addHandler(entry: (String, JobHandler)): IO[JobRunner] =
        JobRunner(handlers + entry, dirs, ctx, logger, replicatorClient, stateR)

      override def runJob(spec: JobSpecMsg): IO[JobResult] =
        runJobInner(spec).handleErrorWith {
          case WorkerErrorWrapper(err) =>
            IO.pure(new JobResult(success = false, retval = None, error = Some(err), stats = None))
          case err => IO.raiseError(err)
        }

      def runJobInner(spec: JobSpecMsg): IO[JobResult] =
        for {
          wid <- stateR.get.flatMap(s =>
            s.wid match {
              case Some(value) => IO.pure(value)
              case None        => IO.raiseError(new AssertionError("wid is none"))
            }
          )
          _ <- logger.debug(s"$wid: got job spec: ${spec}")

          // prepare inputs and outputs
          _ <- logger.debug(s"$wid: preparing inputs for job ${spec.name}...")
          inputs <- prepareInputs(spec.inputs.map(FileEntry.fromMsg(_)))
          outputs <- prepareOuptputs(spec.outputs.map(FileEntry.fromMsg(_)))

          // get handler for job
          handler <- getHandlerOrRaise(handlers, spec.name).adaptError { case e: Exception =>
            errorToWorkerError(WorkerErrorKind.JOB_NOT_FOUND, e)
          }

          // run handler
          _ <- logger.debug(s"$wid: running handler for job ${spec.name}...")
          start <- IO.realTime
          retval <- handler(spec.args, inputs, outputs, ctx, dirs)
            .onError(e => logger.error(s"body raised error: $e"))
            .adaptError { case e: Exception =>
              errorToWorkerError(WorkerErrorKind.BODY_ERROR, e)
            }
          end <- IO.realTime
          _ <- logger.debug(
            s"$wid: handler for job ${spec.name} returned, took ${(end - start) / 1000}"
          )

          // job was successful, replicate all output files, find
          // their actual sizes, then create job result
          replicatedOutputs <- replicateOutputs(spec.outputs, ctx)
          outputs <- resolveFileSizes(replicatedOutputs, ctx)
        } yield {
          val ret = retval match {
            case Some(buf) => Some(ByteString.copyFrom(buf))
            case None      => None
          }
          new JobResult(
            success = true,
            retval = ret,
            error = None,
            stats = None,
            outputs = outputs
          )
        }

      def prepareInputs(inputs: Seq[FileEntry]): IO[Seq[Path]] = {
        for {
          mid <- stateR.get.map(s => s.wid.get.mid)
          _ <- pullMissingFiles(inputs, mid)
        } yield fileEntriesToPaths(inputs)
      }

      def pullMissingFiles(inputs: Seq[FileEntry], mid: Int): IO[Unit] =
        inputs
          .traverse { entry =>
            ctx.exists(Directories.resolvePath(dirs, Path(entry.path)).toString).flatMap {
              case true  => IO.unit
              case false => {
                val sources = entry.replicas.filter(_ != mid)
                val wrongReplicas = entry.replicas.contains(mid)
                IO.whenA(wrongReplicas)(
                  logger.warn(
                    s"file ${entry.path} does not exists even though `replicas` field contains worker's mid ($mid). This is expected if this machine was restarted due to machine fault."
                  )
                ) >>
                  tryPull(entry, sources)
              }
            }
          }
          .map(_ => IO.unit)

      def tryPull(entry: FileEntry, sources: Seq[Mid]): IO[Unit] = {
        sources match {
          case Seq() =>
            logger.error(s"failed to pull missing file $entry") >>
              IO.raiseError(
                WorkerErrorWrapper(
                  WorkerError(kind = WorkerErrorKind.INPUT_REPLICATION_ERROR, inner = None)
                )
              )
          case head +: tail => {
            val request = new PullRequest(path = entry.path, src = head)
            pullWithRetry(request).attempt.flatMap {
              case Left(err) => tryPull(entry, tail)
              case Right(_)  => IO.unit
            }
          }
        }
      }

      def prepareOuptputs(outputs: Seq[FileEntry]): IO[Seq[Path]] =
        IO.pure(fileEntriesToPaths(outputs))

      def fileEntriesToPaths(entries: Seq[FileEntry]): Seq[Path] =
        entries.map(entry => Directories.resolvePath(dirs, Path(entry.path)))

      def getHandlerOrRaise(handlers: Map[String, JobHandler], name: String) =
        for {
          exists <- IO.pure(handlers.exists(_._1 == name))
          _ <- IO.raiseUnless(exists)(new IllegalArgumentException(s"job $name does not exists"))
        } yield handlers.get(name).get

      def errorToWorkerError(kind: WorkerErrorKind, err: Throwable) = {
        val e = JobSystemException.fromThrowable(err)
        val workerError = new WorkerError(kind = kind, inner = Some(JobSystemException.toMsg(e)))
        new WorkerErrorWrapper(workerError)
      }

      def replicateOutputs(entries: Seq[FileEntryMsg], ctx: FileStorage): IO[Seq[FileEntryMsg]] =
        entries
          .traverse { entry =>
            // only replicate files in working directory
            if (entry.path.startsWith("@{working}")) {
              for {
                candidates <- replicationDstCandidates
                replicatedEntry <-
                  candidates match {
                    case List() => IO.pure(entry)

                    // If there are only two machines in the cluster and one machines goes down it is impossible to
                    // replciate output.
                    // Ignore error in this case.
                    case List(mid) =>
                      tryPush(entry, candidates).orElse(
                        logger.error(
                          s"suppressing push error because there are only two machines in cluster"
                        ) >> IO.pure(entry)
                      )

                    case _ => tryPush(entry, candidates)
                  }
              } yield replicatedEntry
            } else IO.pure(entry)
          }

      def replicationDstCandidates: IO[List[Mid]] =
        stateR.get.flatMap { s =>
          val candidates = s.replicatorAddrs.keys.filter(_ != s.wid.get.mid)
          Random[IO].shuffleList(candidates.toList)
        }

      def tryPush(entry: FileEntryMsg, dstCandidates: Seq[Mid]): IO[FileEntryMsg] =
        dstCandidates match {
          case Seq() =>
            logger.error(s"failed to push output file ${entry.path}") >>
              IO.raiseError(
                WorkerErrorWrapper(
                  WorkerError(kind = WorkerErrorKind.OUTPUT_REPLICATION_ERROR, inner = None)
                )
              )

          case mid +: nextCandidates => {
            val request = PushRequest(path = entry.path, dst = mid)
            pushWithRetry(request).attempt.flatMap {
              case Left(err) => tryPush(entry, nextCandidates)
              case Right(_)  =>
                logger.debug(s"pushed ${entry.path} to $mid") >> IO.pure(
                  entry.focus(_.replicas).modify(_.appended(mid))
                )
            }
          }
        }

      def resolveFileSizes(entries: Seq[FileEntryMsg], ctx: FileStorage): IO[Seq[FileEntryMsg]] =
        entries.traverse { entry =>
          val path = Directories.resolvePath(dirs, Path(entry.path))
          ctx.fileSize(path.toString).map(size => entry.focus(_.size).replace(size))
        }

      // NOTE: these methods will retry pull until local RPC service becomes available, and
      // will NOT retry on replication failure.
      private def pullWithRetry(request: PullRequest): IO[ReplicationResult] =
        replicatorClient
          .pull(request, new Metadata)
          .handleErrorWith(RPChelper.handleRpcErrorWithRetry(pullWithRetry(request)))

      private def pushWithRetry(request: PushRequest): IO[ReplicationResult] =
        replicatorClient
          .push(request, new Metadata)
          .handleErrorWith(RPChelper.handleRpcErrorWithRetry(pushWithRetry(request)))

      override def getHandlers: Map[String, JobHandler] = handlers
    })

  def init(
      handlers: Map[String, JobHandler],
      dirs: Directories,
      ctx: FileStorage,
      logger: SourceLogger,
      replicatorClient: ReplicatorLocalServiceFs2Grpc[IO, Metadata],
      stateR: Ref[IO, SharedState]
  ): IO[JobRunner] =
    for {
      _ <- Directories.ensureDirs(dirs, ctx)
      jobRunner <- JobRunner(handlers, dirs, ctx, logger, replicatorClient, stateR)
    } yield jobRunner
}
