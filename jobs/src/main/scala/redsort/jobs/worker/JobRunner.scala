package redsort.jobs.worker.jobrunner

import cats._
import cats.effect._
import cats.syntax._
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
      logger: SourceLogger
  ): IO[JobRunner] =
    IO.pure(new JobRunner {
      override def addHandler(entry: (String, JobHandler)): IO[JobRunner] =
        JobRunner(handlers + entry, dirs, ctx, logger)

      override def runJob(spec: JobSpecMsg): IO[JobResult] =
        runJobInner(spec).handleErrorWith {
          case WorkerErrorWrapper(err) =>
            IO.pure(new JobResult(success = false, retval = None, error = Some(err), stats = None))
          case _ => unreachableIO
        }

      def runJobInner(spec: JobSpecMsg): IO[JobResult] =
        for {
          inputs <- prepareInputs(spec.inputs.map(FileEntry.fromMsg(_)))
          outputs <- prepareOuptputs(spec.outputs.map(FileEntry.fromMsg(_)))
          handler <- getHandlerOrRaise(handlers, spec.name).adaptError { case e: Exception =>
            errorToWorkerError(WorkerErrorKind.JOB_NOT_FOUND, e)
          }
          retval <- handler(spec.args, inputs, outputs, ctx, dirs).adaptError { case e: Exception =>
            errorToWorkerError(WorkerErrorKind.BODY_ERROR, e)
          }
        } yield {
          val ret = retval match {
            case Some(buf) => Some(ByteString.copyFrom(buf))
            case None      => None
          }
          new JobResult(
            success = true,
            retval = ret,
            error = None,
            stats = None
          )
        }

      // later this method will replicate input files
      def prepareInputs(inputs: Seq[FileEntry]): IO[Seq[Path]] =
        IO.pure(fileEntriesToPaths(inputs))

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

      override def getHandlers: Map[String, JobHandler] = handlers
    })

  def init(
      handlers: Map[String, JobHandler],
      dirs: Directories,
      ctx: FileStorage,
      logger: SourceLogger
  ): IO[JobRunner] =
    for {
      _ <- Directories.ensureDirs(dirs, ctx)
      jobRunner <- JobRunner(handlers, dirs, ctx, logger)
    } yield jobRunner
}
