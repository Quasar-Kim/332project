package redsort.jobs.worker

import cats._
import cats.effect._
import cats.syntax.all._
import redsort.jobs.Common._

import io.grpc._
import fs2.grpc.syntax.all._
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
import redsort.jobs.messages._
import com.google.protobuf.empty.Empty

import redsort.jobs.worker.jobrunner._
import org.log4s._
import redsort.jobs.workers.SharedState
import redsort.jobs.context.WorkerCtx
import redsort.jobs.scheduler.JobSpec
import monocle.syntax.all._
import redsort.jobs.context.interface.FileStorage
import redsort.jobs.JobSystemException

object WorkerRpcService {
  private[this] val logger = getLogger

  def init(
      stateR: Ref[IO, SharedState],
      jobRunner: JobRunner
  ): WorkerFs2Grpc[IO, Metadata] =
    new WorkerFs2Grpc[IO, Metadata] {
      override def runJob(request: JobSpecMsg, ctx: Metadata): IO[JobResult] =
        for {
          spec <- IO.pure(JobSpec.fromMsg(request))
          _ <- IO(logger.info(s"received job of type ${spec.name}"))
          result <- tryRunJob(spec)
        } yield result

      def tryRunJob(spec: JobSpec): IO[JobResult] =
        for {
          state <- stateR.get
          _ <- IO.raiseWhen(state.runningJob)(new IllegalStateException("worker is busy"))
          result <- jobRunner
            .runJob(spec)
            .guarantee(stateR.set(state.focus(_.runningJob).replace(false)))
        } yield result

      // TODO
      override def halt(request: JobSystemError, ctx: Metadata): IO[Empty] =
        IO.raiseError(JobSystemException.fromMsg(request))
    }
}

object WorkerServerFiber {
  private[this] val logger = getLogger

  def start(
      stateR: Ref[IO, SharedState],
      port: Int,
      handlers: Map[String, JobHandler],
      dirs: Directories,
      ctx: WorkerCtx
  ): Resource[IO, Server] =
    for {
      _ <- IO(logger.debug("worker RPC server started")).toResource
      jobRunner <- JobRunner.init(handlers = handlers, dirs = dirs, ctx = ctx).toResource
      server <- ctx
        .workerRpcServer(
          WorkerRpcService.init(stateR = stateR, jobRunner = jobRunner),
          port
        )
        .evalMap(server => IO(server.start()))
    } yield server
}
