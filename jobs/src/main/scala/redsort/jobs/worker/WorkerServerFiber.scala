package redsort.jobs.worker

import cats._
import cats.effect._
import cats.syntax._
import cats.syntax.all._
import redsort.jobs.Common._

import io.grpc._
import fs2.grpc.syntax.all._
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
import redsort.jobs.messages._
import com.google.protobuf.empty.Empty

import redsort.jobs.worker.jobrunner._
import redsort.jobs.worker.filestorage.{FileStorage, AppContext}

class WorkerRpcService(isBusy: Ref[IO, Boolean], fileStorage: FileStorage[AppContext])
    extends WorkerFs2Grpc[IO, Metadata] {
  val jobRunner = new JobRunner(
    Map(
      JobType.SAMPLING -> new JobSampler(fileStorage).run,
      JobType.SORTING -> new JobSorter(fileStorage).run,
      JobType.PARTITIONING -> new JobPartitioner(fileStorage).run,
      JobType.MERGING -> new JobMerger(fileStorage).run
    )
  )

  override def runJob(request: JobSpecMsg, ctx: Metadata): IO[JobResult] = {
    println(s"[WorkerRpcService] Received job of type ${request.jobType}")
    isBusy
      .modify {
        case true  => (true, false)
        case false => (true, true)
      }
      .flatMap { canProceed =>
        if (canProceed) {
          for {
            result <- jobRunner.runJob(request).guarantee(isBusy.set(false))
          } yield result
        } else {
          IO.pure(
            JobResult(
              success = false,
              retval = None,
              error = Some(
                WorkerError(
                  kind = WorkerErrorKind.WORKER_BUSY,
                  inner = Some(
                    JobSystemError(
                      message = s"Worker is currently busy processing another job."
                    )
                  )
                )
              ),
              stats = None
            )
          )
        }
      }
  }

  override def halt(request: WorkerHaltRequest, ctx: Metadata): IO[Empty] = {
    println(s"[WorkerRpcService] Received halt request")
    IO.never
    // TODO
  }

}

object WorkerServerFiber {
  def start(port: Int, ctx: AppContext = AppContext.Production): IO[Unit] = {
    for {
      busyFlag <- Ref.of[IO, Boolean](false)
      fileStorage <- FileStorage.create(ctx)
      serviceDefinition = new WorkerRpcService(busyFlag, fileStorage)
      _ <- WorkerFs2Grpc
        .bindServiceResource[IO](serviceDefinition)
        .use { service =>
          NettyServerBuilder
            .forPort(port)
            .addService(service)
            .resource[IO]
            .evalMap(server => IO(server.start()))
            .useForever
        }
    } yield ()
  }
}
