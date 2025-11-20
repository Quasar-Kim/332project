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

class WorkerRpcService(isBusy: Ref[IO, Boolean]) extends WorkerFs2Grpc[IO, Metadata] {
  val jobRunner = new JobRunner(
    Map(
      JobType.SAMPLING -> JobSampler.run
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
              success = false
              // TODO
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
  def start(port: Int): IO[Unit] = {
    for {
      busyFlag <- Ref.of[IO, Boolean](false)
      serviceDefinition = new WorkerRpcService(busyFlag)
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
