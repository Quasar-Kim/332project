package redsort.jobs.scheduler

import cats._
import cats.effect._
import cats.effect.std.Queue
import cats.syntax.all._
import redsort.jobs.Common._
import scala.concurrent.duration._
import redsort.jobs.messages.WorkerFs2Grpc
import io.grpc.Metadata
import redsort.jobs.context.interface.WorkerRpcClient
import redsort.jobs.scheduler
import org.log4s._
import redsort.jobs.SourceLogger
import com.google.protobuf.empty.Empty
import redsort.jobs.scheduler.WorkerFiberEvents.Initialized
import redsort.jobs.messages.JobSpecMsg
import io.grpc.StatusRuntimeException
import io.grpc.Status
import redsort.jobs.messages.JobResult
import org.scalatest.Assertion
import redsort.jobs.messages.WorkerError

object WorkerRpcClientFiber {
  private[this] val logger = new SourceLogger(getLogger, "scheduler")

  def start(
      stateR: Ref[IO, SharedState],
      wid: Wid,
      inputQueue: Queue[IO, WorkerFiberEvents],
      schedulerFiberQueue: Queue[IO, SchedulerFiberEvents],
      ctx: WorkerRpcClient
  ): Resource[IO, Unit] = {
    val io = for {
      _ <- logger.debug(s"RPC client fiber for $wid started, waiting for registration")

      // wait for worker registration event
      evt <- inputQueue.take

      // start rpc client
      _ <- evt match {
        case Initialized(addr) =>
          ctx
            .workerRpcClient(addr)
            .use(rpcClient => main(stateR, wid, inputQueue, schedulerFiberQueue, rpcClient))
        case _ => IO.raiseError(new RuntimeException("got event other than Initialized"))
      }
    } yield ()

    io.toResource
  }

  private def main(
      stateR: Ref[IO, SharedState],
      wid: Wid,
      queue: Queue[IO, WorkerFiberEvents],
      schedulerFiberQueue: Queue[IO, SchedulerFiberEvents],
      rpcClient: WorkerFs2Grpc[IO, Metadata]
  ): IO[Unit] = for {
    event <- queue.take
    _ <- logger.debug(s"received event $event")
    _ <- handleEvent(stateR, wid, event, schedulerFiberQueue, rpcClient)
    _ <- main(stateR, wid, queue, schedulerFiberQueue, rpcClient)
  } yield ()

  private def handleEvent(
      stateR: Ref[IO, SharedState],
      wid: Wid,
      event: WorkerFiberEvents,
      schedulerFiberQueue: Queue[IO, SchedulerFiberEvents],
      rpcClient: WorkerFs2Grpc[IO, Metadata]
  ): IO[Unit] = event match {
    case WorkerFiberEvents.Job(spec) =>
      for {
        _ <- logger.debug(s"got job spec $spec")
        result <- runJobWithRetry(rpcClient, JobSpec.toMsg(spec))
        _ <- schedulerFiberQueue.offer(
          if (result.success) new SchedulerFiberEvents.JobCompleted(result, wid)
          else new SchedulerFiberEvents.JobFailed(result, wid)
        )
      } yield ()

    case WorkerFiberEvents.Complete =>
      for {
        _ <- logger.debug(s"shutting down worker $wid")
        _ <- completeWithRetry(rpcClient)
        _ <- schedulerFiberQueue.offer(new SchedulerFiberEvents.WorkerCompleted(from = wid))
      } yield ()

    case _ => IO.raiseError(new RuntimeException(s"got unxpected event $event"))
  }

  private def runJobWithRetry(
      rpcClient: WorkerFs2Grpc[IO, Metadata],
      spec: JobSpecMsg
  ): IO[JobResult] = {
    rpcClient
      .runJob(spec, new Metadata)
      .handleErrorWith(handleRpcErrorWithRetry(runJobWithRetry(rpcClient, spec)))
  }

  private def completeWithRetry(rpcClient: WorkerFs2Grpc[IO, Metadata]): IO[Empty] =
    rpcClient
      .complete(new Empty, new Metadata)
      .handleErrorWith(handleRpcErrorWithRetry(completeWithRetry(rpcClient)))

  // NOTE: lazy evaluation (=>) is required - otherwise stack will overflow
  private def handleRpcErrorWithRetry[T](retry: => IO[T])(err: Throwable): IO[T] =
    err match {
      case err if isTransportError(err) =>
        for {
          _ <- logger.error(s"transport error: $err")
          _ <- logger.error(s"RPC call failed due to transport error, will retry in 1 seconds...")
          _ <- IO.sleep(1.second)
          result <- retry
        } yield result
      case err =>
        logger.error(s"RPC call raised fatal exception: $err") >> IO.raiseError[T](err)
    }

  private def isTransportError(err: Throwable): Boolean = err match {
    case e: StatusRuntimeException => e.getStatus.getCode == Status.Code.UNAVAILABLE
    case _                         => false
  }
}
