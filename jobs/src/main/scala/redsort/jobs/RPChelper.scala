package redsort.jobs

import cats.effect._
import io.grpc.Status
import io.grpc.StatusRuntimeException
import org.log4s._
import scala.concurrent.duration._

object RPChelper {
  private[this] val logger = new SourceLogger(getLogger)

  // NOTE: lazy evaluation (=>) is required - otherwise stack will overflow
  def handleRpcErrorWithRetry[T](retry: => IO[T])(err: Throwable): IO[T] =
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
