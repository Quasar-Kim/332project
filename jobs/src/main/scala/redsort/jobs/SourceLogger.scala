package redsort.jobs

import cats.effect._
import cats.syntax.all._
import org.slf4j.MDC
import org.log4s.Logger
import java.util.concurrent.atomic.AtomicReference

class SourceLogger(underlying: Logger, var initialId: String = "unknown") {
  private val sourceIdRef = new AtomicReference[String](initialId)

  private def formattedSourceId = s"[${sourceIdRef.get}]"

  private def withMDC(f: => Unit): IO[Unit] =
    IO {
      MDC.put("source", formattedSourceId)
      try {
        f
      } finally {
        MDC.remove("source")
      }
    }

  def info(msg: String): IO[Unit] = withMDC { underlying.info(msg) }
  def debug(msg: String): IO[Unit] = withMDC { underlying.debug(msg) }
  def error(msg: String): IO[Unit] = withMDC { underlying.error(msg) }
  def setSourceId(newId: String): IO[Unit] = IO({
    sourceIdRef.set(newId)
  })
}
