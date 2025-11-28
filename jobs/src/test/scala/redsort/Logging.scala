package redsort

import cats.effect._
import org.log4s._
import org.slf4j.LoggerFactory
import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.encoder.PatternLayoutEncoder
import ch.qos.logback.core.FileAppender
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.classic.Level
import ch.qos.logback.classic
import org.slf4j.helpers.SubstituteLoggerFactory

/** XXX: Log will be polluted when multiple test cases are run concurrently. This can ber
  * workarounded by first identifying the failing test case, then re-running only that case. But
  * eventually we need to seperate log files even if multiple test cases run concurrently...
  */
object Logging {
  def fileLogger(name: String): Resource[IO, classic.Logger] = {
    val appenderName = s"FILE-$name"

    val setup = IO {
      // sometimes `asInstanceOf[LoggerContext]` fails if logback is not fully initialized.
      // try mutiple times until we can get logger.
      var iLoggerFactory = LoggerFactory.getILoggerFactory
      var attempts = 0
      val maxAttempts = 10
      while (iLoggerFactory.isInstanceOf[SubstituteLoggerFactory] && attempts < maxAttempts) {
        // Trigger initialization again
        LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME)
        Thread.sleep(50) // Give it a moment to bind
        iLoggerFactory = LoggerFactory.getILoggerFactory
        attempts += 1
      }
      if (iLoggerFactory.isInstanceOf[SubstituteLoggerFactory]) {
        throw new IllegalStateException("SLF4J failed to bind to Logback after waiting.")
      }

      val rootLogger = LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME)
      val context = LoggerFactory.getILoggerFactory().asInstanceOf[LoggerContext]

      // configure encoder
      val encoder = new PatternLayoutEncoder
      encoder.setContext(context)
      encoder.setPattern("%d{HH:mm:ss.SSS} %X{source} %-5level %logger{36} -%kvp- %msg%n")
      encoder.start()

      // configure file appender
      val appender = new FileAppender[ILoggingEvent]
      appender.setContext(context)
      appender.setName(appenderName)
      appender.setFile(s"jobs/target/test-logs/$name.log")
      appender.setAppend(false)
      appender.setEncoder(encoder)
      appender.start()

      // configure logger
      val logger = context.getLogger("redsort")
      logger.setLevel(Level.DEBUG)
      logger.setAdditive(false)
      logger.addAppender(appender)

      logger
    }

    def release(logger: classic.Logger) = IO {
      val appender = logger.getAppender(appenderName)
      appender.stop()
      logger.detachAppender(appender)
      ()
    }

    Resource.make(setup)(release)
  }
}
