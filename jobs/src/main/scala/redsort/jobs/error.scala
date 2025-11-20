package redsort.jobs

import redsort.jobs.messages.JobSystemError

final case class JobSystemException(
    message: String = "",
    source: String = "(unknown)",
    cause: Throwable = None.orNull
) extends Exception(message, cause)

object JobSystemException {
  def fromMsg(msg: JobSystemError, source: String = "(unknown)"): JobSystemException =
    new JobSystemException(
      message = msg.context.size match {
        case 0 => s"job system exception from $source: ${msg.message}"
        case _ =>
          msg.message ++ s"job system exception from $source: ${msg.message} (context: ${msg.context})"
      },
      source = source,
      cause = msg.cause match {
        case Some(innerMsg) => JobSystemException.fromMsg(innerMsg)
        case None           => None.orNull
      }
    )
}

final case class Unreachable(
    message: String = "unreachable expression",
    cause: Throwable = None.orNull
) extends Exception(message, cause)
