/* Common error (exceptions) data structures. */

package redsort.jobs

import redsort.jobs.messages.JobSystemError

/** General exception that can be sent to another machine or component.
  *
  * @param message
  *   a error message.
  * @param source
  *   component or worker that cause the error.
  * @param cause
  *   set in case of exception is caused by another exception.
  */
final case class JobSystemException(
    message: String = "",
    source: String = "(unknown)",
    context: Map[String, String] = Map(),
    cause: Option[JobSystemException] = None
) extends Exception(message, null)

object JobSystemException {
  def fromMsg(msg: JobSystemError, source: String = "(unknown)"): JobSystemException =
    new JobSystemException(
      message = msg.message,
      source = source,
      context = msg.context,
      cause = msg.cause match {
        case Some(innerMsg) => Some(JobSystemException.fromMsg(innerMsg))
        case None           => None
      }
    )

  def toMsg(e: JobSystemException): JobSystemError =
    new JobSystemError(
      message = e.message,
      cause = e.cause match {
        case Some(err) => Some(JobSystemException.toMsg(err))
        case None      => None
      },
      context = e.context
    )

  def fromThrowable(t: Throwable): JobSystemException =
    new JobSystemException(
      message = t.getMessage(),
      cause =
        if (t.getCause() != null) Some(JobSystemException.fromThrowable(t.getCause())) else None
    )
}

/* Unreachable expression. Shoud NEVER happen. */
final case class Unreachable(
    message: String = "unreachable expression",
    cause: Throwable = None.orNull
) extends Exception(message, cause)
