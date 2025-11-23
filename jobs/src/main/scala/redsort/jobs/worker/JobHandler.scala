package redsort.jobs.worker

import fs2.io.file.Path
import redsort.jobs.context.interface.FileStorage
import cats.effect._

/** Job handler.
  */
trait JobHandler {

  /** Run a job upon request from scheduler. All input and output files are absolute paths. Symbolic
    * paths (e.g. "@{working}") are resolved before the call.
    *
    * @param args
    *   list of arguments.
    * @param inputs
    *   list of absolute paths to the job input files.
    * @param outputs
    *   list of absolute paths to the job output files.
    * @param ctx
    * @return
    *   Optional return value of type `Array[Byte]`
    */
  def apply(
      args: Seq[Any],
      inputs: Seq[Path],
      outputs: Seq[Path],
      ctx: FileStorage
  ): IO[Option[Array[Byte]]]
}
