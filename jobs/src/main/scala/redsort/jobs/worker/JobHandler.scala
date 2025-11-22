package redsort.jobs.worker

import fs2.io.file.Path
import redsort.jobs.context.interface.FileStorage
import cats.effect._

trait JobHandler {
  def apply(
      args: Seq[Any],
      inputs: Seq[Path],
      outputs: Seq[Path],
      ctx: FileStorage
  ): IO[Option[Array[Byte]]]
}
