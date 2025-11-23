package redsort.jobs.context.interface

import cats.effect._

trait NetInfo {
  def getIP: IO[String]
}
