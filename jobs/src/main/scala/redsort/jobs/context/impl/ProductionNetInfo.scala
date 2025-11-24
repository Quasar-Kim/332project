package redsort.jobs.context.impl

import redsort.jobs.context.interface.NetInfo
import cats.effect._
import java.net.InetAddress

trait ProductionNetInfo extends NetInfo {
  override def getIP: IO[String] =
    IO(InetAddress.getLocalHost.getHostAddress)
}
