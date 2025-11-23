/* Collection of common data structures and types. */

package redsort.jobs

import cats._
import cats.effect._
import cats.syntax.all._
import redsort.jobs.messages.FileEntryMsg
import redsort.jobs.messages.WidMsg
import redsort.jobs.messages.NetAddrMsg

object Common {

  /** Worker ID, a tuple of mid and wtid.
    *
    * @param mid
    *   machine ID.
    * @param wtid
    *   worker thread ID.
    */
  final case class Wid(mid: Mid, wtid: Wtid) {
    override def toString(): String =
      s"<worker ${this.mid},${this.wtid}>"
  }
  object Wid {
    def fromMsg(msg: WidMsg): Wid =
      new Wid(mid = msg.mid, wtid = msg.wtid)

    def toMsg(wid: Wid): WidMsg =
      new WidMsg(mid = wid.mid, wtid = wid.wtid)

  }

  type Mid = Int
  type Wtid = Int

  /** Tuple of ip and port.
    *
    * @param ip
    *   IPv4 address.
    * @param port
    *   port number.
    */
  final case class NetAddr(ip: String, port: Int)
  object NetAddr {
    def fromMsg(msg: NetAddrMsg): NetAddr =
      new NetAddr(ip = msg.ip, port = msg.port)

    def toMsg(addr: NetAddr): NetAddrMsg =
      new NetAddrMsg(ip = addr.ip, port = addr.port)
  }

  /** A single file entry in one or more workers.
    *
    * @param path
    *   an absolute path to file.
    * @param size
    *   size of a file.
    * @param replicas
    *   sequence of machines on which this file is replicated to.
    */
  final case class FileEntry(path: String, size: Long, replicas: Seq[Mid])

  object FileEntry {
    def toMsg(entry: FileEntry): FileEntryMsg =
      new FileEntryMsg(
        path = entry.path,
        size = entry.size,
        replicas = entry.replicas
      )

    def fromMsg(m: FileEntryMsg): FileEntry =
      new FileEntry(
        path = m.path,
        size = m.size,
        replicas = m.replicas
      )
  }

  /* Cats IO version of assert(). */
  def assertIO(pred: Boolean, msg: String = "IO assertion failure") =
    IO.raiseWhen(!pred)(new AssertionError(msg))

  def unreachableIO[A]: IO[A] = IO.raiseError[A](new Unreachable)

  def notImplmenetedIO[A]: IO[A] = IO.raiseError[A](new NotImplementedError)
}
