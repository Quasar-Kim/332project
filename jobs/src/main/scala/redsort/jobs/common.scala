package redsort.jobs

import redsort.jobs.messages.FileEntryMsg

object Common {

  /** Worker ID, a tuple of mid and wtid.
    *
    * @param mid
    *   machine ID.
    * @param wtid
    *   worker thread ID.
    */
  final case class Wid(mid: Mid, wtid: Wtid)

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

  /** A single file entry in one or more workers.
    *
    * @param path
    *   an absolute path to file.
    * @param size
    *   size of a file.
    * @param replicas
    *   sequence of machines on which this file is replicated to.
    */
  final case class FileEntry(path: String, size: Int, replicas: Seq[Mid])

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
}
