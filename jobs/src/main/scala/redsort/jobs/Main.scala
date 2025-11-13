package redsort.jobs

import redsort.jobs.fileserver.FileServer

object Main {
  def main(args: Array[String]): Unit = {
    FileServer.setup()
  }
}
