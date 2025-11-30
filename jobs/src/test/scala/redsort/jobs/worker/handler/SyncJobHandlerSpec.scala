package scala.redsort.jobs.worker.handler

import redsort.AsyncSpec
import cats.effect._
import redsort.jobs.context.impl.InMemoryFileStorage
import redsort.jobs.messages.FileEntryMsg
import com.google.protobuf.any
import redsort.jobs.worker.Directories
import fs2.io.file.Path
import redsort.jobs.Common.assertIO

class SyncJobHandlerSpec extends AsyncSpec {
  behavior of "SyncJobHandler"

  it should "remove intermediate files not present in args" in {
    val args = Seq(
      new FileEntryMsg(path = "@{working}/a", size = 1024, replicas = Seq(0)),
      new FileEntryMsg(path = "@{input}/x", size = 1024, replicas = Seq(0))
    )

    for {
      // setup file system
      fs <- IO.ref(Map[String, Array[Byte]]())
      storage <- IO.pure(new InMemoryFileStorage(fs))
      _ <- storage.writeAll("/working/a", Array())
      _ <- storage.writeAll("/working/b", Array())
      _ <- storage.writeAll("/input/x", Array())

      // run sync job
      _ <- SyncJobHandler(
        args = args.map(any.Any.pack(_)),
        inputs = Seq(),
        outputs = Seq(Path("/working/synced")),
        ctx = storage,
        dirs = new Directories(
          inputDirectories = Seq(Path("/input")),
          workingDirectory = Path("/working"),
          outputDirectory = Path("/output")
        )
      )
      _ <- storage.exists("/working/a").map(exists => exists should be(true))
      _ <- storage.exists("/working/b").map(exists => exists should be(false))
      _ <- storage.exists("/input/x").map(exists => exists should be(true))
    } yield ()
  }
}
