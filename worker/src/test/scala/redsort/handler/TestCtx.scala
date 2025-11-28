package redsort.worker.testctx

import cats.effect._
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import fs2.{Pipe, Stream}
import fs2.io.file.{Files, Path}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import redsort.jobs.Common.FileEntry
import redsort.jobs.worker.Directories
import redsort.worker.gensort.gensort

import redsort.jobs.context._
import redsort.jobs.context.interface._
import redsort.jobs.context.impl._

trait FakeNetInfo extends NetInfo {
  override def getIP: IO[String] =
    IO.pure("127.0.0.1")
}

class WorkerTestCtx(ref: Ref[IO, Map[String, Array[Byte]]])
    extends InMemoryFileStorage(ref)
    with WorkerCtx
    with ProductionWorkerRpcServer
    with ProductionSchedulerRpcClient
    with FakeNetInfo
    with ProductionReplicatorLocalRpcClient
    with ProductionReplicatorLocalRpcServer
    with ProductionReplicatorRemoteRpcClient
    with ProductionReplicatorRemoteRpcServer
