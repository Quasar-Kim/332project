package redsort.jobs.fileservice

import cats.effect.IO
import io.grpc.Metadata
import org.scalamock.stubs.Stub
import redsort.AsyncSpec
import redsort.jobs.Common.FileEntry
import redsort.jobs.context.impl.ReplicatorLocalServerImpl
import redsort.jobs.messages.{PullRequest, ReplicationResult, ReplicationStats}

class ReplicatorLocalServerImplSpec extends AsyncSpec {
  def fixture = new {
    val path = "file.txt"
    val src = 1

    val pullRequest: PullRequest = PullRequest(path = path, src = src)
    val metadata = new Metadata()

    // stub file service
    val fileService: Stub[FileReplicationService] = stub[FileReplicationService]

    // expected entry (to be created after pull)
    val expectedEntry: FileEntry = FileEntry(path, 0L, Seq())

    val server = new ReplicatorLocalServerImpl(fileService)
  }

  behavior of "server.pull"

  it should "invoke fileService.pull based on the PullRequest" in {
    val f = fixture

    val expectedReplicationResult = ReplicationResult(
      success = true,
      error = None,
      stats = None // ignore stats for now
    )

    // stub the pull of file service
    stubbed(f.fileService.pull _).returns { arg =>
      val entry = arg._1
      val src = arg._2
      if (entry == f.expectedEntry && src == f.src) IO.pure(expectedReplicationResult)
      else IO.pure(ReplicationResult()) // unsuccessful replication
    }

    val res = f.server.pull(f.pullRequest, f.metadata).unsafeRunSync()
    res shouldBe expectedReplicationResult
  }

  it should "propagate error from fileService.pull" in {
    val f = fixture

    val errMsg = "some replication error occurred"

    // mimic failure
    stubbed(f.fileService.pull _).returns { arg =>
      val entry = arg._1
      val src = arg._2
      if (entry == f.expectedEntry && src == f.src) IO.raiseError(new RuntimeException(errMsg))
      else IO.pure(ReplicationResult())
    }

    val thrown = intercept[RuntimeException] {
      f.server.pull(f.pullRequest, f.metadata).unsafeRunSync()
    }

    thrown.getMessage shouldBe errMsg
  }
}
