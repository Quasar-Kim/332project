package redsort.jobs.fileservice

import cats.effect.{IO, Resource}
import fs2.Stream
import org.scalamock.stubs.Stub
import redsort.AsyncSpec
import redsort.jobs.Common.FileEntry
import redsort.jobs.context.ReplicatorCtx
import redsort.jobs.context.interface.FileStorage
import redsort.jobs.messages.ReplicationErrorKind.{CONNECTION_ERROR, FILE_NOT_FOUND_ERROR}

import java.nio.charset.StandardCharsets

class FileReplicationServiceSpec extends AsyncSpec {
  def fixture = new {
    val myMid = 1
    val dstMid = 2
    // make a file
    val content = "qwerty"
    val path = "file.txt"
    val contentBytes: Stream[IO, Byte] =
      Stream.emits(content.getBytes).covary[IO] // stream of bytes for writeFile
    val entry: FileEntry =
      FileEntry(path, content.length.toLong, Seq(myMid)) // file that exists on this machine only

    // network stub
    val network: Stub[NetworkAlgebra[IO]] = stub[NetworkAlgebra[IO]]

    // context stubs
    val myCtx: Stub[ReplicatorCtx] = stub[ReplicatorCtx]
    val dstCtx: Stub[ReplicatorCtx] = stub[ReplicatorCtx]

    // get client stub
    stubbed(network.getClient _).returns { mid =>
      if (mid == myMid) Resource.pure[IO, ReplicatorCtx](myCtx)
      else if (mid == dstMid) Resource.pure[IO, ReplicatorCtx](dstCtx)
      else Resource.eval(IO.raiseError(new RuntimeException(s"unexpected machine id -- $mid")))
    }

    // storage stub
    val storage: Stub[FileStorage] = stub[FileStorage]
    stubbed(storage.writeAll _).returns { arg =>
      val filename = arg._1
      val fileContent = new String(arg._2, StandardCharsets.UTF_8)
      println("wrote to " + filename)
      println("content: " + fileContent)
      IO.unit
    }

    val fileService = new FileReplicationService(network, myMid, storage)
  }

  behavior of "fileService.push"

  it should "return FILE_NOT_FOUND_ERROR if file does not exist in this machine" in {
    val f = fixture

    // imitate file not existing
    stubbed(f.myCtx.exists _).returnsWith(IO.pure(false))

    val res = f.fileService.push(f.entry, f.dstMid).unsafeRunSync()

    res.success shouldBe false
    res.error shouldBe Some(FILE_NOT_FOUND_ERROR)
    res.stats.map(_.path) shouldBe Some(f.entry.path)
    res.stats.map(_.src) shouldBe Some(f.myMid)
    res.stats.map(_.dst) shouldBe Some(f.dstMid)
  }

  it should "return CONNECTION_ERROR if write fails" in {
    val f = fixture

    // imitate all ok except writeFile
    stubbed(f.myCtx.exists _).returnsWith(IO.pure(true))
    stubbed(f.myCtx.read _).returnsWith(f.contentBytes)
    stubbed(f.network.writeFile _).returnsWith(IO.raiseError(new RuntimeException("fail")))

    val res = f.fileService.push(f.entry, f.dstMid).unsafeRunSync()

    res.success shouldBe false
    res.error shouldBe Some(CONNECTION_ERROR)
    res.stats.map(_.path) shouldBe Some(f.entry.path)
    res.stats.map(_.src) shouldBe Some(f.myMid)
    res.stats.map(_.dst) shouldBe Some(f.dstMid)
  }
}
