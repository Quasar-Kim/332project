package redsort.jobs.worker

import cats.effect._
import redsort.AsyncSpec
import fs2.io.file.Files
import fs2.io.file.Path
import redsort.jobs.context.impl.ProductionFileStorage
import java.io.File
import redsort.jobs.context.interface.FileStorage
import redsort.jobs.worker.jobrunner.JobRunner
import redsort.jobs.scheduler.JobSpec
import redsort.jobs.Common.assertIO
import redsort.jobs.Common.FileEntry
import redsort.jobs.messages._
import com.google.protobuf.any.{Any => ProtobufAny}
import com.google.protobuf.ByteString
import redsort.jobs.SourceLogger
import org.log4s._

class JobRunnerSpec extends AsyncSpec {
  def fixture = new {
    val root = Path("/root")
    val dirs = new Directories(
      inputDirectories = Seq(root / "input1", root / "input2"),
      outputDirectory = root / "output",
      workingDirectory = root / "working"
    )
    val fileIO = stub[FileStorage]
    (fileIO.exists _).returnsWith(IO.pure(true))

    val handlerStub = stub[JobHandler]
    val getJobRunner = JobRunner(
      handlers = Map("hello" -> handlerStub),
      dirs = dirs,
      ctx = fileIO,
      logger = new SourceLogger(getLogger)
    )

    val helloSpec = new JobSpec(
      name = "hello",
      args = Seq("hello"),
      inputs = Seq(
        new FileEntry(
          path = "@{input}/root/files/a",
          size = 1024,
          replicas = Seq(0)
        ),
        new FileEntry(
          path = "@{working}/files/b",
          size = 1024,
          replicas = Seq(0)
        )
      ),
      outputs = Seq(
        new FileEntry(
          path = "@{output}/files/c",
          size = 1024,
          replicas = Seq(0)
        )
      )
    )
  }

  "JobRunner.addHandler" should "add handler" in {
    val f = fixture
    for {
      runner <- f.getJobRunner
      runner <- runner.addHandler("hi" -> f.handlerStub)
    } yield {
      runner.getHandlers.size should be(2)
    }
  }

  behavior of "JobRunner.runJob"

  it should "return successful job request if job succeeds" in {
    val f = fixture
    (f.handlerStub.apply _).returns { case (args, inputs, outputs, ctx) =>
      args should be(Seq("hello"))
      inputs.map(_.toString) should be(Seq("/root/files/a", "/root/working/files/b"))
      outputs.map(_.toString) should be(Seq("/root/output/files/c"))
      IO.pure(Some(Array[Byte](192.toByte)))
    }

    for {
      runner <- f.getJobRunner
      result <- runner.runJob(f.helloSpec)
    } yield {
      result.success should be(true)
      result.retval should be(Some(ByteString.copyFrom(Array[Byte](192.toByte))))
      result.error should be(None)
    }
  }

  it should "return failed result if body function throws" in {
    val f = fixture
    (f.handlerStub.apply _).returns { case (args, inputs, outputs, ctx) =>
      IO.raiseError(new RuntimeException("catch me"))
    }

    for {
      runner <- f.getJobRunner
      result <- runner.runJob(f.helloSpec)
    } yield {
      result.success should be(false)
      result.error.get.kind should be(WorkerErrorKind.BODY_ERROR)
    }
  }

  it should "return failed result if input file does not exists" in {
    val f = fixture
    (f.handlerStub.apply _).returns { case (args, inputs, outputs, ctx) =>
      IO.raiseError(new RuntimeException("catch me"))
    }

    for {
      runner <- f.getJobRunner
      result <- runner.runJob(f.helloSpec)
    } yield {
      result.success should be(false)
      result.error.get.kind should be(WorkerErrorKind.BODY_ERROR)
    }
  }

  it should "return failed result if handler does not exists" in {
    val f = fixture
    val badSpec = new JobSpec(
      name = "bad",
      args = Seq(),
      inputs = Seq(),
      outputs = Seq()
    )

    for {
      runner <- f.getJobRunner
      result <- runner.runJob(badSpec)
    } yield {
      result.success should be(false)
      result.error.get.kind should be(WorkerErrorKind.JOB_NOT_FOUND)
    }
  }
}
