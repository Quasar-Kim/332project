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
import monocle.Focus
import monocle.syntax.all._
import io.grpc.Metadata
import redsort.jobs.workers.SharedState
import redsort.jobs.Common.Wid

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
    (fileIO.fileSize _).returnsWith(IO.pure(1024.toLong))

    val handlerStub = stub[JobHandler]
    val replicatorStub = stub[ReplicatorLocalServiceFs2Grpc[IO, Metadata]]
    val getJobRunner =
      for {
        stateR <- SharedState.init
        _ <- stateR.modify { s => (s.focus(_.wid).replace(Some(new Wid(0, 0))), IO.unit) }
        runner <- JobRunner(
          stateR = stateR,
          handlers = Map("hello" -> handlerStub),
          dirs = dirs,
          ctx = fileIO,
          logger = new SourceLogger(getLogger),
          replicatorClient = replicatorStub
        )
      } yield runner

    val helloSpec = JobSpec.toMsg(
      new JobSpec(
        name = "hello",
        args = Seq(new StringArg("hello")), // hello>?
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
    (f.handlerStub.apply _).returns { case (args, inputs, outputs, ctx, dirs) =>
      val parsedArg = args(0).unpack[StringArg].value
      parsedArg should be("hello")
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
    (f.handlerStub.apply _).returns { case (args, inputs, outputs, ctx, dirs) =>
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
    (f.handlerStub.apply _).returns { case (args, inputs, outputs, ctx, dirs) =>
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
    val badSpec = JobSpec.toMsg(
      new JobSpec(
        name = "bad",
        args = Seq(),
        inputs = Seq(),
        outputs = Seq()
      )
    )

    for {
      runner <- f.getJobRunner
      result <- runner.runJob(badSpec)
    } yield {
      result.success should be(false)
      result.error.get.kind should be(WorkerErrorKind.JOB_NOT_FOUND)
    }
  }

  it should "set outputs field of JobResult with actual file sizes" in {
    val f = fixture
    (f.handlerStub.apply _).returns { case (args, inputs, outputs, ctx, dirs) =>
      IO.pure(None)
    }
    val helloSpec = f.helloSpec
      .focus(_.outputs)
      .replace(
        Seq(
          new FileEntryMsg(
            path = "@{output}/files/c",
            size = -1,
            replicas = Seq(0)
          )
        )
      )

    for {
      runner <- f.getJobRunner
      result <- runner.runJob(helloSpec)
    } yield {
      result.success should be(true)
      result.outputs(0).size should be(1024)
    }
  }

  it should "pull missing files from other machines" in {
    val f = fixture
    val filename = "@{output}/files/missing"
    (f.fileIO.exists _).returns(name => IO.pure(name == filename))
    (f.replicatorStub.pull _).returnsWith(IO.pure(new ReplicationResult(success = true)))
    (f.handlerStub.apply _).returnsWith(IO.pure(None))
    val helloSpec = f.helloSpec
      .focus(_.inputs)
      .replace(
        Seq(
          new FileEntryMsg(
            path = filename,
            size = -1,
            replicas = Seq(1) // file is not available here
          )
        )
      )

    for {
      runner <- f.getJobRunner
      result <- runner.runJob(helloSpec)
    } yield {
      (f.replicatorStub.pull _).calls(0)._1.path shouldBe filename
      (f.replicatorStub.pull _).calls(0)._1.src shouldBe 1
    }
  }
}
