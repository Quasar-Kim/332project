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
import redsort.jobs.Common.NetAddr
import redsort.jobs.Common.Mid

class JobRunnerSpec extends AsyncSpec {
  def fixture = new {
    val root = Path("/root")
    val dirs = new Directories(
      inputDirectories = Seq(root / "input1", root / "input2"),
      outputDirectory = root / "output",
      workingDirectory = root / "working"
    )
    val replicatorAddrs = Map(
      0 -> NetAddr("1.1.1.1", 5000),
      1 -> NetAddr("1.1.1.2", 5000),
      2 -> NetAddr("1.1.1.3", 5000),
      3 -> NetAddr("1.1.1.4", 5000)
    )

    val fileIO = stub[FileStorage]
    (fileIO.exists _).returnsWith(IO.pure(true))
    (fileIO.fileSize _).returnsWith(IO.pure(1024.toLong))

    val handlerStub = stub[JobHandler]
    val replicatorStub = stub[ReplicatorLocalServiceFs2Grpc[IO, Metadata]]
    (replicatorStub.pull _).returnsWith(IO.pure(new ReplicationResult(success = true)))
    (replicatorStub.push _).returnsWith(IO.pure(new ReplicationResult(success = true)))

    def getJobRunner(replicators: Map[Mid, NetAddr] = replicatorAddrs) =
      for {
        stateR <- SharedState.init
        _ <- stateR.modify { s =>
          (
            s.focus(_.wid)
              .replace(Some(new Wid(0, 0)))
              .focus(_.replicatorAddrs)
              .replace(replicators),
            IO.unit
          )
        }
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
      runner <- f.getJobRunner()
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
      runner <- f.getJobRunner()
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
      runner <- f.getJobRunner()
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
      runner <- f.getJobRunner()
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
      runner <- f.getJobRunner()
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
      runner <- f.getJobRunner()
      result <- runner.runJob(helloSpec)
    } yield {
      result.success should be(true)
      result.outputs(0).size should be(1024)
    }
  }

  it should "pull missing files from other machines" in {
    val f = fixture
    val filename = "@{working}/files/missing"
    (f.fileIO.exists _).returns(name => IO.pure(name != "/root/working/files/missing"))
    (f.handlerStub.apply _).returnsWith(IO.pure(None))
    val helloSpec = f.helloSpec
      .focus(_.inputs)
      .replace(
        Seq(
          new FileEntryMsg(
            path = filename,
            size = -1,
            replicas = Seq(1) // file is not available in machine 0
          )
        )
      )

    for {
      runner <- f.getJobRunner()
      result <- runner.runJob(helloSpec)
    } yield {
      (f.replicatorStub.pull _).calls(0)._1.path shouldBe filename
      (f.replicatorStub.pull _).calls(0)._1.src shouldBe 1
    }
  }

  it should "pull missing files from other machines even if replicas field is wrong" in {
    val f = fixture
    val filename = "@{working}/files/missing"
    // file is not available in this machine
    (f.fileIO.exists _).returns(name => IO.pure(name != "/root/working/files/missing"))
    (f.handlerStub.apply _).returnsWith(IO.pure(None))
    val helloSpec = f.helloSpec
      .focus(_.inputs)
      .replace(
        Seq(
          new FileEntryMsg(
            path = filename,
            size = -1,
            // jobspec says file is available on this machine but actually it isn't
            // this mismatch can happen if current machine was restarted due to fault
            replicas = Seq(0, 1)
          )
        )
      )

    for {
      runner <- f.getJobRunner()
      result <- runner.runJob(helloSpec)
    } yield {
      (f.replicatorStub.pull _).calls(0)._1.path shouldBe filename
      (f.replicatorStub.pull _).calls(0)._1.src shouldBe 1
    }
  }

  it should "return failed result if all pull fails" in {
    val f = fixture
    val filename = "@{working}/files/missing"
    (f.fileIO.exists _).returns(name => IO.pure(name != "/root/working/files/missing"))
    (f.replicatorStub.pull _)
      .returnsWith(IO.raiseError[ReplicationResult](new IllegalArgumentException("some error")))
    val helloSpec = f.helloSpec
      .focus(_.inputs)
      .replace(
        Seq(
          new FileEntryMsg(
            path = filename,
            size = -1,
            replicas = Seq(1, 2)
          )
        )
      )

    for {
      runner <- f.getJobRunner()
      result <- runner.runJob(helloSpec)
    } yield {
      // check if pull requests has been made
      (f.replicatorStub.pull _).calls(0)._1.path shouldBe filename
      (f.replicatorStub.pull _).calls(0)._1.src shouldBe 1
      (f.replicatorStub.pull _).calls(1)._1.path shouldBe filename
      (f.replicatorStub.pull _).calls(1)._1.src shouldBe 2

      // check result
      result.success shouldBe false
      result.error.get.kind shouldBe WorkerErrorKind.INPUT_REPLICATION_ERROR
    }
  }

  it should "try replicating output file to one of remote replicators" in {
    val f = fixture
    (f.handlerStub.apply _).returnsWith(IO.pure(None))
    (f.replicatorStub.push _).returnsOnCall {
      case 2 => IO.pure(ReplicationResult(success = true))
      case _ => IO.raiseError(new IllegalArgumentException("some error"))
    }
    val helloSpec = JobSpec.toMsg(
      new JobSpec(
        name = "hello",
        args = Seq(new StringArg("hello")),
        inputs = Seq(),
        outputs = Seq(
          new FileEntry(
            path = "@{working}/files/c",
            size = 1024,
            replicas = Seq(0)
          )
        )
      )
    )

    for {
      runner <- f.getJobRunner()
      result <- runner.runJob(helloSpec)
    } yield {
      assume(result.success)

      (f.replicatorStub.push _).calls.length shouldBe 2
      (f.replicatorStub.push _).calls.map(_._1.path).toSet shouldBe Set.fill(2)(
        "@{working}/files/c"
      )
      (f.replicatorStub.push _).calls.map(_._1.dst).toSet.size shouldBe 2
      result.outputs(0).replicas.length shouldBe 2
    }
  }

  it should "not replicate output file if there is only one machine" in {
    val f = fixture
    (f.handlerStub.apply _).returnsWith(IO.pure(None))
    val helloSpec = JobSpec.toMsg(
      new JobSpec(
        name = "hello",
        args = Seq(new StringArg("hello")),
        inputs = Seq(),
        outputs = Seq(
          new FileEntry(
            path = "@{working}/files/c",
            size = 1024,
            replicas = Seq(0)
          )
        )
      )
    )

    for {
      runner <- f.getJobRunner(Map(0 -> f.replicatorAddrs(0)))
      result <- runner.runJob(helloSpec)
    } yield {
      assume(result.success)

      (f.replicatorStub.push _).calls.length shouldBe 0
      result.outputs(0).replicas.length shouldBe 1
    }
  }

  it should "only replicate output files in working directory" in {
    val f = fixture
    (f.handlerStub.apply _).returnsWith(IO.pure(None))
    (f.replicatorStub.push _).returnsWith(IO.pure(ReplicationResult(success = true)))
    val helloSpec = JobSpec.toMsg(
      new JobSpec(
        name = "hello",
        args = Seq(new StringArg("hello")),
        inputs = Seq(),
        outputs = Seq(
          new FileEntry(
            path = "@{working}/files/c",
            size = 1024,
            replicas = Seq(0)
          ),
          new FileEntry(
            path = "@{output}/files/d",
            size = 1024,
            replicas = Seq(0)
          )
        )
      )
    )

    for {
      runner <- f.getJobRunner()
      result <- runner.runJob(helloSpec)
    } yield {
      assume(result.success)

      (f.replicatorStub.push _).calls.length shouldBe 1
      (f.replicatorStub.push _).calls(0)._1.path shouldBe "@{working}/files/c"
      result.outputs.find(_.path == "@{working}/files/c").get.replicas.length shouldBe 2
      result.outputs.find(_.path == "@{output}/files/d").get.replicas shouldBe Seq(0)
    }
  }

  it should "return failed result of output push fails" in {
    val f = fixture
    (f.handlerStub.apply _).returnsWith(IO.pure(None))
    (f.replicatorStub.push _).returnsWith(IO.raiseError(new IllegalArgumentException("some error")))
    val helloSpec = JobSpec.toMsg(
      new JobSpec(
        name = "hello",
        args = Seq(new StringArg("hello")),
        inputs = Seq(),
        outputs = Seq(
          new FileEntry(
            path = "@{working}/files/c",
            size = 1024,
            replicas = Seq(0)
          )
        )
      )
    )

    for {
      runner <- f.getJobRunner()
      result <- runner.runJob(helloSpec)
    } yield {
      result.success shouldBe false
      result.error.get.kind shouldBe WorkerErrorKind.OUTPUT_REPLICATION_ERROR
    }
  }
}
