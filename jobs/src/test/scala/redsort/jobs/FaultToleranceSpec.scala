package redsort.jobs

import cats._
import cats.effect._
import cats.syntax.all._
import org.log4s._
import redsort.{AsyncSpec, NetworkTest}
import redsort.AsyncFunSpec
import scala.sys.process._
import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.SimpleFileVisitor
import java.nio.file.Path
import java.nio.file.FileVisitResult
import java.nio.file.attribute.BasicFileAttributes
import java.io.IOException
import scala.concurrent.duration._
import java.nio.charset.StandardCharsets
import redsort.jobs.{WorkerSimulator, SchedulerSimulator}
import java.net.URLClassLoader
import org.scalatest.time.Span
import org.scalatest.tagobjects.Slow
import java.io.File

class FaultToleranceSpec extends AsyncFunSpec {
  override val timeLimit: Span = 30.seconds

  /** Setup directories used by workers */
  def setup(name: String): Path = {
    val baseDir = Paths.get(s"target/test-jobs-fault-tolerance/$name")

    // clean previous run directory
    if (Files.exists(baseDir)) {
      Files.walkFileTree(
        baseDir,
        new SimpleFileVisitor[Path] {
          override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
            Files.delete(file)
            FileVisitResult.CONTINUE
          }

          override def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult = {
            Files.delete(dir)
            FileVisitResult.CONTINUE
          }
        }
      )
    }

    // scaffold directory structure
    Files.createDirectories(baseDir)
    Files.createDirectories(baseDir.resolve("worker0"))
    Files.createDirectories(baseDir.resolve("worker0").resolve("inputs"))
    Files.createDirectories(baseDir.resolve("worker1"))
    Files.createDirectories(baseDir.resolve("worker1").resolve("inputs"))

    // create input files
    Files.write(
      baseDir.resolve("worker0").resolve("inputs").resolve("input.0"),
      "Hello scala!".getBytes()
    )
    Files.write(
      baseDir.resolve("worker1").resolve("inputs").resolve("input.1"),
      "Hello scala!".getBytes()
    )

    baseDir
  }

  /** Returns `Resource[IO, Process]` for scheduler and workers */
  def schedulerAndWorkerProcs(name: String, offset: Int) = {
    val classpath =
      Thread
        .currentThread()
        .getContextClassLoader()
        .asInstanceOf[URLClassLoader]
        .getURLs()
        .mkString(":")
    val javaBin = System.getProperty("java.home") + "/bin/java"
    val baseDir = s"target/test-jobs-fault-tolerance/$name"

    val baseArgs = Seq(javaBin, "-cp", classpath)
    val schedulerArgs = baseArgs ++ Seq(
      "redsort.jobs.SchedulerSimulator",
      "--test",
      name,
      "--port",
      (9000 + offset).toString()
    )
    val workerBaseArgs =
      baseArgs ++ Seq(
        "redsort.jobs.WorkerSimulator",
        "--test",
        name,
        "--master-port",
        (9000 + offset).toString()
      )

    // spawn scheduler process and two worker processes
    val allWorkersArgs = Seq(
      // worker 0 that kills itself
      workerBaseArgs ++ Seq(
        "--worker-id",
        "0",
        "--base-port",
        (9100 + offset).toString(),
        "--input-dir",
        baseDir + "/worker0/inputs",
        "--output-dir",
        baseDir + "/worker0/outputs",
        "--suicide"
      ),
      // worker 1
      workerBaseArgs ++ Seq(
        "--worker-id",
        "1",
        "--base-port",
        (9200 + offset).toString(),
        "--input-dir",
        baseDir + "/worker1/inputs",
        "--output-dir",
        baseDir + "/worker1/outputs"
      ),
      // worker 0 after revival
      workerBaseArgs ++ Seq(
        "--worker-id",
        "0",
        "--base-port",
        (9100 + offset).toString(),
        "--input-dir",
        baseDir + "/worker0/inputs",
        "--output-dir",
        baseDir + "/worker0/outputs"
      )
    )

    (processResource(schedulerArgs), allWorkersArgs.map(processResource(_)))
  }

  /** Make a resource that spawns process with command line arguments `args` upon acquisition and
    * kills it upon release
    */
  def processResource(args: Seq[String]): Resource[IO, Process] =
    Resource.make(IO(Process(args).run()))({ proc =>
      IO(proc.destroy())
    })

  def faultToleranceTest(name: String, recoverAfter: Duration) = {
    val baseDir = setup(name)
    val (schedulerResource, workerResources) =
      schedulerAndWorkerProcs(name, 0)

    val res = for {
      // start all processes, while waiting for worker 0 to kill itself
      schedulerProc <- schedulerResource
      workerOneProc <- workerResources(1)
      workerZeroProc <- workerResources(0)
      _ <- IO(assume(workerZeroProc.exitValue() == 137)).toResource

      // wait for N seconds, then restart worker 0
      _ <- IO.sleep(recoverAfter).toResource
      workerZeroProc <- workerResources(2)

      // wait for all processes to exit normally
      _ <- IO(schedulerProc.exitValue() shouldBe 0).toResource
      _ <- IO(workerZeroProc.exitValue() shouldBe 0).toResource
      _ <- IO(workerOneProc.exitValue() shouldBe 0).toResource

      // test outputs
      outZero <- IO(
        new String(
          Files.readAllBytes(
            (new File(baseDir.resolve("worker0").resolve("outputs").toString()))
              .listFiles()
              .apply(0)
              .toPath()
          ),
          StandardCharsets.US_ASCII
        )
      ).toResource
      outOne <- IO(
        new String(
          Files.readAllBytes(
            (new File(baseDir.resolve("worker1").resolve("outputs").toString()))
              .listFiles()
              .apply(0)
              .toPath()
          ),
          StandardCharsets.US_ASCII
        )
      ).toResource
    } yield {
      outZero.trim() shouldBe "24"
      outOne.trim() shouldBe "24"
    }

    res.use(_ => IO.unit)
  }

  /*
    All test cases spawns two workers that perform following jobs in sequence:
      - job "length": read input file `input.<n>` and writes length of it as string to file `@{working}/length.<n>`.
      - job "sum": read all output files produced by job "length", sum all lengths, then write it to `@{otuput}/sum.<n>`.

    Worker 0 will kill itself while handling job "length", then will respawn after N seconds (where N is case specific).
   */

  /* Restart worker 0 after 1 seconds while running job "length".
     No files are lost in this case. */
  test("machine restarting after 1 second while running job length", NetworkTest, Slow) {
    faultToleranceTest("while-running-job-length_short", recoverAfter = 1.second)
  }

  /* Same as above case, but restart worker 0 after 10 seconds. This is long enough for replicator
     to give up pull/push to worker 0. */
  ignore("machine restarting after 10 seconds while running job length", NetworkTest, Slow) {
    faultToleranceTest("while-running-job-length_long", recoverAfter = 10.second)
  }

  /* Restart worker 0 after 1 seconds while running job "sum".
     This requires restarted worker to pull missing files from another worker. */
  test("machine restarting after 1 seconds while running job sum", NetworkTest, Slow) {
    faultToleranceTest("while-running-job-sum_short", recoverAfter = 1.second)
  }

  /* "Long" variant of above test. */
  ignore("machine restarting after 10 seconds while running job sum", NetworkTest, Slow) {
    faultToleranceTest("while-running-job-sum_long", recoverAfter = 10.second)
  }
}
