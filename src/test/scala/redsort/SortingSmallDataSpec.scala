package redsort

import cats.effect._
import cats.syntax.all._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import redsort.DistributedSortingTestHelper._
import org.scalatest.tags.Slow
import java.io.File
import java.nio.file.Paths
import java.nio.file.Files
import redsort.master.{Args => MasterArgs, Main => MasterMain}
import redsort.worker.{Configuration => WorkerArgs, Main => WorkerMain}
import org.scalatest.funspec.AsyncFunSpec
import cats.effect.testing.scalatest.AsyncIOSpec
import org.scalatest.funsuite.AsyncFunSuite
import fs2.io.file.Path
import redsort.jobs.Common.NetAddr
import scala.concurrent.duration._

@Slow
class SortingSmallDataSpec extends AsyncFunSuite with AsyncIOSpec {
  val masterPortBase = new NextPort(5000)
  val workerPortBase = new NextPort(6001)

  test("sorting-1x1-1x1-1kb") {
    testSorting(
      name = "sorting-1x1-1x1-1kb",
      numMachines = 1,
      numInputDirs = 1,
      numFilesPerInputDir = 1,
      recordsPerFile = 100,
      numWorkerThreads = 1,
      masterPort = masterPortBase.getNext,
      workerBasePort = workerPortBase.getNext
    ) { config =>
      (
        MasterMain
          .startScheduler(config.masterArgs),
        (0 until config.numMachines)
          .map(mid => WorkerMain.workerProgram(config.workerArgs(mid)))
          .toList
          .parSequence
      ).parMapN((_, _) => Seq(0))
    }
  }

  test("sorting-2x1-1kb") {
    testSorting(
      name = "sorting-2x1-1kb",
      numMachines = 2,
      numInputDirs = 1,
      numFilesPerInputDir = 1,
      recordsPerFile = 100,
      numWorkerThreads = 1,
      masterPort = masterPortBase.getNext,
      workerBasePort = workerPortBase.getNext
    ) { config =>
      (
        MasterMain
          .startScheduler(config.masterArgs),
        (0 until config.numMachines)
          .map(mid => WorkerMain.workerProgram(config.workerArgs(mid)))
          .toList
          .parSequence
      ).parMapN((workerAddrs, _) =>
        DistributedSortingTestHelper.workerAddrsToMachineOrder(workerAddrs)
      )
    }
  }

  test("sorting-1x1-10MB-multi-output") {
    testSorting(
      name = "sorting-1x1-10MB-multi-output",
      numMachines = 1,
      numInputDirs = 1,
      numFilesPerInputDir = 1,
      recordsPerFile = 100 * 1000, // 100KB * 100 = 10MB
      numWorkerThreads = 1,
      masterPort = masterPortBase.getNext,
      workerBasePort = workerPortBase.getNext,
      outFileSize = 1 // 1MB
    ) { config =>
      (
        MasterMain
          .startScheduler(config.masterArgs),
        (0 until config.numMachines)
          .map(mid => WorkerMain.workerProgram(config.workerArgs(mid)))
          .toList
          .parSequence
      ).parMapN((_, _) => Seq(0))
    }
  }

  test("sorting-1x2-1kb") {
    testSorting(
      name = "sorting-1x2-1kb",
      numMachines = 1,
      numInputDirs = 1,
      numFilesPerInputDir = 1,
      recordsPerFile = 100,
      numWorkerThreads = 2,
      masterPort = masterPortBase.getNext,
      workerBasePort = workerPortBase.getNext
    ) { config =>
      (
        MasterMain
          .startScheduler(config.masterArgs),
        (0 until config.numMachines)
          .map(mid => WorkerMain.workerProgram(config.workerArgs(mid)))
          .toList
          .parSequence
      ).parMapN((workerAddrs, _) =>
        DistributedSortingTestHelper.workerAddrsToMachineOrder(workerAddrs)
      )
    }
  }

  test("sorting-2x2-40MB") {
    testSorting(
      name = "sorting-2x2-40MB",
      numMachines = 2,
      numInputDirs = 2,
      numFilesPerInputDir = 1,
      recordsPerFile = 100 * 1000, // 100KB * 100 = 10MB
      numWorkerThreads = 2,
      masterPort = masterPortBase.getNext,
      workerBasePort = workerPortBase.getNext
    ) { config =>
      (
        MasterMain
          .startScheduler(config.masterArgs),
        (0 until config.numMachines)
          .map(mid => IO.sleep(mid * 1.second) >> WorkerMain.workerProgram(config.workerArgs(mid)))
          .toList
          .parSequence
      ).parMapN((workerAddrs, _) =>
        DistributedSortingTestHelper.workerAddrsToMachineOrder(workerAddrs)
      )
    }
  }

  test("sorting-2x4-400MB") {
    testSorting(
      name = "sorting-2x4-400MB",
      numMachines = 2,
      numInputDirs = 2,
      numFilesPerInputDir = 10,
      recordsPerFile = 100 * 1000, // 100KB * 100 = 10MB
      numWorkerThreads = 4,
      masterPort = masterPortBase.getNext,
      workerBasePort = workerPortBase.getNext
    ) { config =>
      (
        MasterMain
          .startScheduler(config.masterArgs),
        (0 until config.numMachines)
          .map(mid => IO.sleep(mid * 3.second) >> WorkerMain.workerProgram(config.workerArgs(mid)))
          .toList
          .parSequence
      ).parMapN((workerAddrs, _) =>
        DistributedSortingTestHelper.workerAddrsToMachineOrder(workerAddrs)
      )
    }
  }
}
