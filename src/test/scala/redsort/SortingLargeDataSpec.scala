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
import redsort.master.CmdParser.numMachines

@Slow
class SortingLargeDataSpec extends AsyncFunSuite with AsyncIOSpec {

  val masterPortBase = new NextPort(7000)
  val workerPortBase = new NextPort(8001)

  test("sorting-1x1-1x10-10mb") {
    testSorting(
      name = "sorting-1x1-1x10-10mb",
      numMachines = 1,
      numInputDirs = 1,
      numFilesPerInputDir = 10,
      recordsPerFile = 10 * 100,
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

  test("sorting-1x1-10x1-10mb") {
    testSorting(
      name = "sorting-1x1-10x1-10mb",
      numMachines = 1,
      numInputDirs = 10,
      numFilesPerInputDir = 1,
      recordsPerFile = 10 * 100,
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

  test("sorting-1x4-4x1-10mb") {
    testSorting(
      name = "sorting-1x4-4x1-10mb",
      numMachines = 1,
      numInputDirs = 4,
      numFilesPerInputDir = 1,
      recordsPerFile = 10 * 100,
      numWorkerThreads = 4,
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

  test("sorting-10x4-4x1-10mb") {
    testSorting(
      name = "sorting-10x4-4x1-10mb",
      numMachines = 10,
      numInputDirs = 4,
      numFilesPerInputDir = 1,
      recordsPerFile = 10 * 100,
      numWorkerThreads = 4,
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

}
