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
import redsort.master.CmdParser.outFileSize

@Slow
class SortingSmallDataSpec extends AsyncFunSuite with AsyncIOSpec {

  val masterPortBase = new NextPort(5000)
  val workerPortBase = new NextPort(6001)

  // AxB-CxD means:
  // A: number of machines
  // B: number of threads per machine
  // C: number of input directories per machine
  // D: number of files per input directory

  test("sorting-1x1-1x1-10kb") {
    testSorting(
      name = "sorting-1x1-1x1-10kb",
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
      ).parMapN((workerAddrs, _) =>
        DistributedSortingTestHelper.workerAddrsToMachineOrder(workerAddrs)
      )
    }
  }

  test("sorting-1x1-10x100-10kb") {
    testSorting(
      name = "sorting-1x1-1x1-10kb",
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
      ).parMapN((workerAddrs, _) =>
        DistributedSortingTestHelper.workerAddrsToMachineOrder(workerAddrs)
      )
    }
  }

  test("sorting-1x3-1x1-10kb") {
    testSorting(
      name = "sorting-1x3-1x1-10kb",
      numMachines = 1,
      numInputDirs = 1,
      numFilesPerInputDir = 1,
      recordsPerFile = 100,
      numWorkerThreads = 3,
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

  test("sorting-3x1-1x1-10kb") {
    testSorting(
      name = "sorting-3x1-1x1-10kb",
      numMachines = 3,
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

  test("sorting-1x1-2x1-10kb") {
    testSorting(
      name = "sorting-1x1-2x1-10kb",
      numMachines = 1,
      numInputDirs = 2,
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

  test("sorting-3x3-1x1-10kb") {
    testSorting(
      name = "sorting-3x3-1x1-10kb",
      numMachines = 3,
      numInputDirs = 1,
      numFilesPerInputDir = 1,
      recordsPerFile = 100,
      numWorkerThreads = 3,
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

  test("sorting-10x1-1x1-10kb") {
    testSorting(
      name = "sorting-10x1-1x1-10kb",
      numMachines = 10,
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

  test("sorting-1x10-1x1-10kb") {
    testSorting(
      name = "sorting-1x10-1x1-10kb",
      numMachines = 1,
      numInputDirs = 1,
      numFilesPerInputDir = 1,
      recordsPerFile = 100,
      numWorkerThreads = 10,
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

  test("sorting-1x1-1x1-10MB-multi-output") {
    testSorting(
      name = "sorting-1x1-1x1-10MB-multi-output",
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
      ).parMapN((workerAddrs, _) =>
        DistributedSortingTestHelper.workerAddrsToMachineOrder(workerAddrs)
      )
    }
  }
}
