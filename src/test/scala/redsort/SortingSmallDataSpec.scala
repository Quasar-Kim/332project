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
class SortingSmallDataSpec extends AsyncFunSuite with AsyncIOSpec {

  test("sorting-1x1-1kb") {
    testSorting(
      name = "sorting-1x1-1kb",
      numMachines = 1,
      numInputDirs = 1,
      numFilesPerInputDir = 1,
      recordsPerFile = 100,
      numWorkerThreads = 1,
      masterPort = 5100,
      workerBasePort = 6101
    ) { config =>
      (
        MasterMain
          .startScheduler(config.masterArgs),
        (0 until 1)
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
      masterPort = 5200,
      workerBasePort = 6201
    ) { config =>
      (
        MasterMain
          .startScheduler(config.masterArgs),
        (0 until 2)
          .map(mid => WorkerMain.workerProgram(config.workerArgs(mid)))
          .toList
          .parSequence
      ).parMapN((workerAddrs, _) =>
        workerAddrs
          .filter { case (wid, _) => wid.wtid == 0 }
          .toList
          .sortBy { case (_, addr) => addr.port }
          .map { case (wid, _) => wid.mid }
      )
    }
  }
}
