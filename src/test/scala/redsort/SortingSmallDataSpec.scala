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
import redsort.master.{Args, Main => MasterMain}
import org.scalatest.funspec.AsyncFunSpec
import cats.effect.testing.scalatest.AsyncIOSpec
import org.scalatest.funsuite.AsyncFunSuite
import redsort.master.CmdParser.numMachines

@Slow
class SortingSmallDataSpec extends AsyncFunSuite with AsyncIOSpec {

  test("Sorting 80KB (80) entries") {
    testSorting(
      name = "sorting-2x4-80kb",
      numMachines = 2,
      numInputDirs = 2,
      numFilesPerInputDir = 2,
      recordsPerFile = 100
    ) { baseDir =>
      (
        MasterMain.startScheduler(new Args(numMachines = 2, port = 5000, threads = 4)),
        (0 until 2).toList.map(i => IO.pure(6000 + i * 10)).parSequence.void
      ).parMapN((machineOrder, _) =>
        machineOrder
          .filter { case (wid, _) => wid.wtid == 0 }
          .map { case (wid, addr) => (wid.mid, (addr.port - 6000) / 10) }
          .toSeq
          .sortBy { case (mid, i) => mid }
          .map { case (mid, i) => i }
      )
    }
  }
}
