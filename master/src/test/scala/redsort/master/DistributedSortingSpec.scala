package redsort.master

import redsort.master.DistributedSorting
import cats.effect._
import cats.syntax.all._
import cats.effect.testing.scalatest.AsyncIOSpec
import org.scalamock.stubs.CatsEffectStubs
import org.scalatest.concurrent.AsyncTimeLimitedTests
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.matchers.should.Matchers
import redsort.jobs.scheduler.Scheduler
import redsort.jobs.Common.FileEntry
import redsort.jobs.scheduler.JobExecutionResult
import redsort.jobs.messages.JobResult
import org.checkerframework.checker.units.qual.m
import redsort.jobs.scheduler.JobSpec
import redsort.jobs.Common.notImplmenetedIO
import com.google.protobuf.ByteString
import redsort.jobs.messages.IntArg
import redsort.jobs.Common.Mid
import redsort.jobs.messages.BytesArg
import monocle.syntax.all._
import com.google.protobuf.any
import redsort.jobs.messages.LongArg

class DistributedSortingSpec
    extends AsyncFunSuite
    with Matchers
    with AsyncIOSpec
    with CatsEffectStubs {

  val fixture = new {
    val sampleA = ByteString.copyFrom(List.fill(100)(0x40.toByte).toArray)
    val sampleB = ByteString.copyFrom(List.fill(100)(0x41.toByte).toArray)
    val sampleC = ByteString.copyFrom(List.fill(100)(0x42.toByte).toArray)
    val sampleD = ByteString.copyFrom(List.fill(100)(0x43.toByte).toArray)
  }

  val machineOneInputs = Map(
    "@{input}/data1/input.0" -> new FileEntry(
      path = "@{input}/data1/input.0",
      size = 32 * 1024 * 1024,
      replicas = Seq(0)
    ),
    "@{input}/data1/input.1" -> new FileEntry(
      path = "@{input}/data1/input.1",
      size = 32 * 1024 * 1024,
      replicas = Seq(0)
    ),
    "@{input}/data2/input.0" -> new FileEntry(
      path = "@{input}/data2/input.0",
      size = 32 * 1024 * 1024,
      replicas = Seq(0)
    ),
    "@{input}/data2/input.1" -> new FileEntry(
      path = "@{input}/data2/input.1",
      size = 32 * 1024 * 1024,
      replicas = Seq(0)
    )
  )

  val machineTwoInputs = Map(
    "@{input}/data1/input.0" -> new FileEntry(
      path = "@{input}/data1/input.0",
      size = 32 * 1024 * 1024,
      replicas = Seq(1)
    ),
    "@{input}/data1/input.1" -> new FileEntry(
      path = "@{input}/data1/input.1",
      size = 32 * 1024 * 1024,
      replicas = Seq(1)
    ),
    "@{input}/data2/input.0" -> new FileEntry(
      path = "@{input}/data2/input.0",
      size = 32 * 1024 * 1024,
      replicas = Seq(1)
    ),
    "@{input}/data2/input.1" -> new FileEntry(
      path = "@{input}/data2/input.1",
      size = 32 * 1024 * 1024,
      replicas = Seq(1)
    )
  )

  test("sample step") {
    val files = Map(
      0 -> machineOneInputs,
      1 -> machineTwoInputs
    )
    val specs = DistributedSorting.sampleStep(files)

    val machineZeroSpec = new JobSpec(
      name = "sample",
      args = Seq(),
      inputs = Seq(files(0)("@{input}/data1/input.0")),
      outputs = Seq()
    )
    val machineOneSpec = new JobSpec(
      name = "sample",
      args = Seq(),
      inputs = Seq(files(1)("@{input}/data1/input.0")),
      outputs = Seq()
    )

    specs.toSet should be(Set(machineZeroSpec, machineOneSpec))
  }

  test("sort step") {
    val files = Map(
      0 -> machineOneInputs,
      1 -> machineTwoInputs
    )
    val specs = DistributedSorting.sortStep(files)
    def jobSpec(input: FileEntry, output: FileEntry) = new JobSpec(
      name = "sort",
      args = Seq(),
      inputs = Seq(input),
      outputs = Seq(output)
    )

    val expectedSpecs = Set(
      jobSpec(
        files(0)("@{input}/data1/input.0"),
        new FileEntry(path = "@{working}/sorted.0", size = 32 * 1024 * 1024, replicas = Seq(0))
      ),
      jobSpec(
        files(0)("@{input}/data1/input.1"),
        new FileEntry(path = "@{working}/sorted.1", size = 32 * 1024 * 1024, replicas = Seq(0))
      ),
      jobSpec(
        files(0)("@{input}/data2/input.0"),
        new FileEntry(path = "@{working}/sorted.2", size = 32 * 1024 * 1024, replicas = Seq(0))
      ),
      jobSpec(
        files(0)("@{input}/data2/input.1"),
        new FileEntry(path = "@{working}/sorted.3", size = 32 * 1024 * 1024, replicas = Seq(0))
      ),
      jobSpec(
        files(1)("@{input}/data1/input.0"),
        new FileEntry(path = "@{working}/sorted.4", size = 32 * 1024 * 1024, replicas = Seq(1))
      ),
      jobSpec(
        files(1)("@{input}/data1/input.1"),
        new FileEntry(path = "@{working}/sorted.5", size = 32 * 1024 * 1024, replicas = Seq(1))
      ),
      jobSpec(
        files(1)("@{input}/data2/input.0"),
        new FileEntry(path = "@{working}/sorted.6", size = 32 * 1024 * 1024, replicas = Seq(1))
      ),
      jobSpec(
        files(1)("@{input}/data2/input.1"),
        new FileEntry(path = "@{working}/sorted.7", size = 32 * 1024 * 1024, replicas = Seq(1))
      )
    )

    specs.toSet should be(expectedSpecs)
  }

  test("partition step") {
    val files = Map(
      0 -> Map(
        "@{working}/sorted.0" -> new FileEntry(
          path = "@{working}/sorted.0",
          size = 32 * 1024 * 1024,
          replicas = Seq(0)
        ),
        "@{working}/sorted.1" -> new FileEntry(
          path = "@{working}/sorted.1",
          size = 32 * 1024 * 1024,
          replicas = Seq(0)
        ),
        "@{working}/sorted.2" -> new FileEntry(
          path = "@{working}/sorted.2",
          size = 32 * 1024 * 1024,
          replicas = Seq(0)
        ),
        "@{working}/sorted.3" -> new FileEntry(
          path = "@{working}/sorted.3",
          size = 32 * 1024 * 1024,
          replicas = Seq(0)
        )
      ).concat(machineOneInputs),
      1 -> Map(
        "@{working}/sorted.4" -> new FileEntry(
          path = "@{working}/sorted.4",
          size = 32 * 1024 * 1024,
          replicas = Seq(1)
        ),
        "@{working}/sorted.5" -> new FileEntry(
          path = "@{working}/sorted.5",
          size = 32 * 1024 * 1024,
          replicas = Seq(1)
        ),
        "@{working}/sorted.6" -> new FileEntry(
          path = "@{working}/sorted.6",
          size = 32 * 1024 * 1024,
          replicas = Seq(1)
        ),
        "@{working}/sorted.7" -> new FileEntry(
          path = "@{working}/sorted.7",
          size = 32 * 1024 * 1024,
          replicas = Seq(1)
        )
      ).concat(machineTwoInputs)
    )
    val partitions = Seq(
      ByteString.fromHex("80000000000000000000"),
      ByteString.fromHex("0100000000000000000000")
    )
    val specs = DistributedSorting.partitionStep(files, partitions)

    def jobSpec(input: FileEntry, outputs: Seq[FileEntry]) =
      new JobSpec(
        name = "partition",
        args = partitions.map(new BytesArg(_)),
        inputs = Seq(input),
        outputs = outputs
      )

    val expectedSpecs = Set(
      jobSpec(
        files(0)("@{working}/sorted.0"),
        Seq(
          new FileEntry(path = "@{working}/partition.0.0", size = -1, replicas = Seq(0)),
          new FileEntry(path = "@{working}/partition.0.1", size = -1, replicas = Seq(0))
        )
      ),
      jobSpec(
        files(0)("@{working}/sorted.1"),
        Seq(
          new FileEntry(path = "@{working}/partition.1.0", size = -1, replicas = Seq(0)),
          new FileEntry(path = "@{working}/partition.1.1", size = -1, replicas = Seq(0))
        )
      ),
      jobSpec(
        files(0)("@{working}/sorted.2"),
        Seq(
          new FileEntry(path = "@{working}/partition.2.0", size = -1, replicas = Seq(0)),
          new FileEntry(path = "@{working}/partition.2.1", size = -1, replicas = Seq(0))
        )
      ),
      jobSpec(
        files(0)("@{working}/sorted.3"),
        Seq(
          new FileEntry(path = "@{working}/partition.3.0", size = -1, replicas = Seq(0)),
          new FileEntry(path = "@{working}/partition.3.1", size = -1, replicas = Seq(0))
        )
      ),
      jobSpec(
        files(1)("@{working}/sorted.4"),
        Seq(
          new FileEntry(path = "@{working}/partition.4.0", size = -1, replicas = Seq(1)),
          new FileEntry(path = "@{working}/partition.4.1", size = -1, replicas = Seq(1))
        )
      ),
      jobSpec(
        files(1)("@{working}/sorted.5"),
        Seq(
          new FileEntry(path = "@{working}/partition.5.0", size = -1, replicas = Seq(1)),
          new FileEntry(path = "@{working}/partition.5.1", size = -1, replicas = Seq(1))
        )
      ),
      jobSpec(
        files(1)("@{working}/sorted.6"),
        Seq(
          new FileEntry(path = "@{working}/partition.6.0", size = -1, replicas = Seq(1)),
          new FileEntry(path = "@{working}/partition.6.1", size = -1, replicas = Seq(1))
        )
      ),
      jobSpec(
        files(1)("@{working}/sorted.7"),
        Seq(
          new FileEntry(path = "@{working}/partition.7.0", size = -1, replicas = Seq(1)),
          new FileEntry(path = "@{working}/partition.7.1", size = -1, replicas = Seq(1))
        )
      )
    )

    specs.toSet should be(expectedSpecs)
  }

  test("merge step") {
    val files = Map(
      0 -> Map(
        "@{working}/partition.0.0" -> new FileEntry(
          path = "@{working}/partition.0.0",
          size = 16 * 1024 * 1024,
          replicas = Seq(0)
        ),
        "@{working}/partition.0.1" -> new FileEntry(
          path = "@{working}/partition.0.1",
          size = 16 * 1024 * 1024,
          replicas = Seq(0)
        ),
        "@{working}/partition.1.0" -> new FileEntry(
          path = "@{working}/partition.1.0",
          size = 16 * 1024 * 1024,
          replicas = Seq(0)
        ),
        "@{working}/partition.1.1" -> new FileEntry(
          path = "@{working}/partition.1.1",
          size = 16 * 1024 * 1024,
          replicas = Seq(0)
        ),
        "@{working}/partition.2.0" -> new FileEntry(
          path = "@{working}/partition.2.0",
          size = 16 * 1024 * 1024,
          replicas = Seq(0)
        ),
        "@{working}/partition.2.1" -> new FileEntry(
          path = "@{working}/partition.2.1",
          size = 16 * 1024 * 1024,
          replicas = Seq(0)
        ),
        "@{working}/partition.3.0" -> new FileEntry(
          path = "@{working}/partition.3.0",
          size = 16 * 1024 * 1024,
          replicas = Seq(0)
        ),
        "@{working}/partition.3.1" -> new FileEntry(
          path = "@{working}/partition.3.1",
          size = 16 * 1024 * 1024,
          replicas = Seq(0)
        )
      ).concat(machineOneInputs),
      1 -> Map(
        "@{working}/partition.4.0" -> new FileEntry(
          path = "@{working}/partition.4.0",
          size = 16 * 1024 * 1024,
          replicas = Seq(1)
        ),
        "@{working}/partition.4.1" -> new FileEntry(
          path = "@{working}/partition.4.1",
          size = 16 * 1024 * 1024,
          replicas = Seq(1)
        ),
        "@{working}/partition.5.0" -> new FileEntry(
          path = "@{working}/partition.5.0",
          size = 16 * 1024 * 1024,
          replicas = Seq(1)
        ),
        "@{working}/partition.5.1" -> new FileEntry(
          path = "@{working}/partition.5.1",
          size = 16 * 1024 * 1024,
          replicas = Seq(1)
        ),
        "@{working}/partition.6.0" -> new FileEntry(
          path = "@{working}/partition.6.0",
          size = 16 * 1024 * 1024,
          replicas = Seq(1)
        ),
        "@{working}/partition.6.1" -> new FileEntry(
          path = "@{working}/partition.6.1",
          size = 16 * 1024 * 1024,
          replicas = Seq(1)
        ),
        "@{working}/partition.7.0" -> new FileEntry(
          path = "@{working}/partition.7.0",
          size = 16 * 1024 * 1024,
          replicas = Seq(1)
        ),
        "@{working}/partition.7.1" -> new FileEntry(
          path = "@{working}/partition.7.1",
          size = 512 * 1024 * 1024, // artifically create large partition for testing
          replicas = Seq(1)
        )
      ).concat(machineTwoInputs)
    )
    val specs = DistributedSorting.mergeStep(files, outFileSize = 128 * 1024 * 1024)

    def jobSpec(inputs: Seq[FileEntry], outputs: Seq[FileEntry]) =
      new JobSpec(
        name = "merge",
        args = Seq(
          new LongArg(128 * 1024 * 1024)
        ),
        inputs = inputs,
        outputs = outputs
      )

    val expectedSpecs = Set(
      jobSpec(
        Seq(
          files(0)("@{working}/partition.0.0"),
          files(0)("@{working}/partition.1.0"),
          files(0)("@{working}/partition.2.0"),
          files(0)("@{working}/partition.3.0"),
          files(1)("@{working}/partition.4.0"),
          files(1)("@{working}/partition.5.0"),
          files(1)("@{working}/partition.6.0"),
          files(1)("@{working}/partition.7.0") // total size: 128MiB
        ),
        Seq(
          new FileEntry(path = "@{output}/partition.0", size = 128 * 1024 * 1024, replicas = Seq(0))
        )
      ),
      jobSpec(
        Seq(
          files(0)("@{working}/partition.0.1"),
          files(0)("@{working}/partition.1.1"),
          files(0)("@{working}/partition.2.1"),
          files(0)("@{working}/partition.3.1"),
          files(1)("@{working}/partition.4.1"),
          files(1)("@{working}/partition.5.1"),
          files(1)("@{working}/partition.6.1"),
          files(1)("@{working}/partition.7.1") // total size: 624MiB
        ),
        Seq(
          new FileEntry(
            path = "@{output}/partition.1",
            size = 128 * 1024 * 1024,
            replicas = Seq(1)
          ),
          new FileEntry(
            path = "@{output}/partition.2",
            size = 128 * 1024 * 1024,
            replicas = Seq(1)
          ),
          new FileEntry(
            path = "@{output}/partition.3",
            size = 128 * 1024 * 1024,
            replicas = Seq(1)
          ),
          new FileEntry(
            path = "@{output}/partition.4",
            size = 128 * 1024 * 1024,
            replicas = Seq(1)
          ),
          new FileEntry(path = "@{output}/partition.5", size = 112 * 1024 * 1024, replicas = Seq(1))
        )
      )
    )

    specs
      .map(spec =>
        spec
          .focus(_.inputs)
          .modify(inputs => inputs.sortBy(_.path))
          .focus(_.outputs)
          .modify(outputs => outputs.sortBy(_.path))
      )
      .toSet should be(expectedSpecs)
  }
}
