package redsort.master

import org.scalatest.matchers.should.Matchers
import com.google.protobuf.ByteString
import org.scalatest.funspec.AnyFunSpec
import scala.io.Source
import java.io.BufferedInputStream
import java.nio.file.{Paths, Files}

class PartitionSpec extends AnyFunSpec with Matchers {
  def readSample(resourcePath: String): ByteString = {
    val resourceUrl = getClass.getResource(resourcePath)
    val path = Paths.get(resourceUrl.toURI)
    ByteString.copyFrom(Files.readAllBytes(path))
  }

  describe("Partition.findPartitions") {
    it("should find 3 partitions from 30 samples") {
      val samples = Seq(
        readSample("/sample1.data"), // each 10 samples
        readSample("/sample2.data"),
        readSample("/sample3.data")
      ).fold(ByteString.empty())((acc, buffer) => acc.concat(buffer))
      val partitions = Partition.findPartitions(samples, 3)

      // calculate expected partitions
      val sortedKeys = Partition.sortedKeys(samples)
      val expectedPartitions =
        Seq(sortedKeys(10), sortedKeys(20), Partition.MAX_KEY)
      partitions should be(expectedPartitions)
    }

    it("should find 7 partitions from 30 samples") {
      val samples = Seq(
        readSample("/sample1.data"), // each 10 samples
        readSample("/sample2.data"),
        readSample("/sample3.data")
      ).fold(ByteString.empty())((acc, buffer) => acc.concat(buffer))
      val partitions = Partition.findPartitions(samples, 7)

      // calculate expected partitions
      val sortedKeys = Partition.sortedKeys(samples)
      val expectedPartitions =
        Seq(
          sortedKeys(4),
          sortedKeys(8),
          sortedKeys(12),
          sortedKeys(17),
          sortedKeys(21),
          sortedKeys(25),
          Partition.MAX_KEY
        )
      partitions should be(expectedPartitions)
    }
  }
}
