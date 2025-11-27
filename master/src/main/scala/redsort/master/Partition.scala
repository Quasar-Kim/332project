package redsort.master

import com.google.protobuf.ByteString

object Partition {
  val MAX_KEY = ByteString.fromHex("ff" * 11)

  /** Find split points that partitions sorted sequence of samples into equal sized subsequences.
    * i-th return value denotes end of i-th subsequence (exclusive) and start (i+1)th subsequence
    * (inclusive). For example, return value of `Seq(5, 10)` denotes two half-open ranges `[0, 10)`
    * and `[10, 20)`.
    *
    * @param samples
    *   `ByteString` of concatenated samples.
    * @param numSubSeq
    *   number of subsequences.
    * @return
    *   splits points of given samples.
    */
  def findPartitions(samples: ByteString, numPartitions: Int): Seq[ByteString] = {
    val keys = sortedKeys(samples)
    val numSamples = keys.size

    if (numPartitions <= 1 || numSamples == 0) return Seq(MAX_KEY)

    (1 until numPartitions)
      .map { i =>
        val splitPointIndex = (i.toLong * numSamples / numPartitions).toInt
        keys(splitPointIndex)
      }
      .appended(MAX_KEY)
  }

  implicit val byteStringOrdering: Ordering[ByteString] =
    Ordering.comparatorToOrdering(ByteString.unsignedLexicographicalComparator())

  def sortedKeys(samples: ByteString): IndexedSeq[ByteString] =
    (0 until samples.size by 100).map(i => samples.substring(i, i + 10)).sorted

}
