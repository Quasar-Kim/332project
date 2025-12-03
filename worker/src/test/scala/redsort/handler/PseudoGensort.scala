package redsort.worker.gensort

import scala.util.Random
import redsort.worker.handlers.Record
import redsort.worker.handlers.Record
import cats.syntax.all._

object gensort {
  private val RECORD_SIZE = 100
  private val KEY_SIZE = 10
  private val SPACE_BYTE = ' '.toByte
  private val CR_BYTE = '\r'.toByte
  private val LF_BYTE = '\n'.toByte

  def randomBytes(length: Int): Array[Byte] = {
    Array.fill(length)((scala.util.Random.nextInt(256) - 128).toByte)
  }

  // format: [KEY 10B] [SPACE 1B] [VALUE 87B] [CRLF 2B]
  def generate(recordCount: Int): Array[Byte] = {
    (0 until recordCount).foldLeft(Array.empty[Byte]) { case (acc, _) =>
      val record =
        randomBytes(10).appended(' '.toByte) ++ randomBytes(87) ++ Array('\r'.toByte, '\n'.toByte)
      acc ++ record
    }
  }

  def validate(data: Array[Byte]): Boolean = {
    val recordCount = data.length / RECORD_SIZE
    for (i <- 0 until recordCount - 1) {
      val offset1 = i * RECORD_SIZE
      val offset2 = (i + 1) * RECORD_SIZE
      val record1 = new Record(data.slice(offset1, offset1 + RECORD_SIZE))
      val record2 = new Record(data.slice(offset2, offset2 + RECORD_SIZE))
      if (record1 > record2) {
        return false
      }
    }
    true
  }
}
