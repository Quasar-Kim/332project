package redsort.worker.gensort

import java.nio.charset.StandardCharsets
import scala.util.Random

object gensort {
  private val RECORD_SIZE = 100
  private val KEY_SIZE = 10
  private val SPACE_BYTE = ' '.toByte
  private val CR_BYTE = '\r'.toByte
  private val LF_BYTE = '\n'.toByte

  def randomString(length: Int): String = {
    (1 to length).map { _ => (33 + Random.nextInt(126 - 33 + 1)).toChar }.mkString
  }

  // format: [KEY 10B] [SPACE 1B] [VALUE 87B] [CRLF 2B]
  def generate(recordCount: Int): Array[Byte] = {
    val sb = new StringBuilder()
    for (_ <- 0 until recordCount) {
      sb.append(randomString(10))
      sb.append(" ")
      sb.append(randomString(87))
      sb.append("\r\n")
    }
    sb.toString().getBytes(StandardCharsets.US_ASCII)
  }

  def validate(data: Array[Byte]): Boolean = {
    val recordCount = data.length / RECORD_SIZE
    for (i <- 0 until recordCount - 1) {
      val offset1 = i * RECORD_SIZE
      val offset2 = (i + 1) * RECORD_SIZE
      val key1 = new String(data, offset1, KEY_SIZE, StandardCharsets.US_ASCII)
      val key2 = new String(data, offset2, KEY_SIZE, StandardCharsets.US_ASCII)
      if (key1 > key2) {
        return false
      }
    }
    true
  }
}
