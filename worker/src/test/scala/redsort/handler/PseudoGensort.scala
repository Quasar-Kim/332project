// package redsort.worker.gensort

// import cats.effect._
// import cats.effect.testing.scalatest.AsyncIOSpec
// import org.scalatest.flatspec.AsyncFlatSpec
// import org.scalatest.matchers.should.Matchers
// import fs2.io.file.{Files, Path}
// import redsort.jobs.worker.filestorage._
// import redsort.jobs.messages._

// import java.nio.charset.StandardCharsets
// import scala.util.Random

// object gensort {

//   private val RECORD_SIZE = 100
//   private val KEY_SIZE = 10

//   private val SPACE_BYTE = ' '.toByte
//   private val CR_BYTE = '\r'.toByte
//   private val LF_BYTE = '\n'.toByte

//   def randomString(length: Int): String = {
//     (1 to length).map { _ => (33 + Random.nextInt(126 - 33 + 1)).toChar }.mkString
//   }

//   // format: [KEY 10B] [SPACE 1B] [VALUE 87B] [CRLF 2B]
//   def generate(recordCount: Int): Array[Byte] = {
//     val sb = new StringBuilder()
//     for (i <- 0 until recordCount) {
//       sb.append(randomString(10))
//       sb.append(" ")
//       sb.append(randomString(87))
//       sb.append("\r\n")
//     }
//     sb.toString().getBytes(StandardCharsets.US_ASCII)
//   }

//   def validate(data: Array[Byte]): Boolean = {
//     if (data.length % RECORD_SIZE != 0) {
//       return false
//     }
//     val recordCount = data.length / RECORD_SIZE
//     var prevKeyBytes: Array[Byte] = null
//     for (i <- 0 until recordCount) {
//       val offset = i * RECORD_SIZE
//       if (
//         data(offset + 10) != SPACE_BYTE ||
//         data(offset + 98) != CR_BYTE ||
//         data(offset + 99) != LF_BYTE
//       ) {
//         return false
//       }
//       val currentKeyBytes = data.slice(offset, offset + KEY_SIZE)
//       if (prevKeyBytes != null) {
//         if (compareBytes(prevKeyBytes, currentKeyBytes) > 0)
//           return false
//       }
//       prevKeyBytes = currentKeyBytes
//     }
//     true
//   }

//   private def compareBytes(a: Array[Byte], b: Array[Byte]): Int = {
//     val len = Math.min(a.length, b.length)
//     for (i <- 0 until len) {
//       val diff = (a(i) & 0xff) - (b(i) & 0xff)
//       if (diff != 0) return diff
//     }
//     a.length - b.length
//   }

// }
