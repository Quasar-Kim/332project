package redsort.worker.handlers

import java.nio.ByteBuffer
import cats.Order

case class Record(buf: Array[Byte]) {
  def key: ByteBuffer = ByteBuffer.wrap(buf.slice(0, 10))
  def value: ByteBuffer = ByteBuffer.wrap(buf.slice(10, 100))
}

object Record {
  def compareKeys(x: Record, y: Record): Int = {
    var i = 0
    while (i < 10) {
      val a = x.buf(i) & 0xff
      val b = y.buf(i) & 0xff

      if (a != b) return a - b
      i += 1
    }
    0
  }

  def compareByteUnsigned(x: Byte, y: Byte): Int = {
    val unsignedIntX = if (x < 0) x.toInt + 256 else x.toInt
    val unsignedIntY = if (y < 0) y.toInt + 256 else y.toInt
    unsignedIntX - unsignedIntY
  }

  implicit object RecordOrder extends Order[Record] {
    def compare(x: Record, y: Record): Int =
      compareKeys(x, y)
  }

  implicit object RecordOrdering extends Ordering[Record] {
    def compare(x: Record, y: Record): Int =
      compareKeys(x, y)
  }
}
