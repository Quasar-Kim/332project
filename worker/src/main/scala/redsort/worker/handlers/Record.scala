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

  implicit object RecordOrder extends Order[Record] {
    def compare(x: Record, y: Record): Int =
      compareKeys(x, y)
  }

  implicit object RecordOrdering extends Ordering[Record] {
    def compare(x: Record, y: Record): Int =
      compareKeys(x, y)
  }
}
