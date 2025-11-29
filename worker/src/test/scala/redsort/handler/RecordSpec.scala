package redsort.worker.handlers

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RecordSpec extends AnyFlatSpec with Matchers {
  behavior of "Records"

  it should "return first 10 bytes for .key" in {
    val key = Array.fill(10)(0.toByte)
    val value = Array.fill(90)(1.toByte)
    val record = new Record(key ++ value)
    record.key.array shouldBe key
  }

  it should "return last 90 bytes for .value" in {
    val key = Array.fill(10)(0.toByte)
    val value = Array.fill(90)(1.toByte)
    val record = new Record(key ++ value)
    record.value.array shouldBe value
  }

  it should "define order by unsigned lexicographical order of keys" in {
    val recordOne = new Record(Array.fill(100)(-1.toByte))
    val recordTwo = new Record(Array.fill(100)(1.toByte))

    recordTwo should be < recordOne
  }
}
