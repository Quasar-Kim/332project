package redsort.worker.gensort

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.nio.charset.StandardCharsets

class GensortSpec extends AnyFlatSpec with Matchers {

  private def makeDummyRecord(key: String): Array[Byte] = {
    require(key.length == 10, "Key length must be 10")
    val sb = new StringBuilder()
    sb.append(key)
    sb.append(" ")
    sb.append("X" * 87)
    sb.append("\r\n")
    sb.toString().getBytes(StandardCharsets.US_ASCII)
  }

  "gensort.generate" should "create data with correct size and format" in {
    val count = 100
    val data = gensort.generate(count)

    data.length shouldBe (count * 100)

    for (i <- 0 until count) {
      val offset = i * 100
      data(offset + 10) shouldBe ' '.toByte
      data(offset + 98) shouldBe '\r'.toByte
      data(offset + 99) shouldBe '\n'.toByte
    }
  }

  "gensort.validate" should "return true for sorted data" in {
    val record1 = makeDummyRecord("AAAAAAAAAA")
    val record2 = makeDummyRecord("BBBBBBBBBB")
    val sortedData = record1 ++ record2
    gensort.validate(sortedData) shouldBe true
  }

  it should "return false for unsorted data" in {
    val record1 = makeDummyRecord("BBBBBBBBBB")
    val record2 = makeDummyRecord("AAAAAAAAAA")
    val unsortedData = record1 ++ record2
    gensort.validate(unsortedData) shouldBe false
  }

  it should "return true for duplicate keys (stable sort condition)" in {
    val record1 = makeDummyRecord("KEYKEYKEY1")
    val record2 = makeDummyRecord("KEYKEYKEY1")
    val data = record1 ++ record2
    gensort.validate(data) shouldBe true
  }

  it should "return true for a single record" in {
    val data = makeDummyRecord("ONLYONEREC")
    gensort.validate(data) shouldBe true
  }

}
