package redsort

import redsort.jobs.context.SchedulerCtx
import cats.effect.testing.scalatest.AsyncIOSpec
import org.scalatest.flatspec.{AsyncFlatSpec, AnyFlatSpec}
import org.scalatest.matchers.should.Matchers
import org.scalamock.stubs.CatsEffectStubs
import org.scalatest.concurrent.AsyncTimeLimitedTests
import scala.concurrent.duration._
import org.scalatest.Inside
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.Tag

trait SpecBase extends Matchers with Inside

class AsyncSpec
    extends AsyncFlatSpec
    with SpecBase
    with AsyncIOSpec
    with CatsEffectStubs
    with AsyncTimeLimitedTests {
  val timeLimit = 10.seconds
}

class AsyncFunSpec
    extends AsyncFunSuite
    with SpecBase
    with AsyncIOSpec
    with CatsEffectStubs
    with AsyncTimeLimitedTests {

  val timeLimit = 10.seconds
}

class SyncSpec extends AnyFlatSpec with SpecBase

// Test that use actual networking
object NetworkTest extends Tag("redsort.networking")
