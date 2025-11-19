package redsort

import redsort.jobs.context.SchedulerCtx
import cats.effect.testing.scalatest.AsyncIOSpec
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalamock.stubs.CatsEffectStubs
import org.scalatest.concurrent.AsyncTimeLimitedTests
import scala.concurrent.duration._
import org.scalatest.Inside

class FlatSpecBase
    extends AsyncFlatSpec
    with AsyncIOSpec
    with Matchers
    with CatsEffectStubs
    with AsyncTimeLimitedTests
    with Inside {
  val timeLimit = 1.seconds
}
