package redsort

import redsort.jobs.context.SchedulerCtx
import cats.effect.testing.scalatest.AsyncIOSpec
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalamock.stubs.CatsEffectStubs

class FlatSpecBase extends AsyncFlatSpec with AsyncIOSpec with Matchers with CatsEffectStubs
