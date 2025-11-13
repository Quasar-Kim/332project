import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.concurrent.ScalaFutures
import org.scalamock.scalatest.MockFactory
import scala.concurrent.{ExecutionContext, Future}

import redsort.jobs.messages.Job._
import redsort.worker
import redsort.worker.config.Config
import redsort.worker.serverloop._

class WorkerServiceImplSpec extends AnyFlatSpec with Matchers with ScalaFutures with MockFactory {

  implicit val ec: ExecutionContext = ExecutionContext.global

  "WorkerServiceImpl" should "handle local jobs" in {
    val service = new WorkerServiceImpl()
    val localJob = JobSpec(name = "local-job", jid = "job-123", args = Seq("local"))

    whenReady(service.submitJob(localJob)) { result =>
      result.success shouldBe false
      result.jid shouldBe "job-123"
    }
  }
}
