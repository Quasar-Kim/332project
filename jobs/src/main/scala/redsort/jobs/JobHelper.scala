package redsort.jobs.jobhelper

import redsort.jobs.messages.Job._

object JobDone {
  def success(
      jid: String
  ): JobResult = JobResult(
    success = true,
    outputs = Seq.empty,
    inputReplications = Seq.empty,
    outputReplications = Seq.empty,
    calculationTime = 0,
    jid = jid
  )
  def fail(
      jid: String
  ): JobResult = JobResult(
    success = false,
    outputs = Seq.empty,
    inputReplications = Seq.empty,
    outputReplications = Seq.empty,
    calculationTime = 0,
    jid = jid
  )
}
