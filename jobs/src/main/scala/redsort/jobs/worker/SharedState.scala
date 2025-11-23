package redsort.jobs.workers

import cats.effect._
import redsort.jobs.Common._
import redsort.jobs.messages.WorkerHello
import redsort.jobs.messages.HaltRequest

final case class SharedState(
    runningJob: Boolean,
    wid: Option[Wid],
    replicatorAddrs: Map[Mid, NetAddr]
)

object SharedState {
  def init = {
    val initialState = new SharedState(
      runningJob = false,
      wid = None,
      replicatorAddrs = Map()
    )

    Ref.of[IO, SharedState](initialState)
  }
}
