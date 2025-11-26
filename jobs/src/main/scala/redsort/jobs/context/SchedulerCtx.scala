package redsort.jobs.context

import redsort.jobs.context.interface._

trait SchedulerCtx extends WorkerRpcClient with SchedulerRpcServer with NetInfo
