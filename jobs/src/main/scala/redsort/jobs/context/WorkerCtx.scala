package redsort.jobs.context

import redsort.jobs.context.interface._

trait WorkerCtx
    extends WorkerRpcServer
    with SchedulerRpcClient
    with ReplicatorLocalRpcClient
    with FileStorage
    with NetInfo
    with ReplicatorCtx
