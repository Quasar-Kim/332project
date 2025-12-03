package redsort.jobs.context

import redsort.jobs.context.interface._

trait ReplicatorCtx
    extends ReplicatorRemoteRpcClient
    with ReplicatorRemoteRpcServer
    with ReplicatorLocalRpcServer
    with FileStorage
