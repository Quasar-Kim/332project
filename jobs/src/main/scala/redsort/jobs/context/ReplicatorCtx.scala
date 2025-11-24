package redsort.jobs.context

import redsort.jobs.context.interface.{
  FileStorage,
  ReplicatorLocalRpcServer,
  ReplicatorRemoteRpcClient,
  ReplicatorRemoteRpcServer
}

trait ReplicatorCtx
    extends ReplicatorLocalRpcServer
    with ReplicatorRemoteRpcServer
    with ReplicatorRemoteRpcClient
    with FileStorage
