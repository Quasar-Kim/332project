package redsort.jobs.context.impl

import cats.effect.IO
import io.grpc.Metadata
import redsort.jobs.Common.FileEntry
import redsort.jobs.fileservice.FileReplicationService
import redsort.jobs.messages.{PullRequest, PushRequest, ReplicationResult, ReplicatorLocalServiceFs2Grpc}

class ReplicatorLocalServerImpl(fileService: FileReplicationService) extends ReplicatorLocalServiceFs2Grpc[IO, Metadata] {
  override def push(request: PushRequest, ctx: Metadata): IO[ReplicationResult] = ???

  override def pull(request: PullRequest, ctx: Metadata): IO[ReplicationResult] = {
    val path = request.path
    val src = request.src

    // file entry based on path
    // FIXME : real data about size and replicas?
    val entry = FileEntry(path, 0L, Seq())

    fileService.pull(entry, src)
  }
}
