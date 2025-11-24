package redsort.jobs.worker

import cats._
import cats.effect._
import cats.syntax.all._
import redsort.jobs.Common._
import redsort.jobs.messages._
import redsort.jobs.context.WorkerCtx
import redsort.jobs.workers.SharedState
import scala.concurrent.duration._
import fs2.io.file.{Files, Path}
import cats.effect.std.Supervisor
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import redsort.jobs.context.interface.FileStorage
import io.grpc.Metadata
import monocle.syntax.all._
import org.log4s._
import redsort.jobs.SourceLogger
import scala.redsort.jobs.worker.handler.SyncJobHandler

trait Worker {
  def waitForComplete: IO[Unit]
}

object Worker {
  private[this] val logger = new SourceLogger(getLogger, "worker")

  def apply(
      handlerMap: Map[String, JobHandler],
      masterAddr: NetAddr,
      inputDirectories: Seq[Path],
      outputDirectory: Path,
      wtid: Int,
      port: Int,
      ctx: WorkerCtx
  ): Resource[IO, Worker] =
    for {
      // initialize state
      stateR <- SharedState.init.toResource
      handlerMap <- IO.pure(handlerMap.updated("__sync__", SyncJobHandler)).toResource

      // create a temporary directory and use it as a working directory
      workingDirectory <- createWorkingDir(ctx).toResource
      dirs <- Directories
        .init(
          inputDirectories = inputDirectories,
          outputDirectory = outputDirectory,
          workingDirectory = workingDirectory
        )
        .toResource

      // start RPC server on the background
      supervisor <- Supervisor[IO]
      completed <- IO.deferred[Unit].toResource
      _ <- Resource.eval {
        supervisor.supervise(
          WorkerServerFiber
            .start(
              stateR = stateR,
              port = port,
              handlers = handlerMap,
              dirs = dirs,
              ctx = ctx,
              logger = logger,
              completed = completed
            )
            .useForever
        )
      }

      // retrieve scheduler RPC client
      // this will block until connection establishes
      schedulerClient <- ctx.schedulerRpcClient(masterAddr)

      // registration worker
      wid <- registerWorkerToScheduler(schedulerClient, stateR, wtid, port, dirs, ctx).toResource
    } yield new Worker {
      override def waitForComplete: IO[Unit] =
        completed.get
    }

  def createWorkingDir(ctx: FileStorage): IO[Path] = {
    for {
      timestamp <- IO(LocalDateTime.now.format(DateTimeFormatter.ofPattern("YYYYMMdd_HHmmss")))
      path <- IO(Path(System.getProperty("user.dir")) / s"redsort-working-$timestamp")
      _ <- ctx.mkDir(path.toString)
    } yield path
  }

  def registerWorkerToScheduler(
      schedulerClient: SchedulerFs2Grpc[IO, Metadata],
      stateR: Ref[IO, SharedState],
      wtid: Int,
      port: Int,
      dirs: Directories,
      ctx: WorkerCtx
  ): IO[Unit] =
    for {
      state <- stateR.get

      // build WorkerHello
      storageInfo <- if (wtid == 0) getStorageInfo(dirs, ctx).map(Some(_)) else IO.pure(None)
      ip <- ctx.getIP
      _ <- logger.info(s"local ip address is $ip")
      workerHello <- IO.pure(
        new WorkerHello(
          wtid = wtid,
          storageInfo = storageInfo,
          ip = ip,
          port = port
        )
      )

      // do registration
      schedulerHello <- schedulerClient.registerWorker(workerHello, new Metadata)
      _ <- IO.raiseUnless(schedulerHello.success)(
        new RuntimeException(s"worker registration failed: ${schedulerHello.failReason}")
      )

      // update state according to SchedulerHello
      wid <- IO.pure(new Wid(schedulerHello.mid, wtid))
      _ <- logger.setSourceId(s"worker ${wid.mid}, ${wid.wtid}")
      _ <- logger.info(s"registered, my wid is $wid")
      _ <- stateR.update { state =>
        state
          .focus(_.wid)
          .replace(Some(wid))
          .focus(_.replicatorAddrs)
          .replace(schedulerHello.replicatorAddrs.view.mapValues(NetAddr.fromMsg(_)).toMap)
      }
    } yield ()

  def getStorageInfo(dirs: Directories, ctx: FileStorage): IO[LocalStorageInfo] =
    for {
      entries <- dirs.inputDirectories.traverse(inputDir =>
        for {
          entries <- ctx.list(inputDir.toString)
        } yield entries.map { case (path, entry) =>
          val symbolicPath = "@{input}" + path
          val newEntry = entry.focus(_.path).replace(symbolicPath)
          (symbolicPath, FileEntry.toMsg(newEntry))
        }
      )
    } yield new LocalStorageInfo(
      mid = None,
      remainingStorage = -1,
      entries = entries.flatten.toMap
    )
}
