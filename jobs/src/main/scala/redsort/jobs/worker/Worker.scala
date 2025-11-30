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
import java.nio.file.FileAlreadyExistsException
import redsort.jobs.replicator.Replicator

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
      ctx: WorkerCtx,
      replicatorLocalPort: Int,
      replicatorRemotePort: Int,
      workingDirectory: Option[Path] = None
  )(program: Worker => IO[Unit]): IO[Unit] =
    for {
      // initialize state
      stateR <- SharedState.init
      handlerMap <- IO.pure(handlerMap.updated("__sync__", SyncJobHandler))
      completed <- IO.deferred[Unit]

      // create a temporary directory and use it as a working directory
      workDir <- workingDirectory match {
        case Some(dir) => IO.pure(dir)
        case None      => getWorkingDir(outputDirectory)
      }
      dirs <- Directories
        .init(
          inputDirectories = inputDirectories,
          outputDirectory = outputDirectory,
          workingDirectory = workDir
        )

      // start RPC server on the background
      serverFiber = ctx
        .replicatorLocalRpcClient(new NetAddr("127.0.0.1", replicatorLocalPort))
        .use { replicatorClient =>
          WorkerServerFiber
            .start(
              stateR = stateR,
              port = port,
              handlers = handlerMap,
              dirs = dirs,
              ctx = ctx,
              logger = logger,
              completed = completed,
              replicatorClient = replicatorClient
            )
            .useForever
            .flatMap(_ => logger.error("server fiber exited prematurely"))
        }

      worker = new Worker {
        override def waitForComplete: IO[Unit] =
          completed.get
      }

      mainFiber = ctx
        .schedulerRpcClient(masterAddr)
        .use(schedulerClient =>
          for {
            _ <- registerWorkerToScheduler(schedulerClient, stateR, wtid, port, dirs, ctx)
            wid <- stateR.get.map(s => s.wid.get)
            _ <- logger.debug(s"${wid}: working directory ${workDir}")
            replicatorIO =
              if (wtid == 0)
                startReplicator(
                  stateR = stateR,
                  ctx = ctx,
                  dirs = dirs,
                  localPort = replicatorLocalPort,
                  remotePort = replicatorRemotePort
                )
              else
                IO.never[Unit]
                  .flatMap(_ => logger.error("replicator exited prematurely"))
            programIO = program(worker).flatMap(_ => logger.debug("user program exited"))
            _ <- programIO.race(replicatorIO)
          } yield ()
        )

      _ <- logger.info(s"worker (port=$port, wtid=$wtid) started, waiting for scheduler server...")
      _ <- serverFiber.race(mainFiber)
      _ <- logger.info(s"worker (wtid: $wtid) done, running finalization")
      _ <- IO.whenA(wtid == 0)(finalize(dirs, ctx))
    } yield ()

  def getWorkingDir(outputDirectory: Path): IO[Path] = {
    IO.pure(outputDirectory / "redsort-working")
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

  def startReplicator(
      stateR: Ref[IO, SharedState],
      ctx: WorkerCtx,
      dirs: Directories,
      localPort: Int,
      remotePort: Int
  ): IO[Unit] =
    for {
      state <- stateR.get
      _ <- Replicator.start(
        replicatorAddrs = state.replicatorAddrs,
        ctx = ctx,
        dirs = dirs,
        localPort = localPort,
        remotePort = remotePort
      )
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

  def finalize(dirs: Directories, ctx: FileStorage): IO[Unit] =
    for {
      _ <- logger.debug("cleaning working directory")
      _ <- ctx.deleteRecursively(dirs.workingDirectory.toString)
    } yield ()
}
