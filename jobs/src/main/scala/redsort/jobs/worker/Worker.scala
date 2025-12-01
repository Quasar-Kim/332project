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
  // private[this] val logger = new SourceLogger(getLogger, "worker")

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
  )(
      program: Worker => IO[Unit]
  )(implicit logger: SourceLogger = new SourceLogger(getLogger, "worker")): IO[Unit] =
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
      _ <- IO.whenA(wtid == 0)(
        logger.info(s"input directories: ${inputDirectories.map(_.toString).mkString(", ")}") >>
          logger.info(s"working directory: ${workDir.toString}") >>
          logger.info(s"output directory: ${outputDirectory.toString}")
      )

      // replicator service (only on wtid == 0)
      replciatorFiber =
        if (wtid == 0)
          startReplicator(
            stateR = stateR,
            ctx = ctx,
            dirs = dirs,
            localPort = replicatorLocalPort,
            remotePort = replicatorRemotePort
          ).flatMap(_ => logger.error("replicator exited prematurely"))
        else
          IO.never[Unit]

      // worker object passed to user program
      worker = new Worker {
        override def waitForComplete: IO[Unit] =
          completed.get
      }
      // user program
      programFiber = program(worker).flatMap(_ => logger.debug("user program exited"))

      // RPC server fiber
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

      _ <- logger.info(
        s"connecting to scheduler server at ${masterAddr.ip}:${masterAddr.port}"
      )

      _ <- ctx.schedulerRpcClient(masterAddr).use { schedulerClient =>
        for {
          // wait for registration
          _ <- logger.info(
            s"worker (port=$port, wtid=$wtid) connected to scheduler server, waiting for registration..."
          )
          _ <- registerWorkerToScheduler(schedulerClient, stateR, wtid, port, dirs, ctx)
          wid <- stateR.get.map(s => s.wid.get)
          _ <- logger.debug(s"${wid}: working directory ${workDir}")

          // start fibers concurrently with user program
          backgroundFiber = (serverFiber, replciatorFiber).parTupled
          _ <- programFiber.race(backgroundFiber)
        } yield ()
      }

      // start RPC server on the background
      // serverFiber = ctx
      //   .replicatorLocalRpcClient(new NetAddr("127.0.0.1", replicatorLocalPort))
      //   .use { replicatorClient =>
      //     WorkerServerFiber
      //       .start(
      //         stateR = stateR,
      //         port = port,
      //         handlers = handlerMap,
      //         dirs = dirs,
      //         ctx = ctx,
      //         logger = logger,
      //         completed = completed,
      //         replicatorClient = replicatorClient
      //       )
      //       .useForever
      //       .flatMap(_ => logger.error("server fiber exited prematurely"))
      //   }

      // worker = new Worker {
      //   override def waitForComplete: IO[Unit] =
      //     completed.get
      // }

      // mainFiber = ctx
      //   .schedulerRpcClient(masterAddr)
      //   .use(schedulerClient =>
      //     for {
      //       _ <- registerWorkerToScheduler(schedulerClient, stateR, wtid, port, dirs, ctx)
      //       wid <- stateR.get.map(s => s.wid.get)
      //       _ <- logger.debug(s"${wid}: working directory ${workDir}")
      //       replicatorIO =
      //         if (wtid == 0)
      //           startReplicator(
      //             stateR = stateR,
      //             ctx = ctx,
      //             dirs = dirs,
      //             localPort = replicatorLocalPort,
      //             remotePort = replicatorRemotePort
      //           )
      //         else
      //           IO.never[Unit]
      //             .flatMap(_ => logger.error("replicator exited prematurely"))
      //       programIO = program(worker).flatMap(_ => logger.debug("user program exited"))
      //       _ <- programIO.race(replicatorIO)
      //     } yield ()
      //   )

      // _ <- serverFiber.race(mainFiber)
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
  )(implicit logger: SourceLogger = new SourceLogger(getLogger, "worker")): IO[Unit] =
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
  )(implicit logger: SourceLogger = new SourceLogger(getLogger, "worker")): IO[Unit] =
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

  def finalize(dirs: Directories, ctx: FileStorage)(implicit
      logger: SourceLogger = new SourceLogger(getLogger, "worker")
  ): IO[Unit] =
    for {
      _ <- logger.debug("cleaning working directory")
      _ <- ctx.deleteRecursively(dirs.workingDirectory.toString)
    } yield ()
}
