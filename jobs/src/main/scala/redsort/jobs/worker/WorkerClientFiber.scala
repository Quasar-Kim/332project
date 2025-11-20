package redsort.jobs.worker

import cats._
import cats.effect._
import cats.syntax._
import cats.syntax.all._
import redsort.jobs.Common._
import scala.concurrent.duration._

import redsort.jobs.messages._
import io.grpc.Metadata
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder
import fs2.grpc.syntax.all._

import redsort.jobs.worker.filestorage.{FileStorage, AppContext}

import java.net.InetAddress

object WorkerClientFiber {
  def start(
      wtid: Int,
      masterIp: String,
      masterPort: Int,
      inputDirectories: Seq[String],
      outputDirectory: String,
      fileStorage: FileStorage[AppContext]
  ): IO[Int] = {

    NettyChannelBuilder
      .forAddress(masterIp, masterPort)
      .usePlaintext()
      .resource[IO]
      .flatMap { channel => SchedulerFs2Grpc.stubResource[IO](channel) }
      .use { client =>
        def register(request: WorkerHello): IO[Int] = {
          client
            .registerWorker(request, new Metadata())
            .map(_.mid)
            .handleErrorWith { err =>
              IO.println(s"[WorkerClientFiber] Registration failed: ${err.getMessage}") *>
                IO.sleep(2.seconds) *>
                register(request)
            }
        }
        for {
          _ <- IO.println(
            s"[WorkerClientFiber] Worker $wtid connected to master at $masterIp:$masterPort"
          )
          myIp <- IO.blocking(InetAddress.getLocalHost.getHostAddress)

          _ <- IO.println(
            s"[WorkerClientFiber] My IP address is $myIp"
          )

          // TODO(jaehwan)
          // This code should be tested!, however, I cannot figure out how to write testcase
          inputFiles <- inputDirectories.toList.flatTraverse { dir =>
            fileStorage.list(dir).compile.toList
          }
          inputFileEntries <- inputFiles.traverse { filePath =>
            fileStorage.fileSize(filePath.toString).map { size =>
              FileEntryMsg(
                path = filePath.toString,
                size = size.toInt,
                replicas = Seq.empty
              )
            }
          }

          localStorageInfo = LocalStorageInfo(
            mid = None,
            remainingStorage = 999999999, // TODO(jaehwan)
            entries = inputFileEntries.map(fe => fe.path -> fe).toMap
          )

          request = WorkerHello(
            wtid = wtid,
            storageInfo = Some(localStorageInfo),
            ip = myIp
          )

          mid <- register(request)
          // TODO(jaehwan): where to store mid?
          _ <- IO.println(s"[WorkerClientFiber] Registered successfully. My Wid is $wtid")
          res <- IO.never[Int]
        } yield res
      }
  }
}
