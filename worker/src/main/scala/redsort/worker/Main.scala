package redsort.worker

import redsort.worker.config.Config
import redsort.worker.serverloop._
import redsort.jobs.JobRunner
import redsort.jobs.fileserver.FileServer

import scala.annotation.tailrec
import io.grpc.{Server, ServerBuilder, ManagedChannel, ManagedChannelBuilder}
import scala.concurrent.ExecutionContext

import redsort.jobs.messages.Job._
import redsort.jobs.fileserver.InMemoryFileStorage

case object ArgParser {
  def parse(args: Array[String]): Config = {

    @tailrec
    def parserLoop(args: List[String], state: Int, config: Config): Config = {
      args match {
        case Nil =>
          config
        case "-I" :: tail =>
          parserLoop(tail, 2, config)
        case "-O" :: tail =>
          parserLoop(tail, 3, config)
        case head :: tail =>
          state match {
            case 1 => // arg: ip:port
              parserLoop(tail, 0, config.copy(address = head))
            case 2 => // opt: input
              parserLoop(tail, 2, config.copy(inputs = config.inputs :+ head))
            case 3 => // opt: input
              parserLoop(tail, 0, config.copy(output = head))
            case 0 => // Ignore
              parserLoop(tail, 0, config)
            case _ => // Error
              parserLoop(tail, 0, config)
          }
      }
    }
    parserLoop(args.toList, 1, Config())
  }
  def validateConfig(config: Config): Boolean = {
    config.address.nonEmpty && config.inputs.nonEmpty && config.output.nonEmpty
  }
}

object Main {
  def main(args: Array[String]): Unit = {
    val config = ArgParser.parse(args)
    if (!ArgParser.validateConfig(config)) {
      println("Invalid configuration")
      sys.exit(-1)
    }

    println(config)

    val WorkerPort = 30040
    val masterIP: String = config.address.split(":").head
    val masterPort: Int = config.address.split(":").last.toInt

    implicit val ec: ExecutionContext = ExecutionContext.global
    val fs = new InMemoryFileStorage
    val worker = new WorkerServiceImpl(fs)
    var server: Server = null
    var channel: ManagedChannel = null

    try {
      val serverDefinition = WorkerServiceGrpc.bindService(worker, ec)
      server = ServerBuilder
        .forPort(WorkerPort)
        .addService(serverDefinition)
        .build()

      // Communication channel to Master server (not used currently)
      // channel = ManagedChannelBuilder
      //   .forAddress(masterIP, masterPort)
      //   .usePlaintext()
      //   .build()
      // val clientStub = MasterServiceGrpc.stub(channel)

      server.start()
      println(s"Worker server started on port $WorkerPort")
      server.awaitTermination()
    } catch {
      case e: InterruptedException =>
        println("Worker server interrupted")
      case e: Exception =>
        e.printStackTrace()
        sys.exit(-1)
    } finally {
      if (server != null) {
        println(s"Worker server stopped on port $WorkerPort")
        server.shutdown()
      }
    }
  }
}
