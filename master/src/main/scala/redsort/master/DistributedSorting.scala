package redsort.master

import cats._
import cats.effect._
import cats.syntax.all._
import redsort.jobs.scheduler.Scheduler
import redsort.jobs.Common._
import redsort.jobs.scheduler.JobSpec
import redsort.jobs.messages.BytesArg
import com.google.protobuf.ByteString
import redsort.jobs.scheduler.JobExecutionResult
import org.log4s._
import redsort.jobs.SourceLogger

object DistributedSorting {
  private[this] val logger = new SourceLogger(getLogger, "master")

  def run(scheduler: Scheduler): IO[Unit] =
    for {
      // print master IP:port
      netAddr <- scheduler.netAddr
      _ <- IO.println(s"${netAddr.ip}:${netAddr.port}")

      // wait for all workers to be initialized
      _ <- logger.info("waiting for workers...")
      initialFiles <- scheduler.waitInit
      addrs <- scheduler.machineAddrs
      _ <- printMachineAddrs(addrs)

      // stage 1: sample
      _ <- logger.info("stage 1: sample")
      samplingResult <- scheduler.runJobs(sampleStep(initialFiles))

      // calculate partitions
      samples <- IO.pure(getSamplesFromResults(samplingResult))
      _ <- logger.info(
        s"calculating partitions from ${samples.size() / 1024 / 1024} MB samples..."
      )
      partitions <- IO.pure(
        Partition.findPartitions(
          samples = samples,
          numPartitions = scheduler.getNumMachines
        )
      )
      _ <- logger.debug(s"partition calculation done: $partitions")

      // stage 2: sort
      _ <- logger.info("stage 2: sorting")
      sortingResult <- scheduler.runJobs(sortStep(samplingResult.files))

      // stage 3: partition
      _ <- logger.info("stage 3: partitioning")
      partitionResult <- scheduler.runJobs(partitionStep(sortingResult.files, partitions))

      // stage 4: merge
      _ <- logger.info("stage 4: merging")
      mergeResult <- scheduler.runJobs(mergeStep(partitionResult.files))

      // finalize
      _ <- logger.info("shutting down cluster...")
      _ <- scheduler.complete
      _ <- logger.info("done!")
    } yield ()

  private def getSamplesFromResults(result: JobExecutionResult): ByteString =
    result.results
      .map(_._2.retval.get)
      .foldLeft(ByteString.empty())((acc, buf) => acc.concat(buf))

  private def printMachineAddrs(addrs: Seq[String]): IO[Unit] =
    IO.println(addrs.mkString(", "))

  /** Create one job per machine that fetches first 1MB of any chunk.
    *
    * @param files
    *   files per machines, as reported by scheduler
    * @return
    */
  def sampleStep(files: Map[Mid, Map[String, FileEntry]]): Seq[JobSpec] = {
    files.toSeq.map { case (mid, localFiles) =>
      val inputs = (localFiles.toSeq).lift(0) match {
        case Some(value) => Seq(value._2)
        case None        => Seq()
      }

      new JobSpec(
        name = "sample",
        args = Seq(),
        inputs = inputs,
        outputs = Seq()
      )
    }
  }

  /** Sort each input files.
    */
  def sortStep(files: Map[Mid, Map[String, FileEntry]]): Seq[JobSpec] = {
    val (_, allSpecs) = files.foldLeft((0, Seq[JobSpec]())) { case (acc, (mid, machineFiles)) =>
      machineFiles.foldLeft(acc) { case ((i, specs), (_, entry)) =>
        val spec = new JobSpec(
          name = "sort",
          args = Seq(),
          inputs = Seq(entry),
          outputs = Seq(
            new FileEntry(
              path = s"@{working}/sorted.$i",
              size = entry.size,
              replicas = entry.replicas
            )
          )
        )
        (i + 1, specs :+ spec)
      }
    }

    allSpecs
  }

  /** Partition each sorted file into multiple chunks.
    */
  def partitionStep(
      files: Map[Mid, Map[String, FileEntry]],
      partitions: Seq[ByteString]
  ): Seq[JobSpec] = {
    files.foldLeft(Seq[JobSpec]()) { case (acc, (mid, machineFiles)) =>
      machineFiles.filter(!_._1.startsWith("@{input}")).foldLeft(acc) {
        case (specs, (_, fileEntry)) =>
          val n =
            fileEntry.path.substring(fileEntry.path.lastIndexOf('.') + 1, fileEntry.path.length)
          val newSpec = new JobSpec(
            name = "partition",
            args = partitions.map(new BytesArg(_)),
            inputs = Seq(fileEntry),
            outputs = (0 until files.size).map { i =>
              new FileEntry(
                path = s"@{working}/partition.$n.$i",
                size = -1,
                replicas = fileEntry.replicas
              )
            }
          )
          specs :+ newSpec
      }
    }
  }

  /** merge partition files into 128MB (or smaller) sized chunks
    */
  def mergeStep(files: Map[Mid, Map[String, FileEntry]]): Seq[JobSpec] = {
    val inputPattern = "^@\\{working\\}\\/partition\\.(\\d+).(\\d+)$".r
    val (_, specs) = files.foldLeft((0, Seq[JobSpec]())) {
      case ((outputCounter, specs), (mid, machineFiles)) =>
        // gather input files
        val inputs = files
          .map(_._2)
          .flatten
          .filter { case ((path, _)) =>
            inputPattern.findFirstMatchIn(path) match {
              case Some(matches) => matches.group(2) == mid.toString()
              case None          => false
            }
          }
          .map(_._2)
          .toSeq

        // determine outputs for this machine
        val (remainingSize, nextPartitionNum, maybeOutputs) =
          inputs.foldLeft((0.toLong, outputCounter, Seq[FileEntry]())) {
            case ((sizeAcc, i, outputsAcc), entry) =>
              val nextSizeAcc = sizeAcc + entry.size
              if (nextSizeAcc >= 128 * 1024 * 1024) {
                (0.toLong until (nextSizeAcc / (128 * 1024 * 1024)))
                  .foldLeft((nextSizeAcc, i, outputsAcc)) {
                    case ((remainingSize, j, outputEntries), _) =>
                      val output = new FileEntry(
                        path = s"@{output}/partition.$j",
                        size = 128 * 1024 * 1024,
                        replicas = Seq(mid)
                      )
                      (remainingSize - 128 * 1024 * 1024, j + 1, outputEntries :+ output)
                  }
              } else (nextSizeAcc, i, outputsAcc)
          }
        val (outputs, nextOutputCounter) = if (remainingSize > 0) {
          (
            maybeOutputs :+ new FileEntry(
              path = s"@{output}/partition.$nextPartitionNum",
              size = remainingSize,
              replicas = Seq(mid)
            ),
            nextPartitionNum + 1
          )
        } else (maybeOutputs, nextPartitionNum)

        // create merge spec
        val spec = new JobSpec(
          name = "merge",
          args = Seq(),
          inputs = inputs,
          outputs = outputs
        )
        (nextOutputCounter, specs :+ spec)
    }
    specs
  }
}
