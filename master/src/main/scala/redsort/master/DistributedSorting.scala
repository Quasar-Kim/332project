package redsort.master

import cats._
import cats.effect._
import cats.syntax._
import redsort.jobs.scheduler.Scheduler
import redsort.jobs.Common._
import redsort.jobs.scheduler.JobSpec
import redsort.jobs.messages.BytesArg
import com.google.protobuf.ByteString

object DistributedSorting {
  def run(numMachines: Int, numWorkersPerMachine: Int, scheduler: Scheduler): IO[Unit] =
    notImplmenetedIO

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
      partitionInfo: Seq[Array[Byte]]
  ): Seq[JobSpec] = {
    files.foldLeft(Seq[JobSpec]()) { case (acc, (mid, machineFiles)) =>
      machineFiles.filter(!_._1.startsWith("@{input}")).foldLeft(acc) {
        case (specs, (_, fileEntry)) =>
          val n =
            fileEntry.path.substring(fileEntry.path.lastIndexOf('.') + 1, fileEntry.path.length)
          val newSpec = new JobSpec(
            name = "partition",
            args = partitionInfo.map(bytes => new BytesArg(ByteString.copyFrom(bytes))),
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
