// package redsort.worker.handlers

// import cats._
// import cats.effect._
// import cats.syntax._
// import redsort.jobs.Common._
// import redsort.jobs.messages._

// import scala.concurrent.duration._
// import redsort.jobs.worker.filestorage.{FileStorage, AppContext}

// // TODO: ASSUME:
// // Just use the first input file for sampling
// // The number of outputs is just one

// // TODO: This implementation uses in-memory sorting.

// class JobSorter(fileStorage: FileStorage[AppContext]) {

//   private def compareBytes(a: Array[Byte], b: Array[Byte]): Int = {
//     val len = Math.min(a.length, b.length)
//     for (i <- 0 until len) {
//       val diff = (a(i) & 0xff) - (b(i) & 0xff)
//       if (diff != 0) return diff
//     }
//     a.length - b.length
//   }

//   // format: [KEY 10B] [SPACE 1B] [VALUE 87B] [CRLF 2B]
//   // ASSUME:
//   // Since key is placed at the beginning of the record,
//   // we don't need to divided each record to extract the key for comparison.
//   // Just compare the whole 100 bytes!

//   def run(job: JobSpecMsg): IO[JobResult] = {
//     val inputpath = job.inputs.head.path // TODO: Assume single input
//     val outputpaths = job.outputs.map(_.path)
//     val program: IO[Unit] = for {
//       _ <- IO.println(s"[Sorting] Sorting job ${job.name} is started")
//       data <- fileStorage.readAll(inputpath)
//       records = data.grouped(100).toArray
//       sortedRecords = records.sortWith { (a, b) => compareBytes(a, b) < 0 }
//       _ <- fileStorage.writeAll(outputpaths.head, sortedRecords.flatten.toArray).void
//     } yield ()

//     program.timed.attempt
//       .map {
//         case Right((duration, _)) =>
//           println(s"[Sorting] Sorting job ${job.name} completed in ${duration.toMillis} ms")
//           JobResult(
//             success = true,
//             retval = None,
//             error = None,
//             stats = Some(JobExecutionStats(calculationTime = duration.toMillis.toInt))
//           )
//         case Left(err) =>
//           JobResult(
//             success = false,
//             retval = None,
//             error = Some(
//               WorkerError(
//                 kind = WorkerErrorKind.BODY_ERROR,
//                 inner = Some(JobSystemError(message = s"Sorting job failed: ${err.getMessage}"))
//               )
//             ),
//             stats = None
//           )
//       }
//   }
// }
