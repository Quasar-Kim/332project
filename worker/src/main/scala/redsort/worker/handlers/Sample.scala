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

// class JobSampler(fileStorage: FileStorage[AppContext]) {
//   def run(job: JobSpecMsg): IO[JobResult] = {
//     val RECORD_SIZE = 100L // 100 bytes / record
//     val NUM_RECORDS = 10000L
//     val SAMPLING_SIZE = RECORD_SIZE * NUM_RECORDS // 1 MB
//     val inputpath = job.inputs.head.path // Just use the first for sampling
//     val outputpaths = job.outputs.map(_.path)
//     val program: IO[Unit] = for {
//       _ <- IO.println(s"[Sampling] Sampling job ${job.name} is started")
//       _ <- fileStorage
//         .read(inputpath)
//         .take(SAMPLING_SIZE)
//         .through(fileStorage.write(outputpaths.head))
//         .compile
//         .drain
//     } yield ()

//     program.timed.attempt
//       .map {
//         case Right((duration, _)) =>
//           println(s"[Sampling] Sampling job ${job.name} completed in ${duration.toMillis} ms")
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
//                 inner = Some(JobSystemError(message = s"Sampling job failed: ${err.getMessage}"))
//               )
//             ),
//             stats = None
//           )
//       }
//   }
// }
