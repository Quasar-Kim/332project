package redsort.worker.handlers

import cats.effect._
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import fs2.{Pipe, Stream}
import fs2.io.file.{Files, Path}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import redsort.jobs.Common.FileEntry
import redsort.jobs.worker.Directories
import redsort.worker.gensort.gensort
import redsort.worker.handlers._

import redsort.jobs.context._
import redsort.jobs.context.interface._
import redsort.jobs.context.impl._

import redsort.worker.testctx._
import com.google.protobuf.ByteString
import redsort.jobs.messages.LongArg
import com.google.protobuf.any

class MergeJobHandlerSpec extends AsyncFlatSpec with AsyncIOSpec with Matchers {
  def fixture = new {
    def withCtx(body: (WorkerTestCtx) => IO[Unit]): IO[Unit] = {
      for {
        fs <- IO.ref[Map[String, Array[Byte]]](Map.empty)
        ctx = new WorkerTestCtx(fs)
        _ <- body(ctx)
      } yield ()
    }

    def prepareSortedFile(path: Path, data: Array[Byte], ctx: FileStorage): IO[Path] = {
      val tmpPath = Path("/tmp") / path
      for {
        _ <- ctx.writeAll(tmpPath.toString, data)
        _ <- (new SortJobHandler).apply(
          args = Seq(any.Any.pack(new LongArg(128 * 1000 * 1000))),
          inputs = Seq(tmpPath),
          outputs = Seq(path),
          ctx = ctx,
          d = null
        )
      } yield path
    }
  }

  "MergeJobHandler" should "exactly merge 10,000 records (1MB)" in {
    val f = fixture

    val recordsPerFile = 10000
    val input1Data = gensort.generate(recordsPerFile)
    val input2Data = gensort.generate(recordsPerFile)
    val data =
      input1Data.grouped(100).map(_.toList).toSet ++ input2Data.grouped(100).map(_.toList).toSet
    val sortinputStr = "/data/input1"
    val sortinputStr2 = "/data/input2"
    val sortoutputStr = "/data/output1"
    val sortoutputStr2 = "/data/output2"
    val mergedoutputStr = "/data/mergedoutput"
    val dummyDirs: Directories = null

    f.withCtx { ctx =>
      val sorter = new SortJobHandler()
      val merger = new MergeJobHandler()
      for {
        _ <- ctx.writeAll(sortinputStr, input1Data)
        _ <- ctx.writeAll(sortinputStr2, input2Data)

        _ <- sorter.apply(
          args = Seq(any.Any.pack(new LongArg(128 * 1000 * 1000))),
          inputs = Seq(Path(sortinputStr)),
          outputs = Seq(Path(sortoutputStr)),
          ctx = ctx,
          d = dummyDirs
        )
        _ <- sorter.apply(
          args = Seq(any.Any.pack(new LongArg(128 * 1000 * 1000))),
          inputs = Seq(Path(sortinputStr2)),
          outputs = Seq(Path(sortoutputStr2)),
          ctx = ctx,
          d = dummyDirs
        )
        _ <- merger.apply(
          args = Seq(any.Any.pack(new LongArg(128 * 1000 * 1000))),
          inputs = Seq(Path(sortoutputStr), Path(sortoutputStr2)),
          outputs = Seq(Path(mergedoutputStr)),
          ctx = ctx,
          d = dummyDirs
        )

        middleBytes1 <- ctx.readAll(sortoutputStr)
        middleBytes2 <- ctx.readAll(sortoutputStr2)

        outputSize <- ctx.fileSize(mergedoutputStr)
        outputBytes <- ctx.readAll(mergedoutputStr)

      } yield {
        outputSize shouldBe (recordsPerFile * 2 * 100) // 200 records, 100 bytes each
        outputBytes.takeRight(2) shouldBe Array('\r'.toByte, '\n'.toByte)
        gensort.validate(middleBytes1) shouldBe true
        gensort.validate(middleBytes2) shouldBe true
        gensort.validate(outputBytes) shouldBe true
        outputBytes.grouped(100).map(_.toList).toSet shouldBe data
      }
    }
  }

  it should "handle empty input" in {
    val f = fixture

    f.withCtx { ctx =>
      for {
        _ <- (new MergeJobHandler).apply(
          args = Seq(any.Any.pack(new LongArg(128 * 1000 * 1000))),
          inputs = Seq.empty,
          outputs = Seq.empty,
          ctx = ctx,
          d = null
        )
      } yield ()
    }
  }

  it should "handle only one input file" in {
    val f = fixture
    val inputData = gensort.generate(100)

    f.withCtx { ctx =>
      for {
        // prepare input file by sorting input files
        _ <- f.prepareSortedFile(Path("/input/a"), inputData, ctx)

        _ <- (new MergeJobHandler).apply(
          args = Seq(any.Any.pack(new LongArg(128 * 1000 * 1000))),
          inputs = Seq(
            Path("/input/a")
          ),
          outputs = Seq(
            Path("/output/b")
          ),
          ctx = ctx,
          d = null
        )

        contents <- ctx.readAll("/output/b")
        expectedContents <- ctx.readAll("/input/a")
      } yield {
        contents shouldBe expectedContents
      }
    }
  }

  it should "output files with max size passed as argument" in {
    val f = fixture
    val inputData = gensort.generate(100) // 10KB

    f.withCtx { ctx =>
      for {
        _ <- f.prepareSortedFile(Path("/input/a"), inputData, ctx)

        _ <- (new MergeJobHandler).apply(
          args = Seq(
            any.Any.pack(new LongArg(5000)) // 5KB
          ),
          inputs = Seq(
            Path("/input/a")
          ),
          outputs = Seq(
            Path("/output/b.0"),
            Path("/output/b.1")
          ),
          ctx = ctx,
          d = null
        )

        contentsZero <- ctx.readAll("/output/b.0")
        contentsOne <- ctx.readAll("/output/b.1")
        expectedContents <- ctx.readAll("/input/a")
      } yield {
        val contents = contentsZero.concat(contentsOne)
        contents shouldBe expectedContents
      }
    }
  }

  // regression: https://github.com/Quasar-Kim/332project/issues/34
  it should "merge two files with sizes 5500 and 4500" in {
    val f = fixture
    val input1Data = gensort.generate(55)
    val input2Data = gensort.generate(45)
    val mergedData = input1Data.concat(input2Data)

    f.withCtx { ctx =>
      for {
        // prepare input file by sorting input files
        _ <- f.prepareSortedFile(Path("/input/a"), input1Data, ctx)
        _ <- f.prepareSortedFile(Path("/input/b"), input2Data, ctx)
        _ <- f.prepareSortedFile(Path("/sorted"), mergedData, ctx)

        _ <- (new MergeJobHandler).apply(
          args = Seq(
            any.Any.pack(new LongArg(128 * 1000 * 1000)) // 128MB
          ),
          inputs = Seq(
            Path("/input/a"),
            Path("/input/b")
          ),
          outputs = Seq(
            Path("/output/c")
          ),
          ctx = ctx,
          d = null
        )

        contents <- ctx.readAll("/output/c")
        expectedContents <- ctx.readAll("/sorted")
      } yield {
        ByteString.copyFrom(contents) shouldBe ByteString.copyFrom(expectedContents)
      }
    }
  }
}
