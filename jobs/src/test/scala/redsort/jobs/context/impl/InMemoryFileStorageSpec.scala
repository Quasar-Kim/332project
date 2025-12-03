package redsort

import redsort.jobs.context.SchedulerCtx
import org.scalamock.stubs.CatsEffectStubs
import scala.concurrent.duration._

import redsort.jobs.context.impl.InMemoryFileStorage
import cats.effect._
import fs2._
import java.io.FileNotFoundException

class InMemoryFileStorageSpec extends AsyncSpec {

  def withStorage(testCode: InMemoryFileStorage => IO[Unit]) = {
    for {
      ref <- Ref.of[IO, Map[String, Array[Byte]]](Map.empty)
      storage = new InMemoryFileStorage(ref)
      _ <- testCode(storage)
    } yield ()
  }

  "InMemoryFileStorage" should "write and read a file correctly using streams" in {
    val path = "/tmp/test/stream.txt"
    val content = "Hello World!".getBytes

    withStorage { storage =>
      for {
        _ <- Stream.emits(content).through(storage.write(path)).compile.drain
        readBytes <- storage.read(path).compile.to(Array)
      } yield {
        readBytes shouldBe content
      }
    }
  }

  it should "write and read a file correctly using readAll and writeAll" in {
    val path = "/tmp/test/all.txt"
    val content = "Hello World!".getBytes

    withStorage { storage =>
      for {
        _ <- storage.writeAll(path, content)
        readBytes <- storage.readAll(path)
      } yield {
        readBytes shouldBe content
      }
    }
  }

  it should "check existence of files correctly" in {
    val path = "/tmp/test/exist.txt"
    val content = "Hello World!".getBytes

    withStorage { storage =>
      for {
        existsBefore <- storage.exists(path)
        _ <- storage.writeAll(path, content)
        existsAfter <- storage.exists(path)
      } yield {
        existsBefore shouldBe false
        existsAfter shouldBe true
      }
    }
  }

  it should "delete a file correctly" in {
    val path = "/tmp/test/delete.txt"
    val content = "Hello World!".getBytes

    withStorage { storage =>
      for {
        _ <- storage.writeAll(path, content)
        existsBefore <- storage.exists(path)
        _ <- storage.delete(path)
        existsAfter <- storage.exists(path)
      } yield {
        existsBefore shouldBe true
        existsAfter shouldBe false
      }
    }
  }

  it should "delete nonempty directory" in {
    val dirPath = "/tmp/deleteMe/"
    val path = dirPath + "a"
    val content = "hello".getBytes

    withStorage { storage =>
      for {
        _ <- storage.writeAll(path, content)
        _ <- storage.deleteRecursively(dirPath)
        exists <- storage.exists(path)
      } yield {
        exists shouldBe false
      }
    }
  }

  it should "rename a file correctly" in {
    val beforePath = "/tmp/test/before.txt"
    val afterPath = "/tmp/test/after.txt"
    val content = "Hello World!".getBytes

    withStorage { storage =>
      for {
        _ <- storage.writeAll(beforePath, content)
        _ <- storage.rename(beforePath, afterPath)
        existsBefore <- storage.exists(beforePath)
        existsAfter <- storage.exists(afterPath)
        readBytes <- storage.readAll(afterPath)
      } yield {
        existsBefore shouldBe false
        existsAfter shouldBe true
        readBytes shouldBe content
      }
    }
  }

  it should "fail to rename if source file does not exist" in {
    withStorage { storage =>
      storage
        .rename("non-existent", "target")
        .attempt
        .map { result =>
          result should matchPattern { case Left(_: FileNotFoundException) => }
        }
    }
  }

  it should "list files with a given prefix" in {
    withStorage { storage =>
      for {
        _ <- storage.writeAll("/tmp/folder/file1.txt", Array.empty)
        _ <- storage.writeAll("/tmp/folder/file2.txt", Array.empty)
        _ <- storage.writeAll("/tmp/other/file3.txt", Array.empty)

        files <- storage.list("/tmp/folder").map(_.keys.toList)
      } yield {
        files should contain theSameElementsAs Seq("/tmp/folder/file1.txt", "/tmp/folder/file2.txt")
      }
    }
  }

  it should "return correct file size" in {
    val path = "/tmp/test/size.txt"
    val content = Array[Byte](1, 2, 3, 4, 5)

    withStorage { storage =>
      for {
        _ <- storage.writeAll(path, content)
        size <- storage.fileSize(path)
      } yield {
        size shouldBe 5L
      }
    }
  }

  it should "raise FileNotFoundException when reading non-existent file" in {
    withStorage { storage =>
      storage.read("/tmp/ghost.txt").compile.drain.attempt.map { result =>
        result should matchPattern { case Left(_: FileNotFoundException) => }
      }
    }
  }
}
