package redsort

import redsort.jobs.context.impl.ProductionFileStorage
import cats.effect._
import fs2._
import fs2.io.file.{Files, Path}
import java.io.FileNotFoundException
import redsort.AsyncSpec

class LocalFileStorage extends ProductionFileStorage

class ProductionFileStorageSpec extends AsyncSpec {

  def withStorage(testCode: (ProductionFileStorage, String) => IO[Unit]): IO[Unit] = {
    val storage = new LocalFileStorage()

    Files[IO]
      .tempDirectory(Some(Path("/tmp")), "redsort-test-", None)
      .use { tempPath =>
        testCode(storage, tempPath.absolute.toString)
      }
  }

  "LocalFileStorage" should "write and read a file correctly using streams" in {
    val content = "Hello World!".getBytes

    withStorage { (storage, root) =>
      val path = s"$root/stream.txt"
      for {
        _ <- Stream.emits(content).through(storage.write(path)).compile.drain
        readBytes <- storage.read(path).compile.to(Array)
      } yield {
        readBytes shouldBe content
      }
    }
  }

  it should "write and read a file correctly using readAll and writeAll" in {
    val content = "Hello World!".getBytes

    withStorage { (storage, root) =>
      val path = s"$root/all.txt"
      for {
        _ <- storage.writeAll(path, content)
        readBytes <- storage.readAll(path)
      } yield {
        readBytes shouldBe content
      }
    }
  }

  it should "check existence of files correctly" in {
    val content = "Hello World!".getBytes

    withStorage { (storage, root) =>
      val path = s"$root/exist.txt"
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
    val content = "Hello World!".getBytes

    withStorage { (storage, root) =>
      val path = s"$root/delete.txt"
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

  it should "delete nonempty directory recursively" in {
    val content = "hello".getBytes

    withStorage { (storage, root) =>
      val dirPath = s"$root/deleteMe/"
      val path = dirPath + "a"
      for {
        _ <- storage.mkDir(dirPath)
        _ <- storage.writeAll(path, content)
        _ <- storage.deleteRecursively(dirPath)
        exists <- storage.exists(path)
      } yield {
        exists shouldBe false
      }
    }
  }

  it should "rename a file correctly" in {
    val content = "Hello World!".getBytes

    withStorage { (storage, root) =>
      val beforePath = s"$root/before.txt"
      val afterPath = s"$root/after.txt"
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
    withStorage { (storage, root) =>
      val invalidPath = s"$root/non-existent"
      val targetPath = s"$root/target"

      storage
        .rename(invalidPath, targetPath)
        .attempt
        .map { result =>
          result should matchPattern { case Left(_: java.nio.file.NoSuchFileException) => }
        }
    }
  }

  it should "list files with a given prefix" in {
    withStorage { (storage, root) =>
      val dir = s"$root/folder"
      val file1 = s"$root/folder/file1.txt"
      val file2 = s"$root/folder/file2.txt"
      val other = s"$root/other/file3.txt"

      for {
        _ <- storage.writeAll(file1, Array.empty)
        _ <- storage.writeAll(file2, Array.empty)
        _ <- storage.writeAll(other, Array.empty)

        files <- storage.list(dir).map(_.keys.toList)
      } yield {
        files should contain theSameElementsAs Seq(file1, file2)
      }
    }
  }

  it should "return correct file size" in {
    val content = Array[Byte](1, 2, 3, 4, 5)

    withStorage { (storage, root) =>
      val path = s"$root/size.txt"
      for {
        _ <- storage.writeAll(path, content)
        size <- storage.fileSize(path)
      } yield {
        size shouldBe 5L
      }
    }
  }

  it should "raise FileNotFoundException when reading non-existent file" in {
    withStorage { (storage, root) =>
      val path = s"$root/ghost.txt"
      storage.read(path).compile.drain.attempt.map { result =>
        result should matchPattern { case Left(_: FileNotFoundException) => }
      }
    }
  }
}
