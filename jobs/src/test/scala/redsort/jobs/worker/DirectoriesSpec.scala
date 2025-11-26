package redsort.jobs.worker

import cats.effect._
import redsort.AsyncSpec
import fs2.io.file.Files
import fs2.io.file.Path
import redsort.jobs.context.impl.ProductionFileStorage
import java.io.File
import redsort.jobs.context.interface.FileStorage

class DirectoriesSpec extends AsyncSpec {
  def fixture = new {
    val root = Path("/root")
    val dir = new Directories(
      inputDirectories = Seq(root / "input1", root / "input2"),
      outputDirectory = root / "output",
      workingDirectory = root / "working"
    )

    val fileIO = stub[FileStorage]
  }

  behavior of "Directories.ensureDir"

  it should "create missing working and output directories" in {
    val f = fixture

    (f.fileIO.exists _).returns { p =>
      p match {
        case "/root/input1" | "/root/input2" => IO.pure(true)
        case _                               => IO.pure(false)
      }
    }
    (f.fileIO.mkDir _).returns(IO.pure(_))

    for {
      _ <- Directories.ensureDirs(f.dir, f.fileIO)
    } yield {
      (f.fileIO.mkDir _).calls.toSet should be(Set("/root/working", "/root/output"))
    }
  }

  it should "do nothing if directory already exists" in {
    val f = fixture

    (f.fileIO.exists _).returnsWith(IO.pure(true))

    for {
      _ <- Directories.ensureDirs(f.dir, f.fileIO)
    } yield {
      (f.fileIO.mkDir _).calls should be(List())
    }
  }

  behavior of "Directoreis.resolvePath"

  it should "convert @{input} to absolute path" in {
    val f = fixture
    Directories.resolvePath(f.dir, Path("@{input}/root/input1/a")).toString should be(
      "/root/input1/a"
    )
    Directories.resolvePath(f.dir, Path("@{input}/root/input2/b")).toString should be(
      "/root/input2/b"
    )
  }

  it should "convert @{working} to path of working directory" in {
    val f = fixture
    Directories.resolvePath(f.dir, Path("@{working}/a")).toString should be("/root/working/a")
  }

  it should "convert @{output} to path of output directory" in {
    val f = fixture
    Directories.resolvePath(f.dir, Path("@{output}/a")).toString should be("/root/output/a")
  }
}
