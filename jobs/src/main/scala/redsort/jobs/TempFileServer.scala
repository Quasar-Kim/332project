package redsort.jobs.fileserver

import scala.sys.process._
import java.io._
import scala.collection.mutable.ArrayBuffer

import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters._

trait FileStorage {
  def writeFile(fileName: String, data: Array[Byte]): Unit
  def readFile(fileName: String): Option[Array[Byte]]
  def readNbytesFromFile(fileName: String, n: Int): Option[Array[Byte]]
  def renameFile(oldName: String, newName: String): Boolean
  def removeFile(fileName: String): Boolean
}

class InMemoryFileStorage extends FileStorage {

  private val fileStore: scala.collection.concurrent.Map[String, Array[Byte]] =
    new ConcurrentHashMap[String, Array[Byte]]().asScala

  override def writeFile(fileName: String, data: Array[Byte]): Unit = {
    fileStore.put(fileName, data)
  }

  override def readFile(fileName: String): Option[Array[Byte]] = {
    fileStore.get(fileName)
  }

  override def readNbytesFromFile(fileName: String, n: Int): Option[Array[Byte]] = {
    fileStore.get(fileName).map(_.take(n))
  }

  override def renameFile(oldName: String, newName: String): Boolean = {
    fileStore.synchronized {
      fileStore.get(oldName) match {
        case Some(data) =>
          fileStore.remove(oldName)
          fileStore.put(newName, data)
          true
        case None =>
          false
      }
    }
  }

  override def removeFile(fileName: String): Boolean = {
    fileStore.synchronized {
      fileStore.remove(fileName).isDefined
    }
  }

  // Only for debug!
  def clear(): Unit = {
    fileStore.clear()
  }

  // Only for debug!
  def setup(): Unit = {
    // For a test, initially read "./test_input/50MB.txt" file and store in-memory.
    val CWD: String = "pwd".!!
    println(s"Current working directory: ${CWD}")

    val fileRelativePath = "./test_input/50MB.txt"
    val file = new File(fileRelativePath)
    val fileName = file.getName
    val fileAbsolutePath = file.getAbsolutePath
    val fis = new FileInputStream(file)
    val buffer = ArrayBuffer[Byte]()
    val byteArray = new Array[Byte](1024 * 1024) // 1 MB buffer
    var bytesRead = 0
    while (bytesRead != -1) {
      bytesRead = fis.read(byteArray)
      if (bytesRead > 0) {
        buffer ++= byteArray.take(bytesRead)
      }
    }
    writeFile(fileName, buffer.toArray)

    // debug
    println(s"FileName: $fileName")
    println(s"FilePointer: ${readNbytesFromFile(fileName, 1000)}")
    val contentSampleOpt: Option[Array[Byte]] = readNbytesFromFile(fileName, 1000)
    val contentStringOpt: Option[String] = contentSampleOpt.map { byteArray =>
      new String(byteArray, "UTF-8")
    }
    println(s"FileContent: ${contentStringOpt}")

  }
}

object FileServer extends InMemoryFileStorage
