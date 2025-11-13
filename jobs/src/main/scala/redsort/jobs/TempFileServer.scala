package redsort.jobs.fileserver

import scala.sys.process._
import java.io._
import scala.collection.mutable.ArrayBuffer

import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters._

trait FileStorage {
  // Basic File Operations
  // The names of methods related stream are different with original ones.
  // This is to avoid ambiguity compile errors. 
  def inputStream(fileName: String): InputStream
  def outputStream(fileName: String): OutputStream
  def readNbytesFromFile(fileName: String, n: Int): Option[Array[Byte]]
  def renameFile(oldName: String, newName: String): Boolean
  def removeFile(fileName: String): Boolean
  def listFiles(): List[String]
  def fileSize(fileName: String): Option[Long]

  // Debug
  // In production, these methods may be dangerous for large files.
  def writeFile(fileName: String, data: Array[Byte]): Boolean
  def readFile(fileName: String): Option[Array[Byte]]
}

class InMemoryFileStorage extends FileStorage {

  private val fileStore: scala.collection.concurrent.Map[String, Array[Byte]] =
    new ConcurrentHashMap[String, Array[Byte]]().asScala

  override def inputStream(fileName: String): InputStream = {
    fileStore.get(fileName) match {
      case Some(data) => new ByteArrayInputStream(data)
      case None       => throw new FileNotFoundException(s"File not found in memory: $fileName")
    }
  }

  override def outputStream(fileName: String): OutputStream = {
    new ByteArrayOutputStream() {
      override def close(): Unit = {
        try {
          fileStore.put(fileName, this.toByteArray)
        } finally {
          super.close()
        }
      }
    }
  }

  // Debug!
  override def writeFile(fileName: String, data: Array[Byte]): Boolean = {
    fileStore.put(fileName, data)
    true
  }
  // Debug!
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

  override def listFiles(): List[String] = {
    fileStore.keys.toList
  }

  override def fileSize(fileName: String): Option[Long] = {
    fileStore.get(fileName).map(_.length.toLong)
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
