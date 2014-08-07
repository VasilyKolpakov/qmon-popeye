package popeye.analytics.hadoop

import java.io.{DataOutput, DataInput}
import java.util

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.{CellUtil, KeyValue}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.io.hfile.{CacheConfig, HFileScanner, HFile}
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.codehaus.jackson.map.ObjectMapper

object HFileInputFormat {
  val splitsKey = "popeye.analytics.hadoop.HFileInputFormat.splits"

  def setInputPaths(job: Job, files: Seq[HDFSFileMeta]) = {
    job.getConfiguration.set(splitsKey, HDFSFileMeta.serializeSeqToString(files))
  }
}

class HFileInputFormat extends InputFormat[ImmutableBytesWritable, KeyValue] {
  override def getSplits(context: JobContext): util.List[InputSplit] = {
    import scala.collection.JavaConverters._
    val splitsString = context.getConfiguration.get(HFileInputFormat.splitsKey)
    val metas = HDFSFileMeta.deserializeSeqFromString(splitsString)
    metas.map(m => new FileInputSplit(m).asInstanceOf[InputSplit]).asJava
  }

  override def createRecordReader(split: InputSplit,
                                  context: TaskAttemptContext): RecordReader[ImmutableBytesWritable, KeyValue] = {
    new HFileRecordReader
  }

  class HFileRecordReader extends RecordReader[ImmutableBytesWritable, KeyValue] {
    var reader: HFile.Reader = null
    var scanner: HFileScanner = null
    var entryNumber = 0
    var numberOfEntries = 0l

    override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {
      val hFileInputSplit = split.asInstanceOf[FileInputSplit]
      val path = hFileInputSplit.getPath
      val fs = org.apache.hadoop.fs.FileSystem.get(context.getConfiguration)
      reader = HFile.createReader(fs, path, new CacheConfig(context.getConfiguration))
      scanner = reader.getScanner(false, false)
      numberOfEntries = reader.getEntries
    }

    override def getProgress: Float = entryNumber / numberOfEntries

    override def nextKeyValue(): Boolean = {
      entryNumber += 1
      if (!scanner.isSeeked)
      // Had to move this here because "nextKeyValue" is called before the first getCurrentKey
      // which was causing us to miss the first row of the HFile.
        scanner.seekTo()
      else {
        scanner.next()
      }
    }

    override def getCurrentValue: KeyValue = scanner.getKeyValue

    override def getCurrentKey: ImmutableBytesWritable = {
      val row = CellUtil.cloneRow(scanner.getKeyValue)
      new ImmutableBytesWritable(row)
    }

    override def close(): Unit = {
      Option(reader).foreach(_.close())
    }
  }

}

object HDFSFileMeta {

  def serializeToString(split: HDFSFileMeta): String = {
    import scala.collection.JavaConverters._
    val mapper = new ObjectMapper
    val javaMap = Map(
      "path" -> split.path.toString,
      "length" -> split.length,
      "locations" -> split.locations.mkString(",")
    ).asJava
    mapper.writeValueAsString(javaMap)
  }

  def serializeSeqToString(splits: Seq[HDFSFileMeta]): String = {
    import scala.collection.JavaConverters._
    val mapper = new ObjectMapper
    val javaList = splits.map(serializeToString).asJava
    mapper.writeValueAsString(javaList)
  }

  def deserializeSeqFromString(string: String): Seq[HDFSFileMeta] = {
    import scala.collection.JavaConverters._
    val mapper = new ObjectMapper
    val tree = mapper.readTree(string)
    val strings = tree.asScala.toList.map(_.getTextValue)
    strings.map(deserializeFromString)
  }

  def deserializeFromString(string: String): HDFSFileMeta = {
    val mapper = new ObjectMapper
    val tree = mapper.readTree(string)
    val path = new Path(tree.get("path").getTextValue)
    val length = tree.get("length").getLongValue
    val locations = tree.get("locations").getTextValue.split(",").filter(_.nonEmpty).toVector
    HDFSFileMeta(path, length, locations)
  }
}

case class HDFSFileMeta(path: Path, length: Long, locations: Vector[String])

class FileInputSplit(var data: HDFSFileMeta) extends InputSplit with Writable {
  // default constructor is required for Wirtable implementations
  def this() = this(null)

  def getPath: Path = data.path

  override def getLength: Long = data.length

  override def getLocations: Array[String] = data.locations.toArray

  override def write(out: DataOutput): Unit = {
    out.writeUTF(HDFSFileMeta.serializeToString(data))
  }

  override def readFields(in: DataInput): Unit = {
    data = HDFSFileMeta.deserializeFromString(in.readUTF())
  }
}




