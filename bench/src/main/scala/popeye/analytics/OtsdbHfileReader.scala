package popeye.analytics

import java.io.File
import java.util
import java.util.Comparator

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.{KeyValue, CellUtil}
import org.apache.hadoop.hbase.io.hfile.{HFileScanner, CacheConfig, HFile, HFileReaderV2}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils
import popeye.storage.hbase.BytesKey
import scala.collection.mutable.ArrayBuffer
import scala.math.Ordering.comparatorToOrdering
import scala.collection.mutable

import scala.collection.mutable
import scala.util.{Success, Failure, Try}
import scala.collection.JavaConverters._

object OtsdbHfileReader {
  /** Number of LSBs in time_deltas reserved for flags.  */
  val FLAG_BITS: Short = 4


  case class QId(kind: String, id: BytesKey)

  case class IdMapping(metrics: Map[BytesKey, String], tagKeys: Map[BytesKey, String], tagValues: Map[BytesKey, String])

  case class IdMappingJava(metrics: util.Map[BytesKey, String],
                           tagKeys: util.Map[BytesKey, String],
                           tagValues: util.Map[BytesKey, String])

  case class ParsedRow(metric: String, baseTime: Int, tags: Seq[(String, String)])

  case class TsdbRowPointsNumber(metric: String, tags: String, baseTime: Int, points: Int)

  case class TsdbRowTags(metric: String, tags: String, tagKey: String, tagValue: String)


  def hadoopConfiguration(hadoopConfigurationPaths: Seq[String]) = {
    val conf = new Configuration()
    for (path <- hadoopConfigurationPaths) {
      conf.addResource(new File(path).toURI.toURL)
    }
    conf
  }

  def main(args: Array[String]) {
    val configuration = hadoopConfiguration(Seq("/etc/hadoop/conf/hdfs-site.xml", "/etc/hadoop/conf/core-site.xml"))
    val nameColumnFamilyPath = new Path("/user/quasi/korgen/39c6ec29b782ce2a8a1615d93fd022b0/name")
    //    val nameColumnFamilyPath = new Path("/home/quasi/programming/otsdb-analytics/name")
    //    val configuration = new Configuration()
    val fs = FileSystem.get(configuration)
    val cc = new CacheConfig(configuration)
    println("loading names...")
    val names = loadAllNames(fs, cc, nameColumnFamilyPath)
    println("names loaded")
    val sortedMetricIds = names.metrics.keys.toVector.sorted(Ordering.ordered[BytesKey])
    val metricId = sortedMetricIds(100)
    println(f"metricId: ${ Bytes.toStringBinary(metricId.bytes) }")
    val ranges = HFileRanges.getAllKeyRanges(fs, cc, new Path("/user/quasi/korgen/tsdb"))
    println(f"ranges:")
    ranges.foreach(println)

    val paths = ranges.filter(_._2.isPrefixInRange(metricId)).map(_._1)
    println(f"metrics.size = ${ names.metrics.size }")
    println("scannning...")
    val inspectorPointNumber = ObjectInspectorFactory.getReflectionObjectInspector(classOf[TsdbRowPointsNumber], ObjectInspectorOptions.JAVA)
    val inspectorTags = ObjectInspectorFactory.getReflectionObjectInspector(classOf[TsdbRowTags], ObjectInspectorOptions.JAVA)
    val typeString = "struct<metric:string,tags:string,tag_key:string,tag_value:string>"

    val typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(typeString)
    val oip = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(typeInfo)

    usingReaders(fs, cc, paths) {
      readers =>
        OtdsbOrcConverter.usingOrcWriters(Seq((new Path("number_of_points"), inspectorPointNumber), (new Path("tags"), oip))) {
          case Seq(pointsCountWriter, tagsWriter) =>
            val tagsSet = mutable.HashSet[String]()
            for (rowKVs <- filterRowsByPrefix(metricId, readers)) {
              val numberOfPoints = getNumberOfPointsInARow(rowKVs)
              val row = CellUtil.cloneRow(rowKVs(0))
              val ParsedRow(metric, baseTime, tags) = parseRow(row, names)
              val tagsString = tags.map { case (name, value) => f"$name=$value" }.mkString(",")
              pointsCountWriter.addRow(TsdbRowPointsNumber(metric, tagsString, baseTime, numberOfPoints))
              if (tagsSet.add(tagsString)) {
                for ((tagKey, tagValue) <- tags) {
                  tagsWriter.addRow(util.Arrays.asList(metric, tagsString, tagKey, tagValue))
                }
              }
            }
        }
    }
    println(f"metric id : ${ Bytes.toStringBinary(metricId.bytes) }")
    fs.close()
  }

  def filterRowsByPrefix(prefix: BytesKey, readers: Seq[HFile.Reader]) = {
    val scanners = readers.map(_.getScanner(false, false))
    val prefixLength = prefix.bytes.length
    scanners.foreach {
      scanner =>
        val arr = Array.ofDim[Byte](prefixLength + 2)
        val lengthBytes = Bytes.toBytes(prefixLength.toShort)
        System.arraycopy(lengthBytes, 0, arr, 0, 2)
        System.arraycopy(prefix.bytes, 0, arr, 2, prefixLength)
        val returnCode = scanner.seekTo(arr)
        println(f"ret code = $returnCode")
        if (returnCode == -1) {
          scanner.seekTo()
        }
    }
    getRowsIterator(scanners).takeWhile {
      rowKVs =>
        val keyValue = rowKVs.head
        Bytes.equals(prefix.bytes, 0, prefixLength, keyValue.getRowArray, keyValue.getRowOffset, prefixLength)
    }
  }

  def getUniqKVIterator(scanners: Seq[HFileScanner]) = {
    val iters = scanners.map(asKeyValueIterator(_).buffered)
    val ordering: Ordering[KeyValue] = comparatorToOrdering(KeyValue.COMPARATOR.asInstanceOf[Comparator[KeyValue]])
    val mergedIter = IteratorUtils.mergeSorted(iters)(ordering)
    IteratorUtils.uniqByOrdering(mergedIter)(ordering)
  }

  def getRowsIterator(scanners: Seq[HFileScanner]) = {
    val uniqIter = getUniqKVIterator(scanners)
    asRowIterator(uniqIter)
  }

  def parseRow(row: Array[Byte], idMapping: IdMapping) = {
    val metricId = util.Arrays.copyOfRange(row, 0, 3)
    val baseTimeOffset = 3
    val baseTime = Bytes.toInt(row, 3)
    val tagsOffset = baseTimeOffset + 4
    require((row.length - tagsOffset) % 6 == 0)
    val numberOfTags = (row.length - tagsOffset) / 6
    val tags =
      for (i <- 0 until numberOfTags) yield {
        val tagNameOffset = tagsOffset + i * 6
        val tagKeyId = util.Arrays.copyOfRange(row, tagNameOffset, tagNameOffset + 3)
        val tagValueId = util.Arrays.copyOfRange(row, tagNameOffset + 3, tagNameOffset + 6)
        val tagKey = idMapping.tagKeys(new BytesKey(tagKeyId))
        val tagValue = idMapping.tagValues(new BytesKey(tagValueId))
        (tagKey, tagValue)
      }
    val metric = idMapping.metrics(new BytesKey(metricId))
    ParsedRow(metric, baseTime, tags)
  }

  def printKeyValues(scanner: HFileScanner) {
    while(true) {
      val keyValue = scanner.getKeyValue
      val namedArrays = List(
        "key" -> CellUtil.cloneRow(keyValue),
        "family" -> CellUtil.cloneFamily(keyValue),
        "qual" -> CellUtil.cloneQualifier(keyValue),
        "value" -> CellUtil.cloneValue(keyValue)
      )
      val line = for ((name, array) <- namedArrays) yield {
        f"$name = ${ Bytes.toStringBinary(array) }"
      }
      println(line.mkString(" "))
      if (!scanner.next()) {
        return
      }
    }
  }

  def loadAllNames(fs: FileSystem, cc: CacheConfig, nameColumnPath: Path): IdMapping = {
    val hFiles = fs.listStatus(nameColumnPath).toList.map(_.getPath)
    usingReaders(fs, cc, hFiles) {
      readers =>
        val scanners = readers.map(_.getScanner(false, false))
        scanners.foreach(_.seekTo())
        val kvIterator = getUniqKVIterator(scanners)
        parseNames(kvIterator)
    }
  }

  def loadAllNamesJava(fs: FileSystem, cc: CacheConfig, nameColumnPath: Path): IdMappingJava = {
    val names = loadAllNames(fs, cc, nameColumnPath)
    IdMappingJava(
      metrics = names.metrics.asJava,
      tagKeys = names.metrics.asJava,
      tagValues = names.metrics.asJava
    )
  }

  def usingReaders[A](fs: FileSystem, cc: CacheConfig, hFilesPaths: Seq[Path])(operation: Seq[HFile.Reader] => A) = {
    val readerTrys = hFilesPaths.map(path => Try(HFile.createReader(fs, path, cc)))
    val failedCreation = readerTrys.collect { case Failure(t) => t }.headOption
    for (fail <- failedCreation) {
      readerTrys.collect { case Success(reader) => reader }.foreach(_.close())
      throw fail
    }
    val readers = readerTrys.map(_.get)
    try {
      operation(readers)
    } finally {
      readers.foreach(_.close())
    }
  }

  def parseNames(idColumnFamilyKVIterator: Iterator[KeyValue]): IdMapping = {
    val names = Map(
      "metrics" -> mutable.ArrayBuffer[(BytesKey, String)](),
      "tagk" -> mutable.ArrayBuffer[(BytesKey, String)](),
      "tagv" -> mutable.ArrayBuffer[(BytesKey, String)]()
    )
    for (keyValue <- idColumnFamilyKVIterator) {
      val kind = Bytes.toString(CellUtil.cloneQualifier(keyValue))
      val id = new BytesKey(CellUtil.cloneRow(keyValue))
      val name = Bytes.toString(CellUtil.cloneValue(keyValue))
      names(kind) += ((id, name))
    }
    val nameMappings = names.mapValues(_.toMap)
    IdMapping(
      metrics = nameMappings("metrics"),
      tagKeys = nameMappings("tagk"),
      tagValues = nameMappings("tagv")
    )
  }

  def getHFileReader(path: Path, configuration: Configuration) = {
    val fs = FileSystem.get(configuration)
    val cc = new CacheConfig(configuration)
    HFile.createReader(fs, path, cc)
  }

  private def parseValue(qualifierBytes: Array[Byte], valueBytes: Array[Byte]): (Short, Either[Long, Float]) = {
    /**
     * When this bit is set, the value is a floating point value.
     * Otherwise it's an integer value.
     */
    val FLAG_FLOAT: Short = 0x8
    require(
      qualifierBytes.length == Bytes.SIZEOF_SHORT,
      s"Expected qualifier length was ${ Bytes.SIZEOF_SHORT }, got ${ qualifierBytes.length }"
    )
    val qualifier = Bytes.toShort(qualifierBytes)
    val deltaShort = ((qualifier & 0xFFFF) >>> FLAG_BITS).toShort
    val floatFlag: Int = FLAG_FLOAT | 0x3.toShort
    val isFloatValue = (qualifier & floatFlag) == floatFlag
    val isIntValue = (qualifier & 0x8) == 0
    if (isFloatValue) {
      (deltaShort, Right(Bytes.toFloat(valueBytes)))
    } else if (isIntValue) {
      (deltaShort, Left(Bytes.toLong(valueBytes)))
    } else {
      throw new IllegalArgumentException("Neither int nor float values set on point")
    }
  }

  def asKeyValueIterator(scanner: HFileScanner): Iterator[KeyValue] = new Iterator[KeyValue] {
    var _hasNext = true

    override def hasNext: Boolean = _hasNext

    override def next(): KeyValue = {
      val keyValue = scanner.getKeyValue
      _hasNext = scanner.next()
      keyValue
    }
  }

  def asRowIterator(iter: Iterator[KeyValue]) = {
    def isSameRow(leftKV: KeyValue, rightKV: KeyValue) = {
      Bytes.equals(
        leftKV.getRowArray,
        leftKV.getRowOffset,
        leftKV.getRowLength,
        rightKV.getRowArray,
        rightKV.getRowOffset,
        rightKV.getRowLength
      )
    }
    IteratorUtils.groupByPredicate(iter, isSameRow)
  }

  def getNumberOfPointsInARow(row: Seq[KeyValue]) = {
    val deltaTimes = row.map {
      keyValue =>
        val qLength = keyValue.getQualifierLength
        require(qLength % 2 == 0)
        val numberOfPointsInKv = qLength / 2
        val qualArray = keyValue.getQualifierArray
        val qualOffset = keyValue.getQualifierOffset
        for (i <- 0 until numberOfPointsInKv) yield {
          val qualShort = Bytes.toShort(qualArray, qualOffset + i * 2)
          (qualShort & 0xFFFF) >>> FLAG_BITS
        }
    }
    deltaTimes.toSet.size
  }

  class RowIterator(kvIter: Iterator[KeyValue]) extends Iterator[ArrayBuffer[KeyValue]] {

    var currentRowKVs = ArrayBuffer[KeyValue]()
    var currentRow = {
      val kv = kvIter.next()
      currentRowKVs += kv
      CellUtil.cloneRow(kv)
    }

    override def hasNext: Boolean = currentRowKVs.nonEmpty

    override def next(): ArrayBuffer[KeyValue] = {
      if (kvIter.hasNext) {
        var kv = kvIter.next()
        while(Bytes.equals(currentRow, 0, currentRow.length, kv.getRowArray, kv.getRowOffset, kv.getRowLength) && kvIter.hasNext) {
          currentRowKVs += kv
          kv = kvIter.next()
        }
        val row = currentRowKVs
        if (!kvIter.hasNext && Bytes.equals(currentRow, 0, currentRow.length, kv.getRowArray, kv.getRowOffset, kv.getRowLength)) {
          // last row
          currentRowKVs = ArrayBuffer[KeyValue]()
        } else {
          // non last row
          currentRowKVs = ArrayBuffer[KeyValue](kv)
        }
        currentRow = CellUtil.cloneRow(kv)
        row
      } else {
        val row = currentRowKVs
        currentRowKVs = ArrayBuffer[KeyValue]()
        row
      }
    }
  }


}
