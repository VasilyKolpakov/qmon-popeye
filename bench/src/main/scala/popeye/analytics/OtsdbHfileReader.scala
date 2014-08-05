package popeye.analytics

import java.util
import java.util.Comparator

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.{KeyValue, CellUtil}
import org.apache.hadoop.hbase.io.hfile.{HFileScanner, CacheConfig, HFile, HFileReaderV2}
import org.apache.hadoop.hbase.util.Bytes
import popeye.storage.hbase.BytesKey
import scala.collection.mutable.ArrayBuffer
import scala.math.Ordering.comparatorToOrdering

import scala.collection.mutable

object OtsdbHfileReader {
  /** Number of LSBs in time_deltas reserved for flags.  */
  val FLAG_BITS: Short = 4


  case class QId(kind: String, id: BytesKey)

  case class IdMapping(metrics: Map[BytesKey, String], tagKeys: Map[BytesKey, String], tagValues: Map[BytesKey, String])

  def main(args: Array[String]) {
    val paths = Seq(
      //      new Path("/home/quasi/programming/otsdb-analytics/229045a4f13f48899e88158379750d82"),
      //      new Path("/home/quasi/programming/otsdb-analytics/229045a4f13f48899e88158379750d82"),
      new Path("/home/quasi/programming/otsdb-analytics/229045a4f13f48899e88158379750d82")
    )
    val configuration: Configuration = new Configuration
    val readers = paths.map(path => getHFileReader(path, configuration))
    val scanners = readers.map(_.getScanner(false, false))
    scanners.foreach(_.seekTo())
    val iters = scanners.map(asKeyValueIterator(_).buffered)
    val ordering: Ordering[KeyValue] = comparatorToOrdering(KeyValue.COMPARATOR.asInstanceOf[Comparator[KeyValue]])
    val mergedIter = IteratorUtils.mergeSorted(iters)(ordering)
    val uniqIter = IteratorUtils.uniqByOrdering(mergedIter)(ordering)
    val rowIter = asRowIterator(uniqIter)
    val names = loadAllNames(configuration)
    var maxPoints = 0
    for (rowKVs <- rowIter) {
      val numberOfPoints = getNumberOfPointsInARow(rowKVs)
      val row = CellUtil.cloneRow(rowKVs(0))
      val hrString = rowToHumanReadableString(row, names)
      maxPoints = math.max(maxPoints, numberOfPoints)
      println(f"$hrString $numberOfPoints")
    }
    println(uniqIter.size)
    println(f"max points: $maxPoints")

    //    printPoints(scanner, names)
    //    printKeyValues(scanner)
    readers.foreach(_.close())
  }

  def printPoints(scanner: HFileScanner, names: Map[QId, String]) {
    val groupedNames = names
      .groupBy { case (QId(kind, id), name) => kind }
      .mapValues(_.map { case (QId(kind, id), name) => (id, name) }.toMap)
    val metrics = groupedNames("metrics")
    val tagKeys = groupedNames("tagk")
    val tagValues = groupedNames("tagv")
    println(f"${ metrics.size } ${ tagKeys.size } ${ tagValues.size }")
    for (keyValue <- asKeyValueIterator(scanner)) {
      val row = CellUtil.cloneRow(keyValue)
      //      println(Bytes.toStringBinary(row))
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
          val tagKey = tagKeys(new BytesKey(tagKeyId))
          val tagValue = tagValues(new BytesKey(tagValueId))
          (tagKey, tagValue)
        }
      val qualifier = CellUtil.cloneQualifier(keyValue)
      val value = CellUtil.cloneValue(keyValue)
      val (delta, numVal) = parseValue(qualifier, value)
      val metric = metrics(new BytesKey(metricId))
      val timestamp = baseTime + delta
      val rowString = f"$metric $timestamp $tags $numVal"
      println(rowString)
    }
  }

  def rowToHumanReadableString(row: Array[Byte], idMapping: IdMapping) = {
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
    f"$metric $baseTime $tags"
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

  def loadAllNames(configuration: Configuration): IdMapping = {
    val nameFilePaths = Seq(
      "43f2205c299947298c2c728768b93f10",
      "670a18ca73944880a73fd3103db2a61c",
      "e6d13fa04cf94ce1802b5ba6f4baddce"
    ).map(fileName => new Path(f"/home/quasi/programming/otsdb-analytics/name/$fileName"))
    val maps = nameFilePaths.map(path => loadNames(path, configuration))
    val names = maps.reduce(_ ++ _)
    val groupedNames = names
      .groupBy { case (QId(kind, id), name) => kind }
      .mapValues(_.map { case (QId(kind, id), name) => (id, name) }.toMap)
    IdMapping(
      metrics = groupedNames("metrics"),
      tagKeys = groupedNames("tagk"),
      tagValues = groupedNames("tagv")
    )
  }

  def loadNames(nameFilePath: Path, configuration: Configuration): Map[QId, String] = {
    val reader = getHFileReader(nameFilePath, configuration)
    val pairs = mutable.ArrayBuffer[(QId, String)]()
    val scanner = reader.getScanner(false, false)
    scanner.seekTo()
    var done = false
    while(!done) {
      val keyValue = scanner.getKeyValue
      val qId = QId(Bytes.toString(CellUtil.cloneQualifier(keyValue)), new BytesKey(CellUtil.cloneRow(keyValue)))
      val name = Bytes.toString(CellUtil.cloneValue(keyValue))
      pairs += ((qId, name
        ))
      if (!scanner.next()) {
        done = true
      }
    }
    pairs.toMap
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
