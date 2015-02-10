package popeye.storage.hbase

import popeye.paking.RowPacker.QualifierAndValue
import popeye.paking.{RowPacker, ValueTypeDescriptor}
import popeye.proto.Message
import popeye.storage.{RawQuery, QualifiedId, ValueIdFilterCondition}
import popeye.storage.hbase.TsdbFormat._
import DownsamplingResolution.DownsamplingResolution
import AggregationType.AggregationType
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.util.Bytes
import popeye.{ListPoint, PointRope, Point, Logging}
import scala.collection.JavaConverters._
import org.apache.hadoop.hbase.client.{Scan, Result}
import java.util.Arrays.copyOfRange
import scala.collection.immutable.SortedMap
import java.nio.ByteBuffer
import java.nio.charset.Charset
import org.apache.hadoop.hbase.filter.{CompareFilter, RowFilter, RegexStringComparator}
import popeye.storage.TranslationConstants._

object TsdbFormat {

  final val Encoding = Charset.forName("UTF-8")

  final val PointsFamily = "t".getBytes(Encoding)
  
  /** Number of LSBs in time_deltas reserved for flags.  */
  final val FLAG_BITS: Short = 4
  /**
   * When this bit is set, the value is a floating point value.
   * Otherwise it's an integer value.
   */
  final val FLAG_FLOAT: Short = 0x8
  /** Mask to select the size of a value from the qualifier.  */
  final val LENGTH_MASK: Short = 0x7
  /** Mask to select all the FLAG_BITS.  */
  final val FLAGS_MASK: Short = (FLAG_FLOAT | LENGTH_MASK).toShort
  /** Max time delta (in seconds) we can store in a column qualifier.  */
  val MAX_TIMESPAN: Int = TsdbFormat.DownsamplingResolution.maxTimespan

  trait Downsampling {
    def rowTimespanInSeconds: Int

    def resolutionInSeconds: Int
  }

  case object NoDownsampling extends Downsampling {
    override def rowTimespanInSeconds: Int = DownsamplingResolution.secondsInHour

    override def resolutionInSeconds: Int = 1
  }

  case class EnabledDownsampling(downsamplingResolution: DownsamplingResolution,
                                 aggregationType: AggregationType) extends Downsampling {
    override def rowTimespanInSeconds: Int = DownsamplingResolution.timespanInSeconds(downsamplingResolution)

    override def resolutionInSeconds: Int = DownsamplingResolution.resolutionInSeconds(downsamplingResolution)
  }

  object AggregationType extends Enumeration {
    type AggregationType = Value
    val Max, Min, Sum, Avg = Value

    def getId(aggregationType: AggregationType) = aggregationType match {
      case Max => 1
      case Min => 2
      case Sum => 3
      case Avg => 4
    }

    def getById(id: Int) = id match {
      case 1 => Max
      case 2 => Min
      case 3 => Sum
      case 4 => Avg
    }
  }

  object DownsamplingResolution extends Enumeration {

    val secondsInHour = 3600
    val secondsInDay = secondsInHour * 24
    type DownsamplingResolution = Value
    val Minute5, Hour, Day = Value

    val resolutions:SortedMap[Int, DownsamplingResolution] = {
      val pairs = values.toList.map {
        resolution => (resolutionInSeconds(resolution), resolution)
      }
      SortedMap(pairs: _*)
    }

    val maxTimespan = values.map(timespanInSeconds).max

    val noDownsamplingResolutionId = 0

    def getId(resolution: DownsamplingResolution) = resolution match {
      case Minute5 => 1
      case Hour => 2
      case Day => 3
    }

    def getById(id: Int) = id match {
      case 1 => Minute5
      case 2 => Hour
      case 3 => Day
    }

    def resolutionInSeconds(resolution: DownsamplingResolution) = resolution match {
      case Minute5 => 300
      case Hour => secondsInHour
      case Day => secondsInDay
    }

    def timespanInSeconds(resolution: DownsamplingResolution) = resolution match {
      case Minute5 => secondsInHour * 4
      case Hour => secondsInDay * 2
      case Day => secondsInDay * 14
    }
  }

  def renderDownsamplingByte(downsampling: Downsampling): Byte = {
    downsampling match {
      case NoDownsampling =>
        0.toByte
      case EnabledDownsampling(resolution, aggregationType) =>
        val resId = DownsamplingResolution.getId(resolution)
        val aggrId = AggregationType.getId(aggregationType)
        renderDownsamplingByteFromIds(resId, aggrId)
    }
  }

  def parseDownsamplingByte(dsByte: Byte): Downsampling = {
    if (dsByte == 0) {
      NoDownsampling
    } else {
      val downsamplingId = parseDownsamplingResolution(dsByte)
      val aggregationId = dsByte & 0x0f
      EnabledDownsampling(DownsamplingResolution.getById(downsamplingId), AggregationType.getById(aggregationId))
    }
  }

  def renderDownsamplingByteFromIds(resId: Int, aggrId: Int): Byte = {
    (resId << 4 | aggrId).toByte
  }

  def parseDownsamplingResolution(dsByte: Byte): Int = {
    (dsByte & 0xf0) >> 4
  }

  def getTimespanByDownsamplingId(downsamplingId: Int): Int = {
    if (downsamplingId == 0) {
      NoDownsampling.rowTimespanInSeconds
    } else {
      DownsamplingResolution.timespanInSeconds(DownsamplingResolution.getById(downsamplingId))
    }
  }

  val metricWidth: Int = UniqueIdMapping(MetricKind)

  val attributeNameWidth: Int = UniqueIdMapping(AttrNameKind)
  val attributeValueWidth: Int = UniqueIdMapping(AttrValueKind)
  val attributeWidth = attributeNameWidth + attributeValueWidth
  val baseTimeWidth: Int = 4
  val shardIdWidth: Int = UniqueIdMapping(ShardKind)
  val valueTypeIdWidth: Int = 1
  val downsamplingQualByteWidth: Int = 1
  val uniqueIdGenerationWidth = 2

  val Seq(
  uniqueIdGenerationOffset,
  downsamplingQualByteOffset,
  metricOffset,
  valueTypeIdOffset,
  shardIdOffset,
  baseTimeOffset,
  attributesOffset
  ) = {
    val widths = Seq(
      uniqueIdGenerationWidth,
      downsamplingQualByteWidth,
      metricWidth,
      valueTypeIdWidth,
      shardIdWidth,
      baseTimeWidth
    )
    val offsets = widths.scanLeft(0)((previousOffset, nextWidth) => previousOffset + nextWidth)
    offsets
  }

  val ROW_REGEX_FILTER_ENCODING = Charset.forName("ISO-8859-1")

  val rowPacker = new RowPacker(qualifierLength = 2, valueTypeDescriptor = ValueTypes.TsdbValueTypeDescriptor)

  trait ValueType {
    def mkQualifiedValue(point: Message.Point, downsampling: Downsampling): (Array[Byte], Array[Byte])

    def getValueTypeStructureId: Byte
  }

  object ValueTypes {

    object TsdbValueTypeDescriptor extends ValueTypeDescriptor {
      override def getValueLength(qualifierArray: Array[Byte],
                                  qualifierOffset: Int,
                                  qualifierLength: Int): Int = {
        val qualifier = Bytes.toShort(qualifierArray, qualifierOffset, qualifierLength)
        val floatFlag: Int = TsdbFormat.FLAG_FLOAT | 0x3.toShort
        val isFloatValue = (qualifier & floatFlag) == floatFlag
        if (isFloatValue) {
          4
        } else {
          8
        }
      }
    }

    val SingleValueTypeStructureId: Byte = 0
    val ListValueTypeStructureId: Byte = 1

    def renderQualifier(timestamp: Int, downsampling: Downsampling, isFloat: Boolean): Array[Byte] = {
      val delta: Short = ((timestamp % downsampling.rowTimespanInSeconds) / downsampling.resolutionInSeconds).toShort
      val ndelta = delta << FLAG_BITS
      if (isFloat) {
        Bytes.toBytes(((0xffff & (FLAG_FLOAT | 0x3)) | ndelta).toShort)
      } else {
        Bytes.toBytes((0xffff & ndelta).toShort)
      }
    }

    def parseQualifier(qualifierBytes: Array[Byte]) = parseQualifierFromSlice(qualifierBytes, 0, qualifierBytes.length)

    def parseQualifierFromSlice(qualifierArray: Array[Byte], qualifierOffset: Int, qualifierLength: Int) = {
      require(
        qualifierLength == Bytes.SIZEOF_SHORT,
        s"Expected qualifier length was ${Bytes.SIZEOF_SHORT}, got $qualifierLength"
      )
      val qualifier = Bytes.toShort(qualifierArray, qualifierOffset, qualifierLength)
      val deltaShort = ((qualifier & 0xFFFF) >>> FLAG_BITS).toShort
      val floatFlag: Int = FLAG_FLOAT | 0x3.toShort
      val isFloatValue = (qualifier & floatFlag) == floatFlag
      val isIntValue = (qualifier & 0xf) == 0
      if (!isFloatValue && !isIntValue) {
        throw new IllegalArgumentException("Neither int nor float values set on point")
      }
      (deltaShort, isFloatValue)
    }

    def parseSingleValue(valueBytes: Array[Byte], isFloat: Boolean) =
      parseSingleValueFromSlice(valueBytes, 0, valueBytes.length, isFloat)

    def parseSingleValueFromSlice(valueArray: Array[Byte],
                                  valueOffset: Int,
                                  valueLength: Int,
                                  isFloatValue: Boolean): Either[Long, Float] = {
      if (isFloatValue) {
        require(
          valueLength == Bytes.SIZEOF_FLOAT,
          s"Expected value length was ${Bytes.SIZEOF_FLOAT}, got $valueLength"
        )
        Right(Bytes.toFloat(valueArray, valueOffset))
      } else {
        require(
          valueLength == Bytes.SIZEOF_LONG,
          s"Expected value length was ${Bytes.SIZEOF_LONG}, got $valueLength"
        )
        Left(Bytes.toLong(valueArray, valueOffset))
      }
    }

    def parseListValue(valueBytes: Array[Byte], isFloatValue: Boolean): Either[Seq[Long], Seq[Float]] = {
      if (isFloatValue) {
        Right(FloatListValueType.parseFloatListValue(valueBytes))
      } else {
        Left(IntListValueType.parseIntListValue(valueBytes))
      }
    }

    def getType(valueType: Message.Point.ValueType): ValueType = {
      import Message.Point.ValueType._
      valueType match {
        case INT => IntValueType
        case FLOAT => FloatValueType
        case INT_LIST => IntListValueType
        case FLOAT_LIST => FloatListValueType
      }
    }
  }


  case object IntValueType extends ValueType {
    override def mkQualifiedValue(point: Message.Point, downsampling: Downsampling): (Array[Byte], Array[Byte]) = {
      makeQualifierAndValue(point.getTimestamp.toInt, point.getIntValue, downsampling)
    }

    def makeQualifierAndValue(timestamp: Int, value: Long, downsampling: Downsampling) = {
      val qualifier = ValueTypes.renderQualifier(timestamp, downsampling, isFloat = false)
      val valueBytes = Bytes.toBytes(value)
      (qualifier, valueBytes)
    }

    override def getValueTypeStructureId: Byte = ValueTypes.SingleValueTypeStructureId
  }

  case object FloatValueType extends ValueType {
    override def mkQualifiedValue(point: Message.Point, downsampling: Downsampling): (Array[Byte], Array[Byte]) = {
      makeQualifierAndValue(point.getTimestamp.toInt, point.getFloatValue, downsampling)
    }

    def makeQualifierAndValue(timestamp: Int, value: Float, downsampling: Downsampling) = {
      val qualifier = ValueTypes.renderQualifier(timestamp, downsampling, isFloat = true)
      val valueBytes = Bytes.toBytes(value)
      (qualifier, valueBytes)
    }

    override def getValueTypeStructureId: Byte = ValueTypes.SingleValueTypeStructureId
  }

  case object IntListValueType extends ValueType {
    override def mkQualifiedValue(point: Message.Point, downsampling: Downsampling): (Array[Byte], Array[Byte]) = {
      val longs = point.getIntListValueList.asScala.map(_.longValue())
      makeQualifierAndValue(point.getTimestamp.toInt, longs, downsampling)
    }

    def makeQualifierAndValue(timestamp: Int, value: Seq[Long], downsampling: Downsampling) = {
      val qualifier = ValueTypes.renderQualifier(timestamp, downsampling, isFloat = false)
      val valueBytes = longsToBytes(value)
      (qualifier, valueBytes)
    }

    override def getValueTypeStructureId: Byte = ValueTypes.ListValueTypeStructureId

    def parseIntListValue(value: Array[Byte]): Array[Long] = {
      val longBuffer = ByteBuffer.wrap(value).asLongBuffer()
      val array = Array.ofDim[Long](longBuffer.remaining())
      longBuffer.get(array)
      array
    }

    private def longsToBytes(longs: Seq[Long]) = {
      val buffer = ByteBuffer.allocate(longs.size * 8)
      for (l <- longs) {
        buffer.putLong(l)
      }
      buffer.array()
    }
  }

  case object FloatListValueType extends ValueType {
    override def mkQualifiedValue(point: Message.Point, downsampling: Downsampling): (Array[Byte], Array[Byte]) = {
      val floats = point.getFloatListValueList.asScala.map(_.floatValue())
      makeQualifierAndValue(point.getTimestamp.toInt, floats, downsampling)
    }

    def makeQualifierAndValue(timestamp: Int, value: Seq[Float], downsampling: Downsampling) = {
      val qualifier = ValueTypes.renderQualifier(timestamp, downsampling, isFloat = true)
      val valueBytes = floatsToBytes(value)
      (qualifier, valueBytes)
    }

    override def getValueTypeStructureId: Byte = ValueTypes.ListValueTypeStructureId

    def parseFloatListValue(value: Array[Byte]): Array[Float] = {
      val floatBuffer = ByteBuffer.wrap(value).asFloatBuffer()
      val array = Array.ofDim[Float](floatBuffer.remaining())
      floatBuffer.get(array)
      array
    }

    private def floatsToBytes(floats: Seq[Float]) = {
      val buffer = ByteBuffer.allocate(floats.size * 4)
      for (f <- floats) {
        buffer.putFloat(f.floatValue())
      }
      buffer.array()
    }
  }

  def createRowRegexp(offset: Int,
                      attrNameLength: Int,
                      attrValueLength: Int,
                      attributes: Map[BytesKey, ValueIdFilterCondition]): String = {
    require(attrNameLength > 0, f"attribute name length must be greater than 0, not $attrNameLength")
    require(attrValueLength > 0, f"attribute value length must be greater than 0, not $attrValueLength")
    require(attributes.nonEmpty, "attribute map is empty")
    val sortedAttributes = attributes.toList.sortBy(_._1)
    def checkAttrNameLength(name: Array[Byte]) =
      require(name.length == attrNameLength,
        f"invalid attribute name length: expected $attrNameLength, actual ${ name.length }")

    val anyNumberOfAnyAttributesRegex = f"(?:.{${ attrNameLength + attrValueLength }})*"
    val prefix = f"(?s)^.{$offset}" + anyNumberOfAnyAttributesRegex
    val suffix = anyNumberOfAnyAttributesRegex + "$"
    val infix = sortedAttributes.map {
      case (attrNameId, valueCondition) =>
        checkAttrNameLength(attrNameId)
        renderAttributeRegexp(attrNameId, valueCondition, attrValueLength)
    }.mkString(anyNumberOfAnyAttributesRegex)
    prefix + infix + suffix
  }

  private def renderAttributeRegexp(attrNameId: BytesKey, valueCondition: ValueIdFilterCondition, attrValueLength: Int) = {
    import ValueIdFilterCondition._
    def checkAttrValueLength(value: Array[Byte]) = require(value.length == attrValueLength,
      f"invalid attribute value length: expected $attrValueLength, actual ${ value.length }")
    valueCondition match {
      case SingleValueId(attrValue) =>
        checkAttrValueLength(attrValue)
        escapeRegexp(decodeBytes(attrNameId) + decodeBytes(attrValue))
      case MultipleValueIds(attrValues) =>
        val nameRegex = escapeRegexp(decodeBytes(attrNameId))
        val attrsRegexps = attrValues.map {
          value =>
            checkAttrValueLength(value)
            escapeRegexp(decodeBytes(value))
        }
        nameRegex + attrsRegexps.mkString("(?:", "|", ")")
      case AllValueIds =>
        val nameRegex = escapeRegexp(decodeBytes(attrNameId))
        nameRegex + f".{$attrValueLength}"
    }
  }

  private def escapeRegexp(string: String) = f"\\Q${ string.replace("\\E", "\\E\\\\E\\Q") }\\E"

  private def decodeBytes(bytes: Array[Byte]) = {
    val byteBuffer = ByteBuffer.wrap(bytes)
    ROW_REGEX_FILTER_ENCODING.decode(byteBuffer).toString
  }

  def parseSingleValueRowResult(result: Result): ParsedSingleValueRowResult = {
    val row = result.getRow
    require(row(valueTypeIdOffset) == ValueTypes.SingleValueTypeStructureId)
    val (timeseriesId, baseTime) = parseTimeseriesIdAndBaseTime(row)
    val downsampling = parseDownsamplingByte(row(downsamplingQualByteOffset))
    val cells = result.rawCells()
    val qualifierAndValues = rowPacker.unpackRow(cells)
    val points = qualifierAndValues.iterator.map {
      case QualifierAndValue(
      qualifierArray, qualifierOffset, qualifierLength,
      valueArray, valueOffset, valueLength,
      timestamp) =>
        val (delta, isFloat) = ValueTypes.parseQualifierFromSlice(qualifierArray, qualifierOffset, qualifierLength)
        val value = ValueTypes.parseSingleValueFromSlice(valueArray, valueOffset, valueLength, isFloat)
        Point(baseTime + delta * downsampling.resolutionInSeconds, value.fold(_.toDouble, _.toDouble))
    }
    ParsedSingleValueRowResult(timeseriesId, PointRope.fromIterator(points))
  }

  def parseListValueRowResult(result: Result): ParsedListValueRowResult = {
    val row = result.getRow
    val (timeseriesId, baseTime) = parseTimeseriesIdAndBaseTime(row)
    val columns = result.getFamilyMap(PointsFamily).asScala.toList
    val listPoints = columns.map {
      case (qualifierBytes, valueBytes) =>
        val (delta, isFloat) = ValueTypes.parseQualifier(qualifierBytes)
        val value = ValueTypes.parseListValue(valueBytes, isFloat)
        ListPoint(baseTime + delta, value)
    }
    ParsedListValueRowResult(timeseriesId, listPoints)
  }

  def parseTimeseriesIdAndBaseTime(row: Array[Byte]): (TimeseriesId, Int) = {
    val attributesLength = row.length - attributesOffset
    require(
      attributesLength >= 0 && attributesLength % attributeWidth == 0,
      f"illegal row length: ${row.length}, attributes length: $attributesLength, attr size: ${attributeWidth}"
    )
    val generationId = new BytesKey(copyOfRange(row, 0, uniqueIdGenerationWidth))
    val downsamplingByte = row(downsamplingQualByteOffset)
    val metricId = new BytesKey(copyOfRange(row, metricOffset, metricOffset + metricWidth))
    val valueTypeId = row(valueTypeIdOffset)
    val shardId = new BytesKey(copyOfRange(row, shardIdOffset, shardIdOffset + attributeValueWidth))
    val baseTime = Bytes.toInt(row, baseTimeOffset, baseTimeWidth)
    val attributesBytes = copyOfRange(row, attributesOffset, row.length)
    val timeseriedId = TimeseriesId(
      generationId,
      parseDownsamplingByte(downsamplingByte),
      metricId,
      valueTypeId,
      shardId,
      createAttributesMap(attributesBytes)
    )
    (timeseriedId, baseTime)
  }

  private def createAttributesMap(attributes: Array[Byte]): SortedMap[BytesKey, BytesKey] = {
    val attributeWidth = attributeNameWidth + attributeValueWidth
    require(attributes.length % attributeWidth == 0, "bad attributes length")
    val numberOfAttributes = attributes.length / attributeWidth
    val attrNamesIndexes = (0 until numberOfAttributes).map(i => i * attributeWidth)
    val attributePairs =
      for (attrNameIndex <- attrNamesIndexes)
      yield {
        val attrValueIndex = attrNameIndex + attributeNameWidth
        val nameArray = new Array[Byte](attributeNameWidth)
        val valueArray = new Array[Byte](attributeValueWidth)
        System.arraycopy(attributes, attrNameIndex, nameArray, 0, attributeNameWidth)
        System.arraycopy(attributes, attrValueIndex, valueArray, 0, attributeValueWidth)
        (new BytesKey(nameArray), new BytesKey(valueArray))
      }
    SortedMap[BytesKey, BytesKey](attributePairs: _*)
  }
}

case class TimeseriesId(generationId: BytesKey,
                        downsampling: Downsampling,
                        metricId: BytesKey,
                        valueTypeId: Byte,
                        shardId: BytesKey,
                        attributeIds: SortedMap[BytesKey, BytesKey])

sealed trait RawPointT {
  def timeseriesId: TimeseriesId

  def timestamp: Int
}

case class RawPoint(timeseriesId: TimeseriesId,
                    timestamp: Int,
                    value: Either[Long, Float]) extends RawPointT {
  require(timeseriesId.valueTypeId == ValueTypes.SingleValueTypeStructureId)
}

case class RawListPoint(timeseriesId: TimeseriesId,
                        timestamp: Int,
                        value: Either[Seq[Long], Seq[Float]]) extends RawPointT {
  require(timeseriesId.valueTypeId == ValueTypes.ListValueTypeStructureId)
}

case class ParsedSingleValueRowResult(timeseriesId: TimeseriesId, points: PointRope)

case class ParsedListValueRowResult(timeseriesId: TimeseriesId, lists: Seq[ListPoint])

class TsdbFormat(timeRangeIdMapping: GenerationIdMapping, shardAttributeNames: Set[String]) extends Logging {

  import TsdbFormat._

  def createPointKeyValue(rawPoint: RawPointT, keyValueTimestamp: Long) = {
    val qualifiedValue = rawPoint match {
      case RawPoint(tsId, timestamp, value) =>
        value.fold(
          longValue => {
            IntValueType.makeQualifierAndValue(timestamp, longValue, tsId.downsampling)
          },
          floatValue => {
            FloatValueType.makeQualifierAndValue(timestamp, floatValue, tsId.downsampling)
          }
        )

      case RawListPoint(tsId, timestamp, value) =>
        value.fold(
          longsValue => {
            IntListValueType.makeQualifierAndValue(timestamp, longsValue, tsId.downsampling)
          },
          floatsValue => {
            FloatListValueType.makeQualifierAndValue(timestamp, floatsValue, tsId.downsampling)
          }
        )
    }
    val timeseriesId = rawPoint.timeseriesId
    mkKeyValue(
      timeseriesId.generationId,
      timeseriesId.downsampling,
      timeseriesId.metricId,
      timeseriesId.valueTypeId,
      timeseriesId.shardId,
      rawPoint.timestamp,
      timeseriesId.attributeIds.toSeq,
      keyValueTimestamp,
      qualifiedValue
    )
  }

  private def getRowFilerOption(attributePredicates: Map[BytesKey, ValueIdFilterCondition]): Option[RowFilter] = {
    if (attributePredicates.nonEmpty) {
      val rowRegex = createRowRegexp(
        attributesOffset,
        attributeNameWidth,
        attributeValueWidth,
        attributePredicates
      )
      val comparator = new RegexStringComparator(rowRegex)
      comparator.setCharset(ROW_REGEX_FILTER_ENCODING)
      Some(new RowFilter(CompareFilter.CompareOp.EQUAL, comparator))
    } else {
      None
    }
  }

  def renderScan(rawQuery: RawQuery) = {
    import rawQuery._
    val (startTime, endTime) = timeRange
    val baseStartTime = startTime - (startTime % downsampling.rowTimespanInSeconds)
    val downsamplingByte = renderDownsamplingByte(downsampling)
    val rowPrefix = (generationId.bytes :+ downsamplingByte) ++ metricId.bytes :+ valueTypeStructureId
    val startTimeBytes = Bytes.toBytes(baseStartTime)
    val stopTimeBytes = Bytes.toBytes(endTime)
    val startRow = rowPrefix ++ shardId.bytes ++ startTimeBytes
    val stopRow = rowPrefix ++ shardId.bytes ++ stopTimeBytes
    val scan = new Scan()
    scan.setStartRow(startRow)
    scan.setStopRow(stopRow)
    scan.addFamily(PointsFamily)
    getRowFilerOption(attributePredicates).foreach {
      filter => scan.setFilter(filter)
    }
    scan
  }

  private def mkKeyValue(generationId: BytesKey,
                         downsampling: Downsampling,
                         metric: BytesKey,
                         valueTypeStructureId: Byte,
                         shardId: BytesKey,
                         timestamp: Long,
                         attributeIds: Seq[(BytesKey, BytesKey)],
                         keyValueTimestamp: Long,
                         value: (Array[Byte], Array[Byte])) = {
    val downsamplingByte = renderDownsamplingByte(downsampling)
    val rowTimespan = downsampling.rowTimespanInSeconds
    val baseTime: Int = (timestamp - (timestamp % rowTimespan)).toInt
    val rowLength = attributesOffset + attributeIds.length * attributeWidth
    val row = new Array[Byte](rowLength)
    var off = 0
    off = copyBytes(generationId, row, off)
    row(off) = downsamplingByte
    off += 1
    off = copyBytes(metric, row, off)
    row(off) = valueTypeStructureId
    off += 1
    off = copyBytes(shardId, row, off)
    off = copyBytes(Bytes.toBytes(baseTime.toInt), row, off)
    val sortedAttributes = attributeIds.sortBy(_._1)
    for (attr <- sortedAttributes) {
      off = copyBytes(attr._1, row, off)
      off = copyBytes(attr._2, row, off)
    }
    new KeyValue(row, PointsFamily, value._1, keyValueTimestamp, value._2)
  }

  @inline
  private def copyBytes(src: Array[Byte], dst: Array[Byte], off: Int): Int = {
    System.arraycopy(src, 0, dst, off, src.length)
    off + src.length
  }

}
