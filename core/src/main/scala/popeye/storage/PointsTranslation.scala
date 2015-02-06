package popeye.storage

import org.apache.hadoop.hbase.util.Bytes
import popeye.proto.Message
import popeye.proto.Message.{Attribute, Point}
import popeye.proto.Message.Point.ValueType._
import popeye.storage.PointsTranslation.TranslationResult
import popeye.storage.hbase.TsdbFormat._
import popeye.storage.hbase._

import scala.collection.JavaConverters._
import scala.collection.immutable.SortedMap
import scala.collection.mutable

object PointsTranslation {

  trait TranslationResult

  case class SuccessfulTranslation(rawPoint: RawPointT) extends TranslationResult

  case object IdCacheMiss extends TranslationResult

  def shardAttributeToShardName(attrName: String, attrValue: String): String = {
    mutable.StringBuilder.newBuilder
      .append('{')
      .append(attrName)
      .append(": ")
      .append(attrValue)
      .append('}')
      .append('_')
      .append(attrName.length)
      .append('_')
      .append(attrValue.length)
      .toString()
  }

}

class PointsTranslation(timeRangeIdMapping: GenerationIdMapping, shardAttributeNames: Set[String]) {

  def translateToRawPoint(point: Message.Point,
                          idCache: QualifiedName => Option[BytesKey],
                          generationId: BytesKey,
                          downsampling: Downsampling = NoDownsampling): TranslationResult = {
    val timeseriesIdOption: Option[TimeseriesId] = getTimeseriesId(point, idCache, generationId, downsampling)
    timeseriesIdOption.map {
      timeseriesId =>
        val rawPoint = point.getValueType match {
          case INT =>
            RawPoint(timeseriesId, point.getTimestamp.toInt, Left(point.getIntValue))
          case FLOAT =>
            RawPoint(timeseriesId, point.getTimestamp.toInt, Right(point.getFloatValue))
          case INT_LIST =>
            val ints = point.getIntListValueList.asScala.map(_.longValue())
            RawListPoint(timeseriesId, point.getTimestamp.toInt, Left(ints))
          case FLOAT_LIST =>
            val floats = point.getFloatListValueList.asScala.map(_.floatValue())
            RawListPoint(timeseriesId, point.getTimestamp.toInt, Right(floats))
        }
        PointsTranslation.SuccessfulTranslation(rawPoint)
    }.getOrElse {
      PointsTranslation.IdCacheMiss
    }
  }

  def getAllQualifiedNames(point: Message.Point, currentTimeInSeconds: Int): Seq[QualifiedName] = {
    val timeRangeId = getGenerationId(point, currentTimeInSeconds)
    val attributes: mutable.Buffer[Attribute] = point.getAttributesList.asScala
    val buffer = attributes.flatMap {
      attr => Seq(
        QualifiedName(AttrNameKind, timeRangeId, attr.getName),
        QualifiedName(AttrValueKind, timeRangeId, attr.getValue)
      )
    }
    buffer += QualifiedName(MetricKind, timeRangeId, point.getMetric)
    buffer += QualifiedName(ShardKind, timeRangeId, getShardName(point))
    buffer
  }

  private def getGenerationId(point: Message.Point, currentTimeSeconds: Int): BytesKey = {
    val id = timeRangeIdMapping.getGenerationId(point.getTimestamp.toInt, currentTimeSeconds)
    new BytesKey(Bytes.toBytes(id))
  }

  private def getTimeseriesId(point: Point,
                              idCache: (QualifiedName) => Option[BytesKey],
                              generationId: BytesKey,
                              downsampling: Downsampling): Option[TimeseriesId] = {
    val metricId = idCache(QualifiedName(MetricKind, generationId, point.getMetric))
    val attributes = point.getAttributesList.asScala

    val shardName = getShardName(point)
    val shardId = idCache(QualifiedName(ShardKind, generationId, shardName))

    val attributeIds = attributes.map {
      attr =>
        val name = idCache(QualifiedName(AttrNameKind, generationId, attr.getName))
        val value = idCache(QualifiedName(AttrValueKind, generationId, attr.getValue))
        (name, value)
    }
    def attributesAreDefined = attributeIds.forall { case (n, v) => n.isDefined && v.isDefined}
    val valueTypeId = getValueTypeId(point)
    if (metricId.isDefined && shardId.isDefined && attributesAreDefined) {
      val tags = attributeIds.map { case (n, v) => (n.get, v.get)}
      val timeseriesId = TimeseriesId(
        generationId,
        downsampling,
        metricId.get,
        valueTypeId,
        shardId.get,
        SortedMap(tags: _*)
      )
      Some(timeseriesId)
    } else {
      None
    }
  }

  private def getValueTypeId(point: Point): Byte = {
    val valueTypeId = point.getValueType match {
      case INT | FLOAT => TsdbFormat.ValueTypes.SingleValueTypeStructureId
      case INT_LIST | FLOAT_LIST => TsdbFormat.ValueTypes.ListValueTypeStructureId
    }
    valueTypeId
  }

  private def getShardName(point: Message.Point): String = {
    val attributes = point.getAttributesList.asScala
    val shardAttributes = attributes.filter(attr => shardAttributeNames.contains(attr.getName))
    require(
      shardAttributes.size == 1,
      f"a point must have exactly one shard attribute; shard attributes: $shardAttributeNames"
    )

    val shardAttribute = shardAttributes.head
    shardAttributeToShardName(shardAttribute.getName, shardAttribute.getValue)
  }

}

