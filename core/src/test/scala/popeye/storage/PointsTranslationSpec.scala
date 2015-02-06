package popeye.storage

import org.apache.hadoop.hbase.util.Bytes
import org.scalatest.{Matchers, FlatSpec}
import popeye.proto.Message
import popeye.storage.hbase._
import popeye.storage.hbase.TsdbFormat._
import popeye.test.PopeyeTestUtils._
import scala.collection.JavaConverters._
import scala.collection.immutable.SortedMap

class PointsTranslationSpec extends FlatSpec with Matchers {

  val shardAttrName = "name"

  val defaultGenerationId: Short = 10
  val defaultGenerationIdBytes = new BytesKey(Bytes.toBytes(defaultGenerationId))

  val sampleNamesToIdMapping = Seq(
    (MetricKind, "test") -> bytesKey(1, 0, 1),

    (AttrNameKind, "name") -> bytesKey(2, 0, 1),
    (AttrNameKind, "anotherName") -> bytesKey(2, 0, 2),

    (AttrValueKind, "value") -> bytesKey(3, 0, 1),
    (AttrValueKind, "anotherValue") -> bytesKey(3, 0, 2),

    (ShardKind, shardAttributeToShardName("name", "value")) -> bytesKey(4, 0, 1)
  )

  val sampleIdMap = sampleNamesToIdMapping.map {
    case ((kind, name), id) => qualifiedName(kind, name) -> id
  }.toMap

  val samplePoint = {
    val attrNameValues = Seq("name" -> "value", "anotherName" -> "anotherValue")
    val attributes = attrNameValues.map {
      case (name, valueName) => Message.Attribute.newBuilder().setName(name).setValue(valueName).build()
    }.asJava
    Message.Point.newBuilder()
      .setMetric("test")
      .setTimestamp(3610)
      .setIntValue(31)
      .addAllAttributes(attributes)
      .setValueType(Message.Point.ValueType.INT)
      .build()
  }

  val sampleTimeseriesId = {
    val metricId = Array[Byte](1, 0, 1)
    val shardId = Array[Byte](4, 0, 1)
    val attrs = SortedMap(
      bytesKey(2, 0, 1) -> bytesKey(3, 0, 1),
      bytesKey(2, 0, 2) -> bytesKey(3, 0, 2)
    )
    val valueTypeId = TsdbFormat.ValueTypes.SingleValueTypeStructureId
    val downsampling = NoDownsampling
    TimeseriesId(
      defaultGenerationIdBytes,
      downsampling,
      metricId,
      valueTypeId,
      shardId,
      attrs
    )
  }

  behavior of "PointsTranslation.translateToRawPoint"

  it should "convert point" in {
    val pointsTranslation = createPointsTranslation()
    val result = pointsTranslation.translateToRawPoint(samplePoint, sampleIdMap.get, defaultGenerationIdBytes, NoDownsampling)
    val rawPoint = RawPoint(sampleTimeseriesId, samplePoint.getTimestamp.toInt, Left(samplePoint.getIntValue))
    result should be(PointsTranslation.SuccessfulTranslation(rawPoint))
  }

  it should "not convert point if not all names are in cache" in {
    val pointsTranslation = createPointsTranslation()
    val notInCache = sampleIdMap.keys.head
    val idCache = (name: QualifiedName) => (sampleIdMap - notInCache).get(name)
    val result = pointsTranslation.translateToRawPoint(samplePoint, idCache, defaultGenerationIdBytes, NoDownsampling)
    result should be(PointsTranslation.IdCacheMiss)
  }

  behavior of "PointsTranslation.getAllQualifiedNames"

  it should "retrieve qualified names" in {
    val pointsTranslation = createPointsTranslation()
    val allQualifiedNames = sampleIdMap.keys.toSet
    val names: Seq[QualifiedName] = pointsTranslation.getAllQualifiedNames(samplePoint, 0)
    names.toSet should equal(allQualifiedNames)
  }

  def createPointsTranslation() = {
    new PointsTranslation(new FixedGenerationId(defaultGenerationId), Set(shardAttrName))
  }

  def qualifiedName(kind: String, name: String) = QualifiedName(kind, defaultGenerationIdBytes, name)
}
