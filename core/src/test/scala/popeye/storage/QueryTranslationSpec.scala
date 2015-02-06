package popeye.storage

import org.apache.hadoop.hbase.util.Bytes
import org.scalatest.{Matchers, FlatSpec}
import popeye.storage.ValueNameFilterCondition.{AllValueNames, MultipleValueNames, SingleValueName}
import popeye.storage.hbase._
import popeye.storage.hbase.TsdbFormat._
import popeye.storage.TranslationConstants._
import popeye.test.PopeyeTestUtils._

class QueryTranslationSpec extends FlatSpec with Matchers {

  val defaultShardAttributeName = "name"

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

  def qualifiedName(kind: String, name: String) = QualifiedName(kind, defaultGenerationIdBytes, name)

  behavior of "TsdbFormat.getScanNames"

  it should "get qualified names" in {
    val generationIdMapping = createGenerationIdMapping((0, MAX_TIMESPAN, 0), (MAX_TIMESPAN, MAX_TIMESPAN * 2, 1))
    val queryTranslation = createQueryTranslation(generationIdMapping, Set("shard"))
    val attrs = Map(
      "shard" -> SingleValueName("shard_1"),
      "single" -> SingleValueName("name"),
      "mult" -> MultipleValueNames(Seq("mult1", "mult2")),
      "all" -> AllValueNames
    )
    val names = queryTranslation.getQueryNames("test", (0, MAX_TIMESPAN + 1), attrs)

    val expected = Seq(
      (MetricKind, "test"),
      (AttrNameKind, "shard"),
      (AttrNameKind, "single"),
      (AttrNameKind, "mult"),
      (AttrNameKind, "all"),
      (AttrValueKind, "shard_1"),
      (AttrValueKind, "name"),
      (AttrValueKind, "mult1"),
      (AttrValueKind, "mult2"),
      (ShardKind, shardAttributeToShardName("shard", "shard_1"))
    ).flatMap {
      case (kind, name) => Seq(
        QualifiedName(kind, bytesKey(0, 0), name),
        QualifiedName(kind, bytesKey(0, 1), name)
      )
    }.toSet
    if (names != expected) {
      names.diff(expected).foreach(println)
      println("=========")
      expected.diff(names).foreach(println)
    }
    names should equal(expected)
  }

  it should "create 2 raw queries over generations" in {
    val generationIdMapping = createGenerationIdMapping((0, MAX_TIMESPAN, 0), (MAX_TIMESPAN, MAX_TIMESPAN * 2, 1))
    val queryTranslation = createQueryTranslation(generationIdMapping)
    val idMap = sampleNamesToIdMapping.flatMap {
      case ((kind, name), id) => Seq(
        QualifiedName(kind, bytesKey(0, 0), name) -> id,
        QualifiedName(kind, bytesKey(0, 1), name) -> id
      )
    }.toMap
    val rawQueries = queryTranslation.getRawQueries(
      metric = "test",
      timeRange = (0, MAX_TIMESPAN + 1),
      attributeValueFilters = Map(defaultShardAttributeName -> SingleValueName("value")),
      idMap = idMap,
      TsdbFormat.ValueTypes.SingleValueTypeStructureId,
      NoDownsampling
    )
    rawQueries.size should equal(2)
    rawQueries(0).generationId should equal(new BytesKey(Array[Byte](0, 0)))
    rawQueries(1).generationId should equal(new BytesKey(Array[Byte](0, 1)))
  }

  it should "not create raw query if not enough ids resolved" in {
    val prefixMapping = createGenerationIdMapping((0, 3600, 0), (3600, 7200, 1))
    val queryTranslation = createQueryTranslation(prefixMapping)
    val idMap = sampleNamesToIdMapping.map {
      case ((kind, name), id) => QualifiedName(kind, bytesKey(0, 0), name) -> id
    }.toMap
    val rawQueries = queryTranslation.getRawQueries(
      metric = "test",
      timeRange = (0, 4000),
      attributeValueFilters = Map(defaultShardAttributeName -> SingleValueName("value")),
      idMap = idMap,
      TsdbFormat.ValueTypes.SingleValueTypeStructureId,
      NoDownsampling
    )
    rawQueries.size should equal(1)
    rawQueries(0).generationId should equal(new BytesKey(Array[Byte](0, 0)))
  }

  it should "create raw query over 2 shards" in {
    val queryTranslation = createQueryTranslation()
    val idMap = sampleIdMap.updated(
      QualifiedName(ShardKind, defaultGenerationIdBytes, shardAttributeToShardName("name", "anotherValue")),
      bytesKey(4, 0, 2)
    )
    val rawQueries = queryTranslation.getRawQueries(
      metric = "test",
      timeRange = (0, 4000),
      attributeValueFilters = Map(defaultShardAttributeName -> MultipleValueNames(Seq("value", "anotherValue"))),
      idMap = idMap,
      TsdbFormat.ValueTypes.SingleValueTypeStructureId,
      NoDownsampling
    )
    rawQueries.size should equal(2)
    rawQueries(0).shardId should equal(new BytesKey(Array[Byte](4, 0, 1)))
    rawQueries(1).shardId should equal(new BytesKey(Array[Byte](4, 0, 2)))
  }

  it should "be aware of value_type_structure_id byte flag" in {
    val queryTranslation = createQueryTranslation()
    val scans = queryTranslation.getRawQueries(
      metric = "test",
      timeRange = (0, 1),
      attributeValueFilters = Map(defaultShardAttributeName -> SingleValueName("value")),
      idMap = sampleIdMap,
      TsdbFormat.ValueTypes.ListValueTypeStructureId,
      NoDownsampling
    )
    scans.size should equal(1)
    scans(0).valueTypeStructureId should equal(ValueTypes.ListValueTypeStructureId)
  }

  it should "be aware of downsampling byte flag" in {
    val queryTranslation = createQueryTranslation()
    val downsampling = EnabledDownsampling(DownsamplingResolution.Minute5, AggregationType.Min)
    val rawQueries = queryTranslation.getRawQueries(
      metric = "test",
      timeRange = (0, 1),
      attributeValueFilters = Map(defaultShardAttributeName -> SingleValueName("value")),
      idMap = sampleIdMap,
      TsdbFormat.ValueTypes.ListValueTypeStructureId,
      downsampling
    )
    rawQueries.size should equal(1)
    rawQueries(0).downsampling should equal(downsampling)
  }


  def createQueryTranslation(generationIdMapping: GenerationIdMapping = new FixedGenerationId(defaultGenerationId),
                             shardAttributes: Set[String] = Set(defaultShardAttributeName))
  : QueryTranslation = {
    new QueryTranslation(generationIdMapping, shardAttributes)
  }
}
