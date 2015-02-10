package popeye.storage

import popeye.storage.TranslationConstants._
import popeye.storage.hbase.TimeseriesId

object ResultsTranslation {
  def getMetricName(timeseriesId: TimeseriesId, idMap: Map[QualifiedId, String]) = {
    idMap(QualifiedId(MetricKind, timeseriesId.generationId, timeseriesId.metricId))
  }

  def getAttributes(timeseriesId: TimeseriesId, idMap: Map[QualifiedId, String]) = {
    timeseriesId.attributeIds.map {
      case (nameId, valueId) =>
        val name = idMap(QualifiedId(AttrNameKind, timeseriesId.generationId, nameId))
        val value = idMap(QualifiedId(AttrValueKind, timeseriesId.generationId, valueId))
        (name, value)
    }
  }

  def getUniqueIds(timeseriesId: TimeseriesId): Iterable[QualifiedId] = {
    import timeseriesId._
    val metricQId = QualifiedId(MetricKind, generationId, metricId)
    val shardQId = QualifiedId(ShardKind, generationId, shardId)
    val attrNameQIds = attributeIds.keys.map(id => QualifiedId(AttrNameKind, generationId, id))
    val attrValueQIds = attributeIds.values.map(id => QualifiedId(AttrValueKind, generationId, id))
    Iterable(metricQId, shardQId).view ++ attrNameQIds ++ attrValueQIds
  }
}
