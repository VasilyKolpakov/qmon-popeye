package popeye.storage

import org.apache.hadoop.hbase.util.Bytes
import popeye.Logging
import popeye.storage.ValueIdFilterCondition.{AllValueIds, MultipleValueIds, SingleValueId}
import popeye.storage.ValueNameFilterCondition.{AllValueNames, MultipleValueNames, SingleValueName}
import popeye.storage.hbase.{GenerationIdMapping, BytesKey}
import popeye.storage.TranslationConstants._

import scala.collection.mutable

class QueryTranslation(timeRangeIdMapping: GenerationIdMapping, shardAttributeNames: Set[String]) extends Logging {

  def getRawQueries(metric: String,
                    timeRange: (Int, Int),
                    attributeValueFilters: Map[String, ValueNameFilterCondition],
                    idMap: Map[QualifiedName, BytesKey],
                    valueType: ValueType,
                    downsampling: Downsampling): Seq[RawQuery] = {
    val (startTime, stopTime) = timeRange
    val ranges = getGenerationRanges(startTime, stopTime)
    val shardNames = getShardNames(attributeValueFilters)
    info(s"getScans metric: $metric, shard names: $shardNames, ranges: $ranges")
    ranges.map {
      range =>
        val generationId = range.id
        val genIdBytes = new BytesKey(Bytes.toBytes(generationId))
        val shardQNames = shardNames.map(name => QualifiedName(ShardKind, genIdBytes, name))
        val metricQName = QualifiedName(MetricKind, genIdBytes, metric)
        val metricIdOption = idMap.get(metricQName)
        info(f"resolving metric id: $metricQName -> $metricIdOption")
        val shardIdOptions = shardQNames.map(idMap.get)
        info(f"resolving shard ids: ${(shardQNames zip shardIdOptions).toMap}")
        val shardIds = shardIdOptions.collect { case Some(id) => id}
        for {
          metricId <- metricIdOption if shardIds.nonEmpty
          attrIdFilters <- covertAttrNamesToIds(genIdBytes, attributeValueFilters, idMap)
        } yield {
          shardIds.map {
            shardId =>
              RawQuery(
                genIdBytes,
                metricId,
                shardId,
                timeRange,
                attrIdFilters,
                valueType,
                downsampling
              )
          }
        }
    }.collect { case Some(scans) => scans}.flatten
  }

  def getQueryNames(metric: String,
                    timeRange: (Int, Int),
                    attributeValueFilters: Map[String, ValueNameFilterCondition]): Set[QualifiedName] = {
    val (startTime, stopTime) = timeRange
    val generationIds = getGenerationRanges(startTime, stopTime).map(_.id)
    val shardNames = getShardNames(attributeValueFilters)
    generationIds.flatMap {
      generationId =>
        val genIdBytes = new BytesKey(Bytes.toBytes(generationId))
        val metricName = QualifiedName(MetricKind, genIdBytes, metric)
        val shardQNames = shardNames.map(name => QualifiedName(ShardKind, genIdBytes, name))
        val attrNames = attributeValueFilters.keys.map(name => QualifiedName(AttrNameKind, genIdBytes, name)).toSeq
        val attrValues = attributeValueFilters.values.collect {
          case SingleValueName(name) => Seq(name)
          case MultipleValueNames(names) => names
        }.flatten.map(name => QualifiedName(AttrValueKind, genIdBytes, name)).toSeq
        metricName +: (shardQNames ++ attrNames ++ attrValues)
    }.toSet
  }

  private def covertAttrNamesToIds(generationId: BytesKey,
                                   attributes: Map[String, ValueNameFilterCondition],
                                   idMap: Map[QualifiedName, BytesKey]
                                    ): Option[Map[BytesKey, ValueIdFilterCondition]] = {
    val (attrIdOptions, attrIdFiltersOptions) = attributes.toSeq.map {
      case (attrName, valueFilter) =>
        val valueIdFilterOption = convertAttrValuesToIds(generationId, valueFilter, idMap)
        val qName = QualifiedName(AttrNameKind, generationId, attrName)
        val nameIdOption = idMap.get(qName)
        info(f"resolving $qName -> $nameIdOption")
        (nameIdOption, valueIdFilterOption)
    }.unzip
    if (attrIdOptions.exists(_.isEmpty) || attrIdFiltersOptions.exists(_.isEmpty)) {
      None
    } else {
      val ids = attrIdOptions.map(_.get)
      val filters = attrIdFiltersOptions.map(_.get)
      Some((ids zip filters).toMap)
    }
  }

  private def convertAttrValuesToIds(generationId: BytesKey,
                                     value: ValueNameFilterCondition,
                                     idMap: Map[QualifiedName, BytesKey]): Option[ValueIdFilterCondition] = {
    value match {
      case SingleValueName(name) =>
        val qName = QualifiedName(AttrValueKind, generationId, name)
        val maybeId = idMap.get(qName).map {
          id => SingleValueId(id)
        }
        info(f"resolving single value filter: $qName -> $maybeId")
        maybeId
      case MultipleValueNames(names) =>
        val qNames = names.map(name => QualifiedName(AttrValueKind, generationId, name))
        val idOptions = qNames.map(idMap.get)
        info(f"resolving multiple value filter: ${qNames.zip(idOptions).toMap}")
        if (idOptions.exists(_.isDefined)) {
          val ids = idOptions.collect { case Some(id) => id}
          Some(MultipleValueIds(ids))
        } else {
          None
        }
      case AllValueNames => Some(AllValueIds)
    }
  }

  private def getGenerationRanges(startTime: Int, stopTime: Int) = {
    timeRangeIdMapping.backwardIterator(stopTime)
      .takeWhile(_.stop > startTime)
      .toVector
      .reverse
  }

  private def getShardNames(allFilters: Map[String, ValueNameFilterCondition]): Seq[String] = {
    val shardAttrFilterNames = allFilters.keys.filter(name => shardAttributeNames.contains(name))
    require(
      shardAttrFilterNames.size == 1,
      f"scan filters must have exactly one shard attribute; shard attributes: $shardAttributeNames"
    )
    val shardAttrName = shardAttrFilterNames.head
    val shardAttrValues = allFilters(shardAttrName) match {
      case SingleValueName(name) => Seq(name)
      case MultipleValueNames(names) => names
      case AllValueNames => throw new IllegalArgumentException("'*' filter is not supported for shard attributes")
    }
    shardAttrValues.map {
      shardAttrValue => shardAttributeToShardName(shardAttrName, shardAttrValue)
    }
  }

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
