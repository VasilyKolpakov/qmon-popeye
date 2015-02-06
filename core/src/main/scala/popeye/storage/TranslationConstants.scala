package popeye.storage

import scala.collection.mutable

object TranslationConstants {

  final val MetricKind: String = "metric"
  final val AttrNameKind: String = "tagk"
  final val AttrValueKind: String = "tagv"
  final val ShardKind: String = "shard"

  final val UniqueIdMapping = Map[String, Short](
    MetricKind -> 3.toShort,
    AttrNameKind -> 3.toShort,
    AttrValueKind -> 3.toShort,
    ShardKind -> 3.toShort
  )


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
