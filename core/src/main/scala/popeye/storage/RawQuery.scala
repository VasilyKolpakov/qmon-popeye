package popeye.storage

import popeye.storage.hbase.BytesKey

case class RawQuery(generationId: BytesKey,
                    metricId: BytesKey,
                    shardId: BytesKey,
                    timeRange: (Int, Int),
                    attributePredicates: Map[BytesKey, ValueIdFilterCondition],
                    valueType: ValueType,
                    downsampling: Downsampling)
