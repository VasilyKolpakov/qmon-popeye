package popeye.storage

import popeye.{ListPoint, PointRope, AsyncIterator}
import popeye.storage.hbase.BytesKey

import scala.collection.immutable.SortedMap

sealed trait ValueType

case object SingleValueType extends ValueType

case object ListValueType extends ValueType

case class TimeseriesId(generationId: BytesKey,
                        downsampling: Downsampling,
                        metricId: BytesKey,
                        valueType: ValueType,
                        shardId: BytesKey,
                        attributeIds: SortedMap[BytesKey, BytesKey])

sealed trait RawPointT {
  def timeseriesId: TimeseriesId

  def timestamp: Int
}

case class RawPoint(timeseriesId: TimeseriesId,
                    timestamp: Int,
                    value: Either[Long, Float]) extends RawPointT {
  require(timeseriesId.valueType == SingleValueType)
}

case class RawListPoint(timeseriesId: TimeseriesId,
                        timestamp: Int,
                        value: Either[Seq[Long], Seq[Float]]) extends RawPointT {
  require(timeseriesId.valueType == ListValueType)
}

case class PointsResult(timeseriesId: TimeseriesId, points: PointRope)

case class ListPointsResult(timeseriesId: TimeseriesId, lists: Seq[ListPoint])

trait RawTimeseriesStorage {

  def getPointTimeseries(rawQuery: RawQuery): AsyncIterator[Seq[PointsResult]]

}
