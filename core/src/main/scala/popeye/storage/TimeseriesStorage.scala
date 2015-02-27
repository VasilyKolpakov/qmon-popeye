package popeye.storage

import popeye.{AsyncIterator, PointRope}

import scala.concurrent.{Promise, Future, ExecutionContext}


object PointsSeriesMap {
  def empty = PointsSeriesMap(Map.empty)

  def concat(left: PointsSeriesMap, right: PointsSeriesMap) = {
    val concatinatedSeries = right.seriesMap.foldLeft(left.seriesMap) {
      case (accGroup, (attrs, newPoints)) =>
        val pointsOption = accGroup.get(attrs)
        val pointArray = pointsOption.map(oldPoints => oldPoints.concat(newPoints)).getOrElse(newPoints)
        accGroup.updated(attrs, pointArray)
    }
    PointsSeriesMap(concatinatedSeries)
  }

  def collectSeries(groupsIterator: AsyncIterator[PointsSeriesMap], cancellation: Future[Nothing] = Promise().future)
                   (implicit eCtx: ExecutionContext): Future[PointsSeriesMap] = {
    AsyncIterator.foldLeft(
      groupsIterator,
      PointsSeriesMap.empty,
      PointsSeriesMap.concat,
      cancellation
    )
  }
}

case class PointsSeriesMap(seriesMap: Map[PointAttributes, PointRope])


trait TimeseriesStorage {

  def getSeries(metric: String,
                timeRange: (Int, Int),
                attributes: Map[String, ValueNameFilterCondition],
                downsampling: Downsampling,
                cancellation: Future[Nothing])
               (implicit eCtx: ExecutionContext): Future[PointsSeriesMap]
}
