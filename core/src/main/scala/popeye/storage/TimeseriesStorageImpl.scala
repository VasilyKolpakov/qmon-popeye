package popeye.storage

import com.codahale.metrics.MetricRegistry
import popeye.{Instrumented, PointRope}
import popeye.storage.hbase.UniqueId

import scala.concurrent.{ExecutionContext, Future}

case class TimeseriesStorageImplMetrics(name: String,
                                        override val metricRegistry: MetricRegistry) extends Instrumented {
  val pointsDecodingTime = metrics.timer(s"$name.points.decoding.time")
}

class TimeseriesStorageImpl(rawTimeseriesStorage: RawTimeseriesStorage,
                            uniqueId: UniqueId,
                            queryTranslation: QueryTranslation,
                            metrics: TimeseriesStorageImplMetrics) extends TimeseriesStorage {

  override def getSeries(metric: String,
                         timeRange: (Int, Int),
                         attributes: Map[String, ValueNameFilterCondition],
                         downsampling: Downsampling,
                         cancellation: Future[Nothing])
                        (implicit eCtx: ExecutionContext): Future[PointsSeriesMap] = {

    val scanNames = queryTranslation.getQueryNames(metric, timeRange, attributes)
    val scanNameIdPairsFuture = Future.traverse(scanNames) {
      qName =>
        uniqueId.resolveIdByName(qName, create = false)
          .map(id => Some(qName, id))
          .recover { case e: NoSuchElementException => None}
    }
    val rawQueriesFuture = scanNameIdPairsFuture.map {
      scanNameIdPairs =>
        val scanNameToIdMap = scanNameIdPairs.collect { case Some(x) => x}.toMap
        queryTranslation.getRawQueries(
          metric,
          timeRange,
          attributes,
          scanNameToIdMap,
          SingleValueType,
          downsampling
        )
    }
    rawQueriesFuture.flatMap {
      rawQueries =>
        val seriesMapFutures = rawQueries.map {
          rawQuery =>
            val pointsResultsIter = rawTimeseriesStorage.getPointTimeseries(rawQuery)
            val sequencesMapsIter = pointsResultsIter.map(resultsToPointsSequences)
            PointsSeriesMap.collectSeries(sequencesMapsIter, cancellation)
        }
        Future.sequence(seriesMapFutures).map(_.foldLeft(PointsSeriesMap.empty)(PointsSeriesMap.concat))
    }
  }

  private def resultsToPointsSequences(pointsResults: Seq[PointsResult]): Future[PointsSeriesMap] = {
    val decodingTimer = metrics.pointsDecodingTime.timerContext()
    val ids = pointsResults.iterator.map(_.timeseriesId).toSet.flatMap {
      tsId => ResultsTranslation.getUniqueIds(tsId)
    }.toSet
    val idNamePairsFuture = Future.traverse(ids) {
      case qId =>
        uniqueId.resolveNameById(qId).map(name => (qId, name))
    }
    idNamePairsFuture.map {
      idNamePairs =>
        val idMap = idNamePairs.toMap
        val pointSequences: Map[PointAttributes, PointRope] = toPointSequencesMap(pointsResults, idMap)
        decodingTimer.stop()
        PointsSeriesMap(pointSequences)
    }
  }

  private def toPointSequencesMap(rows: Seq[PointsResult],
                                  idMap: Map[QualifiedId, String]): Map[PointAttributes, PointRope] = {
    rows.groupBy(row => ResultsTranslation.getAttributes(row.timeseriesId, idMap)).mapValues {
      groupedRows =>
        val pointsSeq = groupedRows.map(_.points)
        PointRope.concatAll(pointsSeq)
    }.view.force // mapValues returns lazy Map
  }
}
