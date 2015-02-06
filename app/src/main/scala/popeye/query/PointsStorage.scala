package popeye.query

import com.codahale.metrics.MetricRegistry
import org.apache.hadoop.hbase.util.Bytes
import popeye.{Logging, Instrumented, PointRope}
import popeye.storage._
import popeye.storage.TranslationConstants._
import scala.concurrent.{ExecutionContext, Future}
import popeye.storage.hbase._
import popeye.query.PointsStorage.NameType.NameType
import scala.collection.immutable.SortedSet
import scala.collection.immutable.SortedMap

trait PointsStorage {
  def getPoints(metric: String,
                timeRange: (Int, Int),
                attributes: Map[String, ValueNameFilterCondition],
                downsampling: Option[(Int, TsdbFormat.AggregationType.AggregationType)],
                cancellation: Future[Nothing]): Future[PointsGroups]

  def getSuggestions(namePrefix: String, nameType: NameType, maxSuggestions: Int): Seq[String]
}

object PointsStorage {

  val MaxGenerations = 3

  object NameType extends Enumeration {
    type NameType = Value
    val MetricType, AttributeNameType, AttributeValueType = Value
  }

  class PointsStorageMetrics(name: String, override val metricRegistry: MetricRegistry) extends Instrumented {
    val seriesGroupingTime = metrics.timer(s"$name.series.grouping.time")
    val seriesRetrievalTime = metrics.timer(s"$name.series.retrieval.time")
  }

  def createPointsStorage(pointsStorage: TimeseriesStorage,
                          uniqueIdStorage: UniqueIdStorage,
                          timeRangeIdMapping: GenerationIdMapping,
                          executionContext: ExecutionContext,
                          metrics: PointsStorageMetrics) = new PointsStorage with Logging {

    val transparentDownsampling = new StorageWithTransparentDownsampling(pointsStorage)

    def getPoints(metric: String,
                  timeRange: (Int, Int),
                  attributes: Map[String, ValueNameFilterCondition],
                  downsampling: Option[(Int, TsdbFormat.AggregationType.AggregationType)],
                  cancellation: Future[Nothing]) = {
      implicit val exct = executionContext
      val retrievalTimer = metrics.seriesRetrievalTime.timerContext()
      val eventualPointsSeriesMap = transparentDownsampling.getSeries(
        metric,
        timeRange,
        attributes,
        downsampling,
        cancellation
      )
      eventualPointsSeriesMap.map {
        seriesMap =>
          retrievalTimer.stop()
          val groupingTimer = metrics.seriesGroupingTime.timerContext()
          val groupByAttributeNames =
            attributes
              .toList
              .filter { case (attrName, valueFilter) => valueFilter.isGroupByAttribute}
              .map(_._1)
          val groups = seriesMap.seriesMap.groupBy {
            case (pointAttributes, _) =>
              val groupByAttributeValueIds = groupByAttributeNames.map(pointAttributes(_))
              SortedMap[String, String](groupByAttributeNames zip groupByAttributeValueIds: _*)
          }.mapValues(series => PointsSeriesMap(series))
          groupingTimer.stop()
          PointsGroups(groups)
      }
    }

    def getSuggestions(namePrefix: String, nameType: NameType, maxSuggestions: Int): Seq[String] = {

      import NameType._
      val kind = nameType match {
        case MetricType => MetricKind
        case AttributeNameType => AttrNameKind
        case AttributeValueType => AttrValueKind
      }
      val currentTimeInSeconds = System.currentTimeMillis() / 1000
      val currentBaseTime = currentTimeInSeconds - currentTimeInSeconds % TsdbFormat.MAX_TIMESPAN
      val generationIds = timeRangeIdMapping.backwardIterator(currentBaseTime.toInt)
        .take(MaxGenerations)
        .map(_.id)
        .toSeq
      val suggestions = generationIds.flatMap {
        genId => uniqueIdStorage.getSuggestions(
          kind,
          new BytesKey(Bytes.toBytes(genId)),
          namePrefix,
          maxSuggestions
        )
      }
      SortedSet(suggestions: _*).toSeq
    }
  }
}


