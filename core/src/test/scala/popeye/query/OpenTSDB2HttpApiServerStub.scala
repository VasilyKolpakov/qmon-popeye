package popeye.query

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import popeye.query.PointsStorage.NameType
import popeye.query.PointsStorage.NameType.NameType
import popeye.query.PointsStorage.NameType.NameType
import popeye.storage.hbase.HBaseStorage
import popeye.storage.hbase.HBaseStorage.ValueNameFilterCondition.{MultipleValueNames, AllValueNames, SingleValueName}
import popeye.storage.hbase.HBaseStorage._
import popeye.util.FutureStream

import scala.collection.immutable.SortedMap
import scala.concurrent.Future

object OpenTSDB2HttpApiServerStub {
  def main(args: Array[String]) {
    val system = ActorSystem()
    val metrics = Map(
      "sin" -> math.sin _,
      "cos" -> math.cos _
    )
    val periods = Seq(100, 500, 1000)
    val amps = Seq(1000, 2000, 4000)
    val adds = Seq(1000, 2000, 4000)
    case class Timeseries(metric: String, tags: SortedMap[String, String], f: Double => Double)
    val allTs =
      for {
        (metric, f) <- metrics
        period <- periods
        amp <- amps
        add <- adds
      } yield {
        val tags = SortedMap(
          "period" -> period.toString,
          "amp" -> amp.toString,
          "add" -> add.toString
        )
        val func: Double => Double = t => f(t / period * math.Pi) * amp + add
        Timeseries(metric, tags, func)
      }
    val storage = new PointsStorage {
      override def getPoints(metric: String,
                             timeRange: (Int, Int),
                             attributes: Map[String, ValueNameFilterCondition]): Future[PointsStream] = {
        def conditionHolds(tagValue: String, condition: ValueNameFilterCondition) = condition match {
          case SingleValueName(name) => name == tagValue
          case MultipleValueNames(names) => names.contains(tagValue)
          case AllValueNames => true
        }
        val filteredTs = attributes.foldLeft(allTs.filter(_.metric == metric)) {
          case (nonFilteredTs, (tagKey, tagValueFilter)) =>
            nonFilteredTs
              .filter(_.tags.keySet.contains(tagKey))
              .filter(ts => conditionHolds(ts.tags(tagKey), tagValueFilter))
        }
        val (start, stop) = timeRange
        def toPoints(f: Double => Double) = (start to stop).by((stop - start) / 100).map(t => Point(t, Right(f(t).toFloat)))
        val pointSeries = filteredTs.map(ts => (ts.tags, toPoints(ts.f))).toMap
        val groupByTags = attributes.filter { case (tagKey, tagValueFilter) => tagValueFilter.isGroupByAttribute }.map(_._1)
        val groupedTs = pointSeries.groupBy {
          case (tags, _) =>
            val gTags = groupByTags.toList.map(tagK => (tagK, tags(tagK)))
            SortedMap(gTags: _*)
        }
        val groupsMap: Map[PointAttributes, PointsGroup] = groupedTs
        Future.successful(FutureStream.fromItems(PointsGroups(groupsMap)))
      }

      override def getSuggestions(namePrefix: String,
                                  nameType: NameType,
                                  maxSuggestions: Int): Seq[String] = {
        def filterSuggestions(names: Iterable[String]) = names.filter(_.startsWith(namePrefix)).toList.sorted.distinct
        nameType match {
          case NameType.MetricType => filterSuggestions(metrics.keys)
          case NameType.AttributeNameType => filterSuggestions(Seq("period", "amp", "add"))
          case NameType.AttributeValueType => filterSuggestions(Seq(periods, amps, adds).flatten.map(_.toString))
        }
      }

      override def getListPoints(metric: String,
                                 timeRange: (Int, Int),
                                 attributes: Map[String, ValueNameFilterCondition]): Future[ListPointsStream] = ???
    }
    val config = ConfigFactory.parseString(
      """
        |http = {
        |  backlog = 100
        |  listen = "localhost:8080"
        |}
      """.stripMargin)
    OpenTSDB2HttpApiServer.runServer(config, storage, system, system.dispatcher)
  }
}
