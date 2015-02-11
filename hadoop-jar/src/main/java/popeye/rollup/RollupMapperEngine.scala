package popeye.rollup

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import popeye.rollup.RollupMapperEngine.RollupStrategy
import popeye.storage._
import popeye.storage.hbase.{TsdbFormat, TsdbFormatConfig}
import popeye.{Logging, PointRope}

import scala.collection.JavaConverters._

object RollupMapperOutput {
  def fromKeyValue(keyValue: KeyValue) = {
    val row = new ImmutableBytesWritable(keyValue.getRowArray, keyValue.getRowOffset, keyValue.getRowLength)
    RollupMapperOutput(row, keyValue)
  }
}

case class RollupMapperOutput(row: ImmutableBytesWritable, keyValue: KeyValue)

case class DownsamplingPoint(timestamp: Int, value: Float, downsampling: EnabledDownsampling)

object RollupMapperEngine {

  import DownsamplingResolution._

  object RollupStrategy {
    def renderString(rollupStrategy: RollupStrategy): String = rollupStrategy match {
      case HourRollup => "hour"
      case DayRollup => "day"
    }

    def parseString(str: String): RollupStrategy = str match {
      case "hour" => HourRollup
      case "day" => DayRollup
      case _ => throw new IllegalArgumentException(f"bad rollup strategy key: $str, only 'hour' and 'day' are supported")
    }

    case object HourRollup extends RollupStrategy {
      override def resolutions: Seq[DownsamplingResolution] = Seq(Minute5, Hour)
    }

    case object DayRollup extends RollupStrategy {
      override def resolutions: Seq[DownsamplingResolution] = Seq(Day)
    }

  }

  sealed trait RollupStrategy {
    def rollup(points: PointRope): Iterable[DownsamplingPoint] = {
      resolutions.flatMap(resolution => rollupPoints(points, resolution))
    }

    def resolutions: Seq[DownsamplingResolution]

    def maxResolutionInSeconds = {
      resolutions.map(DownsamplingResolution.resolutionInSeconds).max
    }
  }

  val aggregators: Map[AggregationType.AggregationType, Iterable[Double] => Double] = {
    import AggregationType._
    Map(
      Sum -> (seq => seq.sum),
      Min -> (seq => seq.min),
      Max -> (seq => seq.max),
      Avg -> (seq => seq.sum / seq.size)
    )
  }

  val commonKeyPrefix = "popeye.rollup.RollupMapperEngine"

  val strategyKey = f"$commonKeyPrefix.strategy"
  val tsdbFormatConfigKey = f"$commonKeyPrefix.tsdbformat.config"
  val keyValueTimestampKey = f"$commonKeyPrefix.keyvalue.timestamp"

  def createFromConfiguration(conf: Configuration): RollupMapperEngine = {
    val strategyString = conf.get(strategyKey)
    val rollupStrategy = RollupStrategy.parseString(strategyString)
    val tsdbFormat = TsdbFormatConfig.parseString(conf.get(tsdbFormatConfigKey)).tsdbFormat
    val keyValueTimestamp = conf.get(keyValueTimestampKey).toLong
    new RollupMapperEngine(tsdbFormat, rollupStrategy, keyValueTimestamp)
  }

  def setConfiguration(conf: Configuration,
                       strategy: RollupStrategy,
                       tsdbFormatConfig: TsdbFormatConfig,
                       keyValueTimestamp: Long) = {
    conf.set(strategyKey, RollupStrategy.renderString(strategy))
    conf.set(tsdbFormatConfigKey, TsdbFormatConfig.renderString(tsdbFormatConfig))
    conf.set(keyValueTimestampKey, keyValueTimestamp.toString)
  }

  def rollupPoints(points: PointRope, resolution: DownsamplingResolution): Iterable[DownsamplingPoint] = {
    def baseTime(timestamp: Int): Int = timestamp - timestamp % resolutionInSeconds(resolution)
    val groupedByBaseTime = points.asIterable.groupBy(point => baseTime(point.timestamp))
    for {
      (baseTimestamp, pointsToAggregate) <- groupedByBaseTime
      aggregation <- AggregationType.values
    } yield {
      val downsampling = EnabledDownsampling(resolution, aggregation)
      val pointsView = pointsToAggregate.view
      val downsampledValue = RollupMapperEngine.aggregators(aggregation)(pointsView.map(_.value))
      DownsamplingPoint(baseTimestamp, downsampledValue.toFloat, downsampling)
    }
  }
}

class RollupMapperEngine(tsdbFormat: TsdbFormat,
                         rollupStrategy: RollupStrategy,
                         keyValueTimestamp: Long) extends Logging {
  def map(value: Result): java.lang.Iterable[RollupMapperOutput] = {
    val PointsResult(timeseriesId, points) = TsdbFormat.parseSingleValueRowResult(value)
    val mapperOutputs = for (dsPoint <- rollupStrategy.rollup(points)) yield {
      val DownsamplingPoint(timestamp, value, downsampling) = dsPoint
      val dsTimeseriesId = timeseriesId.copy(downsampling = downsampling)
      val rawPoint = RawPoint(dsTimeseriesId, timestamp, Right(value))
      val keyValue = tsdbFormat.createPointKeyValue(rawPoint, keyValueTimestamp)
      RollupMapperOutput.fromKeyValue(keyValue)
    }
    mapperOutputs.asJava
  }

  def cleanup(): java.lang.Iterable[RollupMapperOutput] = {
    Iterable.empty[RollupMapperOutput].asJava
  }

}