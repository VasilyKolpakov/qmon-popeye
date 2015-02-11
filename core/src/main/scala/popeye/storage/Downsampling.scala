package popeye.storage

import popeye.storage.DownsamplingResolution.DownsamplingResolution
import popeye.storage.AggregationType.AggregationType

import scala.collection.immutable.SortedMap

trait Downsampling {
  def rowTimespanInSeconds: Int

  def resolutionInSeconds: Int
}

case object NoDownsampling extends Downsampling {
  override def rowTimespanInSeconds: Int = DownsamplingResolution.secondsInHour

  override def resolutionInSeconds: Int = 1
}

case class EnabledDownsampling(downsamplingResolution: DownsamplingResolution,
                               aggregationType: AggregationType) extends Downsampling {
  override def rowTimespanInSeconds: Int = DownsamplingResolution.timespanInSeconds(downsamplingResolution)

  override def resolutionInSeconds: Int = DownsamplingResolution.resolutionInSeconds(downsamplingResolution)
}

object AggregationType extends Enumeration {
  type AggregationType = Value
  val Max, Min, Sum, Avg = Value
}

object DownsamplingResolution extends Enumeration {

  val secondsInHour = 3600
  val secondsInDay = secondsInHour * 24
  type DownsamplingResolution = Value
  val Minute5, Hour, Day = Value

  val resolutions:SortedMap[Int, DownsamplingResolution] = {
    val pairs = values.toList.map {
      resolution => (resolutionInSeconds(resolution), resolution)
    }
    SortedMap(pairs: _*)
  }

  val maxTimespan = values.map(timespanInSeconds).max

  def resolutionInSeconds(resolution: DownsamplingResolution) = resolution match {
    case Minute5 => 300
    case Hour => secondsInHour
    case Day => secondsInDay
  }

  def timespanInSeconds(resolution: DownsamplingResolution) = resolution match {
    case Minute5 => secondsInHour * 4
    case Hour => secondsInDay * 2
    case Day => secondsInDay * 14
  }
}
