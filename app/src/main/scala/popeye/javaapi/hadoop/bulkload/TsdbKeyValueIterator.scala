package popeye.javaapi.hadoop.bulkload

import com.codahale.metrics.MetricRegistry
import popeye.Logging
import popeye.proto.Message
import popeye.storage.{PointsTranslation, QualifiedName}
import popeye.storage.NoDownsampling
import popeye.storage.hbase._
import org.apache.hadoop.hbase.KeyValue
import scala.collection.JavaConverters._
import org.apache.hadoop.hbase.client.HTablePool
import popeye.hadoop.bulkload.{LightweightUniqueId, KafkaPointsIterator}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object TsdbKeyValueIterator {
  def create(pointsIterator: KafkaPointsIterator,
             tsdbFormat: TsdbFormat,
             pointsTranslation: PointsTranslation,
             generationIdMapping: GenerationIdMapping,
             uniqueIdTableName: String,
             tablePool: HTablePool,
             maxCacheSize: Int,
             maxDelayedPoints: Int) = {
    val metrics = new UniqueIdStorageMetrics("uniqueid.storage", new MetricRegistry)
    val idStorage = new UniqueIdStorage(uniqueIdTableName, tablePool, metrics)
    val uniqueId = new LightweightUniqueId(idStorage, maxCacheSize)
    new TsdbKeyValueIterator(
      pointsIterator,
      uniqueId,
      tsdbFormat,
      pointsTranslation,
      generationIdMapping,
      maxDelayedPoints
    )
  }
}

class TsdbKeyValueIterator(pointsIterator: KafkaPointsIterator,
                           uniqueId: LightweightUniqueId,
                           tsdbFormat: TsdbFormat,
                           pointsTranslation: PointsTranslation,
                           generationIdMapping: GenerationIdMapping,
                           maxDelayedPoints: Int) extends java.util.Iterator[java.util.List[KeyValue]] with Logging {

  val delayedPointsBuffer = mutable.Buffer[Message.Point]()

  def hasNext = pointsIterator.hasNext || delayedPointsBuffer.nonEmpty

  def next(): java.util.List[KeyValue] = {
    val currentTimeInSeconds = (System.currentTimeMillis() / 1000).toInt
    val nextKeyValues =
      if (pointsIterator.hasNext) {
        val points = pointsIterator.next()
        val (keyValues, delayedPoints) = convertToKeyValues(points, currentTimeInSeconds)
        delayedPointsBuffer ++= delayedPoints
        if (delayedPointsBuffer.size > maxDelayedPoints) {
          val delayedKeyValues = convertDelayedPoints(delayedPointsBuffer, currentTimeInSeconds)
          delayedPointsBuffer.clear()
          delayedKeyValues ++ keyValues
        } else {
          keyValues
        }
      } else {
        val keyValues = convertDelayedPoints(delayedPointsBuffer, currentTimeInSeconds)
        delayedPointsBuffer.clear()
        keyValues
      }
    nextKeyValues.asJava
  }

  def convertDelayedPoints(delayedPoints: Seq[Message.Point], currentTimeInSeconds: Int) = {
    val allQNames: Set[QualifiedName] = delayedPoints.flatMap {
      point => pointsTranslation.getAllQualifiedNames(point, currentTimeInSeconds)
    }(scala.collection.breakOut)
    val loadedIds = uniqueId.findOrRegisterIdsByNames(allQNames)
    val keyValues = ArrayBuffer[KeyValue]()
    delayedPoints.map {
      point =>
        try {
          val generationId = generationIdMapping.getGenerationIdBytes(point.getTimestamp.toInt, currentTimeInSeconds)
          pointsTranslation.translateToRawPoint(point, loadedIds.get, generationId, NoDownsampling) match {
            case PointsTranslation.SuccessfulTranslation(rawPoint) =>
              val keyValue = tsdbFormat.createPointKeyValue(rawPoint, currentTimeInSeconds * 1000)
              keyValues += keyValue
            case PointsTranslation.IdCacheMiss => error("some unique id were not resolved")
          }
        } catch {
          case ex: Exception => error(f"cannot convert delayed point: $point", ex)
        }
    }
    keyValues
  }

  def convertToKeyValues(points: Seq[Message.Point], currentTimeInSeconds: Int) = {
    val keyValues = ArrayBuffer[KeyValue]()
    val delayedPoints = ArrayBuffer[Message.Point]()
    points.foreach {
      point =>
        try {
          val generationId = generationIdMapping.getGenerationIdBytes(point.getTimestamp.toInt, currentTimeInSeconds)
          pointsTranslation.translateToRawPoint(point, uniqueId.findByName, generationId, NoDownsampling) match {
            case PointsTranslation.SuccessfulTranslation(rawPoint) =>
              val keyValue = tsdbFormat.createPointKeyValue(rawPoint, currentTimeInSeconds * 1000)
              keyValues += keyValue
            case PointsTranslation.IdCacheMiss => delayedPoints += point
          }
        } catch {
          case ex: Exception => error(f"cannot convert point: $point", ex)
        }
    }
    (keyValues, delayedPoints)
  }

  def getProgress = pointsIterator.getProgress

  override def remove(): Unit = ???
}
