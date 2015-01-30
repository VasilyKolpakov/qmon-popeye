package popeye.storage.hbase

import java.util.concurrent.Executors

import akka.actor.{ActorSystem, Props}
import akka.dispatch.ExecutionContexts
import com.codahale.metrics.MetricRegistry
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.DaemonThreadFactory
import org.apache.hadoop.hbase.client.{HTableInterface, HTableInterfaceFactory, HTablePool}
import org.apache.hadoop.hbase.util.Bytes
import org.kiji.testing.fakehtable.FakeHTable
import popeye.bench.BenchUtils
import popeye.pipeline.MetricGenerator
import popeye.storage.PointsTranslation
import PointsTranslation.SuccessfulTranslation
import popeye.storage.hbase.TsdbFormat.NoDownsampling
import popeye.test.PopeyeTestUtils

import scala.concurrent.Await
import scala.concurrent.duration._

object PointToKeyValueConversionBench {


  def main(args: Array[String]): Unit = {
    implicit val timeout = 5 seconds
    val endTime = 1419865200 // 29/12/14
    val points = createPoints(endTime)
    val shardAttrNames = Set("dc")
    val tsdbFormatConfig = TsdbFormatConfig(Seq(StartTimeAndPeriod("01/10/14", 26)), shardAttrNames)
    val tsdbFormat = tsdbFormatConfig.tsdbFormat
    val actorSystem = ActorSystem()
    val uniqueId = createUniqueId(actorSystem)
    val generationIdMapping = tsdbFormatConfig.generationIdMapping
    val pointTranslation = new PointsTranslation(shardAttrNames)
    for (point <- points) {
      val SuccessfulTranslation(rawPoint) = pointTranslation.translateToRawPoint(
        point,
        qname => Some(Await.result(uniqueId.resolveIdByName(qname, create = true), Duration.Inf)),
        Bytes.toBytes(generationIdMapping.getGenerationId(point.getTimestamp.toInt, endTime)),
        NoDownsampling
      )
    }

    val benchResult = BenchUtils.bench(10, 2) {
      for (point <- points) {
        val SuccessfulTranslation(rawPoint) = pointTranslation.translateToRawPoint(
          point,
          uniqueId.findIdByName,
          Bytes.toBytes(generationIdMapping.getGenerationId(point.getTimestamp.toInt, endTime)),
          NoDownsampling
        )
        tsdbFormat.createPointKeyValue(rawPoint, endTime)
      }
    }
    actorSystem.shutdown()
    println(f"number of points: ${points.size}")
    println(f"median time: ${benchResult.medianTime}, min time:${benchResult.minTime}, max time:${benchResult.maxTime}")
  }

  def createPoints(endTime: Int) = {
    for {
      timestamp <- (endTime - 3600 * 24 * 7) to endTime by 60
      metric <- Seq("test", "disk_usage")
      tags <- MetricGenerator.generateTags(Seq(
        "host" -> Seq("yandex.net", "google.com", "aaa.net"),
        "disk" -> Seq("sda", "sdd", "sdb"),
        "dc" -> Seq("ugr", "sas", "iva")
      ))
    } yield {
      PopeyeTestUtils.createPoint(
        metric = metric + "",
        timestamp = timestamp,
        attributes = tags.map { case (name, value) => (name + "", value + "")},
        value = Left(timestamp)
      )
    }
  }

  def createUniqueId(actorSystem: ActorSystem) = {
    implicit val exct = actorSystem.dispatcher
    val uidTableName = "tsdb-uid"
    val uIdHTable = new FakeHTable(uidTableName, desc = null)
    val uIdHTablePool = new HTablePool(new Configuration(), 1, new HTableInterfaceFactory {
      def releaseHTableInterface(table: HTableInterface) {}

      def createHTableInterface(config: Configuration, tableNameBytes: Array[Byte]): HTableInterface = uIdHTable
    })
    val metricRegistry = new MetricRegistry()
    val metrics = new UniqueIdStorageMetrics("uid", metricRegistry)
    val uniqueIdStorage = new UniqueIdStorage(uidTableName, uIdHTablePool, metrics)
    val executor = Executors.newSingleThreadExecutor(new DaemonThreadFactory("unique id actor"))
    val executionContext = ExecutionContexts.fromExecutor(executor)
    val uniqueIdActorProps = Props.apply(UniqueIdActor(uniqueIdStorage, executionContext))
    new UniqueIdImpl(actorSystem.actorOf(uniqueIdActorProps), new UniqueIdMetrics("uniqueid", metricRegistry))
  }
}
