package popeye.pipeline

import akka.actor.ActorSystem
import com.codahale.metrics.MetricRegistry
import com.typesafe.config.Config
import popeye.pipeline.kafka.{KafkaSinkFactory, KafkaPipelineChannel}
import popeye.storage.hbase.HBasePipelineSinkFactory
import popeye.{ConfigUtil, IdGenerator, MainConfig, PopeyeCommand}
import scala.concurrent.{Future, ExecutionContext}
import scopt.OptionParser
import popeye.storage.BlackHole
import popeye.pipeline.server.telnet.TelnetPointsServer
import popeye.pipeline.memory.MemoryPipelineChannel
import popeye.proto.PackedPoints

object PipelineCommand {

  val sources: Map[String, PipelineSourceFactory] = Map(
    "telnet" -> TelnetPointsServer.sourceFactory())


  def sinkFactories(ectx: ExecutionContext,
                    actorSystem: ActorSystem,
                    storagesConfig: Config,
                    metrics: MetricRegistry,
                    idGenerator: IdGenerator): Map[String, PipelineSinkFactory] = {
    val sinks = Map(
      "hbase-sink" -> new HBasePipelineSinkFactory(storagesConfig, actorSystem, ectx, metrics),
      "kafka-sink" -> new KafkaSinkFactory(actorSystem, ectx, idGenerator, metrics),
      "blackhole" -> BlackHole.sinkFactory(),
      "fail" -> new PipelineSinkFactory {
        def startSink(sinkName: String, config: Config): PointsSink = new PointsSink {
          def send(batchIds: Seq[Long], points: PackedPoints): Future[Long] =
            Future.failed(new RuntimeException("fail sink"))
        }
      }
    )
    sinks.withDefault(_ => throw new IllegalArgumentException("No such sink type"))
  }

  def sourceForType(typeName: String): PipelineSourceFactory = {
    sources.getOrElse(typeName, throw new IllegalArgumentException("No such source type"))
  }
}

class PipelineCommand extends PopeyeCommand {
  def prepare(parser: OptionParser[MainConfig]): OptionParser[MainConfig] = {
    parser cmd "pipeline" action { (_, c) => c.copy(command = Some(this))}
    parser
  }

  def run(actorSystem: ActorSystem, metrics: MetricRegistry, config: Config, mainConfig: MainConfig): Unit = {
    val ectx = ExecutionContext.global
    val pc = config.getConfig("popeye.pipeline")
    val channelConfig = pc.getConfig("channel")
    val storageConfig = config.getConfig("popeye.storages")
    val idGenerator = new IdGenerator(config.getInt("generator.id"), config.getInt("generator.datacenter"))
    val sinks = PipelineCommand.sinkFactories(ectx, actorSystem, storageConfig, metrics, idGenerator)
    val channel = pc.getString("channel.type") match {
      case "kafka" =>
        new KafkaPipelineChannel(
          channelConfig.getConfig("kafka"),
          actorSystem, ectx, metrics, idGenerator)
      case "memory" =>
        new MemoryPipelineChannel(
          channelConfig.getConfig("memory"),
          actorSystem, metrics, idGenerator)
      case x =>
        throw new NoSuchElementException(s"Requested channel type not supported")

    }
    val readersConfig = ConfigUtil.asMap(pc.getConfig("channelReaders"))

    for ((readerName, readerConfig) <- readersConfig) {
      val mainSinkConfig = readerConfig.getConfig("mainSink")
      val mainSink = sinks(mainSinkConfig.getString("type")).startSink(f"$readerName-mainSink", mainSinkConfig)

      val dropSinkConfig = readerConfig.getConfig("dropSink")
      val dropSink = sinks(dropSinkConfig.getString("type")).startSink(f"$readerName-dropSink", dropSinkConfig)

      channel.startReader("popeye-" + readerName, mainSink, dropSink)
    }
    for ((sourceName, sourceConfig) <- ConfigUtil.asMap(pc.getConfig("sources"))) {
      val typeName = sourceConfig.getString("type")
      PipelineCommand.sourceForType(typeName).startSource(sourceName, channel, sourceConfig, ectx)
    }
  }

}
