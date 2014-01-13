package popeye.pipeline.kafka

import popeye.pipeline.{PipelineSinkFactory, PointsSink}
import com.typesafe.config.Config
import akka.routing.FromConfig
import akka.actor.{Props, ActorSystem}
import popeye.IdGenerator
import com.codahale.metrics.MetricRegistry
import popeye.proto.PackedPoints
import scala.concurrent.{ExecutionContext, Future}

class KafkaSinkFactory(actorSystem: ActorSystem,
                       ectx: ExecutionContext,
                       idGenerator: IdGenerator,
                       metrics: MetricRegistry)
  extends PipelineSinkFactory {
  def startSink(sinkName: String, config: Config): PointsSink = {
    val kafkaConfig: Config = config.getConfig("kafka")
    val producerConfig = KafkaPointsProducer.producerConfig(kafkaConfig)
    val kafkaClient = new PopeyeKafkaProducerFactoryImpl(producerConfig)
    val props = KafkaPointsProducer.props("kafka", kafkaConfig, idGenerator, kafkaClient, metrics)
    val producerActor = actorSystem.actorOf(props, f"$sinkName-kafka-producer")
    new KafkaPointsSink(producerActor)(ectx)
  }
}
