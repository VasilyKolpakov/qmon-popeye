package popeye.transport.kafka

import akka.actor._
import kafka.producer._
import java.util.Properties
import com.typesafe.config.Config
import popeye.{IdGenerator, ConfigUtil, Instrumented}
import ConfigUtil._
import popeye.transport.proto.Storage.Ensemble
import akka.routing.FromConfig
import kafka.producer.KeyedMessage
import akka.actor.Status.Failure
import com.codahale.metrics.{Timer, MetricRegistry}
import kafka.client.ClientUtils
import scala.collection
import kafka.utils.VerifiableProperties
import scala.collection.mutable.ArrayBuffer
import popeye.transport.proto.Message.Point
import scala.collection.mutable
import scala.concurrent.duration._
import akka.actor.SupervisorStrategy.Restart
import scala.util.Success

case class KafkaPointProducerMetrics(override val metricRegistry: MetricRegistry) extends Instrumented {
  val writeTimer = metrics.timer("kafka.produce.time")
  val sendTimer = metrics.timer("kafka.send.time")
  val pointsMeter = metrics.meter("kafka.produce.points")
  val batchFailedMeter = metrics.meter("kafka.produce.batch.failed")
  val batchCompleteMeter = metrics.meter("kafka.produce.batch.complete")
}

private object KafkaPointProducerProtocol {

  case class WorkDone(batchId: Long)

  case class CorrelatedPoint(correlationId: Long, sender: ActorRef)(val points: Seq[Point])

  case class ProducePack(batchId: Long, started: Timer.Context, partitions: Int)
                        (val batch: Seq[CorrelatedPoint])

}

class KafkaPointSender(topic: String, producerConfig: ProducerConfig, metrics: KafkaPointProducerMetrics, batcher: KafkaPointProducer) extends Actor with ActorLogging {

  import KafkaPointProducerProtocol._

  val producer = new Producer[Nothing, Ensemble](producerConfig)


  override def preStart() {
    super.preStart()
    log.debug("Starting sender")
    batcher.addWorker(self)
  }

  override def postStop() {
    log.debug("Stopping sender")
    super.postStop()
    producer.close()
  }

  def receive = {
    case p@ProducePack(batchId, started, partitions) =>
      val sendctx = metrics.sendTimer.timerContext()
      val ensembles = new collection.mutable.HashMap[Int, Ensemble.Builder]
      val notifications = mutable.Map[ActorRef, ArrayBuffer[Long]]()
      for (correlatedPoint <- p.batch;
           point <- correlatedPoint.points) {
        metrics.pointsMeter.mark()
        val part = Math.abs(point.getMetric.hashCode()) % partitions
        val ensemble = ensembles.getOrElseUpdate(part,
          Ensemble.newBuilder()
            .setBatchId(batchId)
            .setPartition(part)
        )
        ensemble.addPoints(point)
        val senderPointsId = notifications.getOrElseUpdate(correlatedPoint.sender, new ArrayBuffer)
        senderPointsId += correlatedPoint.correlationId
        notifications.put(correlatedPoint.sender, senderPointsId)
      }
      try {
        producer.send(ensembles.map(e => new KeyedMessage(topic, e._2.build)).toArray: _*)
        metrics.batchCompleteMeter.mark
        notifications foreach {
          p =>
            p._1 ! ProduceDone(p._2, batchId)
        }
      } catch {
        case e: Exception => sender ! Failure(e)
          metrics.batchFailedMeter.mark
          notifications foreach {
            p =>
              p._1 ! ProduceFailed(p._2, e)
          }
          throw e
      } finally {
        val sended = sendctx.stop.nano
        val elapsed = started.stop().nano
        if (log.isDebugEnabled)
          log.debug("batch {} sent in {}ms at total {}ms", p.batchId, sended.toMillis, elapsed.toMillis)
        batcher.addWorker(self)
        sender ! WorkDone(batchId)
      }
  }
}

class KafkaPointProducer(config: Config,
                         producerConfig: ProducerConfig,
                         idGenerator: IdGenerator,
                         val metrics: KafkaPointProducerMetrics)
  extends Actor with ActorLogging {

  import KafkaPointProducerProtocol._

  val topic = config.getString("kafka.points.topic")
  val partitions = ClientUtils
    .fetchTopicMetadata(
    Set(topic),
    ClientUtils.parseBrokerList(producerConfig.brokerList), producerConfig, 1
  ).topicsMetadata
    .filter(_.topic == topic)
    .head.partitionsMetadata.size

  val batchWaitTimeout: FiniteDuration = toFiniteDuration(config.getMilliseconds("kafka.produce.batch-timeout"))
  val maxQueued = config.getInt("kafka.produce.max-queued")
  val batchSize = config.getInt("kafka.produce.batch-size")
  val senders = config.getInt("kafka.produce.senders")

  var flusher: Option[Cancellable] = None

  var workQueue = new AtomicList[ActorRef]()

  override val supervisorStrategy =
    OneForOneStrategy(loggingEnabled = true) {
      case _ ⇒ Restart
    }

  def addWorker(worker: ActorRef) {
    workQueue.add(worker)
  }

  override def preStart() {
    super.preStart()
    log.debug("Starting batcher")

    for (i <- 1 to senders) {
      context.actorOf(
        Props(new KafkaPointSender(topic, producerConfig, metrics, this)).withDeploy(Deploy.local),
        "points-sender-" + i)
    }

    import context.dispatcher
    flusher = Some(context.system.scheduler.schedule(batchWaitTimeout, batchWaitTimeout, self, FlushPoints))
  }

  override def postStop() {
    log.debug("Stopping batcher")
    flusher foreach {
      _.cancel()
    }
    super.postStop()
  }

  private var pendingSize: Long = 0
  private var pending = new ArrayBuffer[CorrelatedPoint]

  private def addPending(point: CorrelatedPoint) = {
    pending += point
    pendingSize += point.points.size
  }

  private def removePending(amount: Int): Seq[CorrelatedPoint] = {
    pending.splitAt(amount) match {
      case (batch, tail) =>
        pending = tail
        pendingSize -= batch.size
        batch
      case _ => Nil
    }
  }

  def receive: Actor.Receive = {
    case FlushPoints =>
      flushPoints(0)

    case WorkDone(_) =>
      flushPoints(batchSize)

    case p@ProducePending(corr) =>
      p.data match {
        case Nil =>
        case points =>
          addPending(CorrelatedPoint(p.correlationId, sender)(points))
      }
      flushPoints(batchSize)
      if (pendingSize > maxQueued)
        sender ! ProduceNeedThrottle
  }

  private def flushPoints(minSend: Long): Unit = {
    while (pendingSize > minSend && !workQueue.isEmpty) {
      if (log.isDebugEnabled) {
        log.debug("Flushing {} points with {} available workers",
          pendingSize, workQueue.size)
      }
      workQueue.headOption() match {
        case Some(worker: ActorRef) =>
          val pp = pendingSize
          val batch = removePending(batchSize)
          log.debug("Batch with {} points was created, pending changed {} -> {}",
            batch.size, pp, pendingSize)

          if (!batch.isEmpty)
            worker ! ProducePack(idGenerator.nextId(), metrics.writeTimer.timerContext(), partitions)(batch)
        case None =>
          return
      }
    }
  }

}

class EnsemblePartitioner(props: VerifiableProperties = null) extends Partitioner[Ensemble] {
  def partition(data: Ensemble, numPartitions: Int): Int = {
    Math.abs(data.getPartition) % numPartitions
  }
}

object KafkaPointProducer {

  def start(config: Config, idGenerator: IdGenerator)(implicit system: ActorSystem, metricRegistry: MetricRegistry): ActorRef = {
    system.actorOf(KafkaPointProducer.props(config, idGenerator)
      .withRouter(FromConfig())
      .withDispatcher("kafka.produce.dispatcher"), "kafka-producer")
  }

  def producerConfig(globalConfig: Config): ProducerConfig = {
    val config: Config = globalConfig.getConfig("kafka.producer")
    val producerProps: Properties = config
    producerProps.setProperty("serializer.class", classOf[EnsembleEncoder].getName)
    producerProps.setProperty("partitioner.class", classOf[EnsemblePartitioner].getName)
    producerProps.setProperty("producer.type", "sync")
    new ProducerConfig(producerProps)
  }

  def props(config: Config, idGenerator: IdGenerator)(implicit metricRegistry: MetricRegistry) = {
    val metrics = KafkaPointProducerMetrics(metricRegistry)
    Props(new KafkaPointProducer(
      config,
      producerConfig(config),
      idGenerator,
      metrics))
  }
}


