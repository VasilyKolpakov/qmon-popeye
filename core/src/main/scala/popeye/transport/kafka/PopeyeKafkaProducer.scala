package popeye.transport.kafka

import java.io.Closeable
import popeye.transport.proto.{PackedPoints, Message}

/**
 * @author Andrey Stepachev
 */
trait PopeyeKafkaProducer extends Closeable {
  def sendPoints(batchId: Long, points: Message.Point*)

  def sendPacked(batchId: Long, buffers: PackedPoints*)
}
