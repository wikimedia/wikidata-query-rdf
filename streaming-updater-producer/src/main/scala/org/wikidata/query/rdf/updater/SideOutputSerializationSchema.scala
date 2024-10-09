package org.wikidata.query.rdf.updater

import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema
import org.apache.kafka.clients.producer.ProducerRecord

import java.lang
import java.time.Instant
import java.util.function.Consumer

class SideOutputSerializationSchema[E](topic: String,
                                       stream: String,
                                       schema: String,
                                       sideOutputsDomain: String,
                                       emitterId: Option[String],
                                       eventPlatformFactory: EventPlatformFactory) extends KafkaRecordSerializationSchema[E] {

  private def serializeValue(element: E, recordTime: Instant): Array[Byte] = {
    lazy val jsonEncoders = new JsonEncoders(sideOutputsDomain, emitterId)
    val eventCreator: Consumer[ObjectNode] = element match {
      case e: InputEvent => jsonEncoders.lapsedActionEvent(e)
      case e: FailedOp => jsonEncoders.fetchFailureEvent(e)
      case e: InconsistentMutation => jsonEncoders.stateInconsistencyEvent(e)
      case _ => throw new IllegalArgumentException("Unknown input type [" + element.getClass + "]")
    }
    val generator = eventPlatformFactory.jsonEventGenerator
    val jsonEvent: ObjectNode = generator.generateEvent(stream, schema, eventCreator, recordTime)
    generator.serializeAsBytes(jsonEvent)
  }

  def serializeRecord(element: E): ProducerRecord[Array[Byte], Array[Byte]] = {
    val recordTime: Instant = eventPlatformFactory.clock.instant()
    val eventData = serializeValue(element, recordTime)
    new ProducerRecord[Array[Byte], Array[Byte]](topic, null, recordTime.toEpochMilli, null, eventData) // scalastyle:ignore null
  }

  override def serialize(element: E,
                         context: KafkaRecordSerializationSchema.KafkaSinkContext,
                         timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    serializeRecord(element)
  }
}
