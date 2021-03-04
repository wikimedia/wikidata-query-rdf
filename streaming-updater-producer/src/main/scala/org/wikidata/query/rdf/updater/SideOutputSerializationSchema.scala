package org.wikidata.query.rdf.updater

import java.lang
import java.time.{Clock, Instant}
import java.util.function.{Consumer, Supplier}

import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema
import org.apache.kafka.clients.producer.ProducerRecord
import org.wikimedia.eventutilities.core.event.{EventStreamConfig, JsonEventGenerator}

class SideOutputSerializationSchema[E](recordTimeClock: Option[() => Instant], topic: String,
                                       stream: String, schema: String, sideOutputsDomain: String,
                                       eventStreamConfigEndpoint: String) extends KafkaSerializationSchema[E] {

  private def getRecordClock(): () => Instant = {
    recordTimeClock match {
      case Some(recTimeClock) => recTimeClock
      case None => {
        val systemClock: Clock = Clock.systemUTC()
        () => systemClock.instant()
      }
    }
  }

  lazy val clock: () => Instant = getRecordClock()

  private def getEventGenerator(): JsonEventGenerator = {
   JsonEventGenerator.builder()
      .eventStreamConfig(EventStreamConfig.builder()
        .setEventStreamConfigLoader(eventStreamConfigEndpoint)
        .build())
      .ingestionTimeClock(new Supplier[Instant] {
        override def get(): Instant = clock()
      }).build()
  }

  lazy val jsonEventGenerator: JsonEventGenerator = getEventGenerator()
  override def serialize(element: E, timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    lazy val jsonEncoders = new JsonEncoders(sideOutputsDomain)
    val recordTime: Instant = clock()
    val eventCreator: Consumer[ObjectNode] = element match {
      case e: InputEvent => jsonEncoders.lapsedActionEvent(e)
      case e: FailedOp => jsonEncoders.fetchFailureEvent(e)
      case e: IgnoredMutation => jsonEncoders.stateInconsistencyEvent(e)
      case _ => throw new IllegalArgumentException("Unknown input type [" + element.getClass + "]")
    }
    val jsonEvent: ObjectNode = jsonEventGenerator.generateEvent(stream, schema, eventCreator, recordTime)
    val eventData: Array[Byte] = jsonEventGenerator.serializeAsBytes(jsonEvent)
    new ProducerRecord[Array[Byte], Array[Byte]](topic, null, recordTime.toEpochMilli, null, eventData) // scalastyle:ignore null
  }
}
