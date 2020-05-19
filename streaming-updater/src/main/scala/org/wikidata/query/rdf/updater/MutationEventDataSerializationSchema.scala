package org.wikidata.query.rdf.updater

import java.lang

import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema
import org.apache.kafka.clients.producer.ProducerRecord
import org.wikidata.query.rdf.tool.MapperUtils

/**
 * Json serializer that sends all events to a specific partition
 */
class MutationEventDataSerializationSchema(topic: String, partition: Int) extends KafkaSerializationSchema[EntityPatchOp] {
  override def serialize(element: EntityPatchOp, timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    // we just produce the data (MutationEventData), rest is useful for monitoring and testing
    val messageBody = MapperUtils.getObjectMapper.writeValueAsBytes(element.data)
    // scalastyle:off null
    new ProducerRecord[Array[Byte], Array[Byte]](topic, partition, element.data.getMeta.timestamp().toEpochMilli, null, messageBody)
    // scalastyle:on null
  }
}
