package org.wikidata.query.rdf.updater

import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema.KafkaSinkContext

import java.lang
import org.apache.kafka.clients.producer.ProducerRecord
import org.wikidata.query.rdf.tool.MapperUtils

/**
 * Json serializer that sends all events to a specific partition
 */
class MutationEventDataSerializationSchema(streamToTopicMap: Map[String, String], partition: Int) extends KafkaRecordSerializationSchema[MutationDataChunk] {
  override def serialize(element: MutationDataChunk,
                         kafkaSinkContext: KafkaSinkContext,
                         timestamp: lang.Long
                        ): ProducerRecord[Array[Byte], Array[Byte]] = {
    // we just produce the data (MutationEventData), rest is useful for monitoring and testing
    val messageBody = MapperUtils.getObjectMapper.writeValueAsBytes(element.data)
    val topic = streamToTopicMap
      .getOrElse(element.data.getMeta.stream(), throw new IllegalArgumentException("Unknown stream " + element.data.getMeta.stream()))
    // scalastyle:off null
    new ProducerRecord[Array[Byte], Array[Byte]](topic, partition, element.data.getMeta.timestamp().toEpochMilli, null, messageBody)
    // scalastyle:on null
  }
}
