package org.wikidata.query.rdf.updater

import java.util.Properties

import org.apache.flink.api.common.serialization.DeserializationSchema

/**
 * Simple case class to hold most info needed to spawn a consumer
 */
case class KafkaConsumerProperties[E](topic: String, brokers: String, consumerGroup: String, schema: DeserializationSchema[E]) {
  def asProperties(): Properties = {
    val props = new Properties()
    props.setProperty("bootstrap.servers", brokers)
    props.setProperty("group.id", consumerGroup)
    props
  }
}
