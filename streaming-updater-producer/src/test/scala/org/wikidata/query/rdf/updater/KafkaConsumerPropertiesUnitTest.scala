package org.wikidata.query.rdf.updater

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.scalatest.{FlatSpec, Matchers}

class KafkaConsumerPropertiesUnitTest extends FlatSpec with Matchers {
  "KafkaConsumerProperties" should "create create properties to configure a consumer" in {
    val kafkaConsumerProperties = KafkaConsumerProperties("test", "test_brokers", "consumer_group_test", new SimpleStringSchema())
    val props = new Properties()
    props.setProperty("bootstrap.servers", "test_brokers")
    props.setProperty("group.id", "consumer_group_test")
    kafkaConsumerProperties.asProperties() should equal(props)
  }
}
