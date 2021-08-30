package org.wikidata.query.rdf.updater

import java.net.URI
import java.time.{Clock, Instant}

import scala.collection.JavaConverters.setAsJavaSetConverter

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.scalatest.{FlatSpec, Matchers}
import org.wikidata.query.rdf.tool.change.events.{ChangeEvent, EventsMeta, PageDeleteEvent, PageUndeleteEvent, RevisionCreateEvent}
import org.wikidata.query.rdf.tool.wikibase.WikibaseRepository.Uris
import org.wikidata.query.rdf.updater.config.{InputKafkaTopics, UpdaterPipelineInputEventStreamConfig}

class IncomingStreamsUnitTest extends FlatSpec with Matchers {

  val resolver: IncomingStreams.EntityResolver = (_, title, _) => title

  val uris: Uris = new Uris(new URI("https://my-hostname"), Set(0, 2, 3, 5).map(_.toLong).map(long2Long).asJava, "/unused", "/wiki/Special:EntityData/")
  "IncomingStreams" should "create properly named streams" in {
    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = IncomingStreams.fromKafka(KafkaConsumerProperties("my-topic", "broker1", "group",
      DeserializationSchemaFactory.getDeserializationSchema(classOf[RevisionCreateEvent])),
      uris, IncomingStreams.REV_CREATE_CONV, 40000, 40000, Clock.systemUTC(), resolver)
    stream.name should equal ("Filtered(my-topic == my-hostname)")
  }

  "IncomingStreams" should "create regular incoming streams proper parallelism" in {
    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = IncomingStreams.buildIncomingStreams(
      UpdaterPipelineInputEventStreamConfig("broker1", "consumerGroup1",
        InputKafkaTopics(
          revisionCreateTopicName = "rev-create-topic",
          pageDeleteTopicName = "page-delete-topic",
          pageUndeleteTopicName = "page-undelete-topic",
          suppressedDeleteTopicName = "suppressed-delete-topic",
          topicPrefixes = List("")),
        10, 10, Set()),
      uris, Clock.systemUTC())
    stream.map(_.parallelism).toSet should contain only 1
  }

  "IncomingStreams" should "create regular incoming streams when using no prefixes" in {
    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = IncomingStreams.buildIncomingStreams(
      UpdaterPipelineInputEventStreamConfig("broker1", "consumerGroup1",
        InputKafkaTopics(
          revisionCreateTopicName = "rev-create-topic",
          pageDeleteTopicName = "page-delete-topic",
          pageUndeleteTopicName = "page-undelete-topic",
          suppressedDeleteTopicName = "suppressed-delete-topic",
          topicPrefixes = List("")),
        10, 10, Set()),
      uris, Clock.systemUTC()
    )
    stream.map(_.name) should contain only("Filtered(rev-create-topic == my-hostname)",
                                           "Filtered(page-delete-topic == my-hostname)",
                                           "Filtered(page-undelete-topic == my-hostname)",
                                           "Filtered(suppressed-delete-topic == my-hostname)"
    )
  }

  "IncomingStreams" should "create twice more incoming streams when using 2 prefixes" in {
    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = IncomingStreams.buildIncomingStreams(
      UpdaterPipelineInputEventStreamConfig("broker1", "consumerGroup1",
      InputKafkaTopics(
          revisionCreateTopicName = "rev-create-topic",
          pageDeleteTopicName = "page-delete-topic",
          pageUndeleteTopicName = "page-undelete-topic",
          suppressedDeleteTopicName = "suppressed-delete-topic",
          topicPrefixes = List("cluster1.", "cluster2.")),
        10, 10, Set()),
      uris, Clock.systemUTC())
    stream.map(_.name) should contain only(
      "Filtered(cluster1.rev-create-topic == my-hostname)",
      "Filtered(cluster1.page-delete-topic == my-hostname)",
      "Filtered(cluster1.page-undelete-topic == my-hostname)",
      "Filtered(cluster1.suppressed-delete-topic == my-hostname)",
      "Filtered(cluster2.rev-create-topic == my-hostname)",
      "Filtered(cluster2.page-delete-topic == my-hostname)",
      "Filtered(cluster2.page-undelete-topic == my-hostname)",
      "Filtered(cluster2.suppressed-delete-topic == my-hostname)"
    )
  }

  "EventWithMetadataHostFilter" should "filter events by hostname" in {
    val filter = new EventWithMetadataHostFilter[FakeEvent](uris)
    filter.filter(FakeEvent("not-my-host", "Q123")) should equal(false)
    filter.filter(FakeEvent("my-hostname", "Unrelated", 10)) should equal(false)
    filter.filter(FakeEvent("my-hostname", "Q123")) should equal(true)
  }

  "EventWithMetadataHostFilter" should "filter events by namespace" in {
    val filter = new EventWithMetadataHostFilter[FakeEvent](uris)
    filter.filter(FakeEvent("my-hostname", "Q123", 2)) should equal(true)
    filter.filter(FakeEvent("my-hostname", "Q123", 10)) should equal(false)
  }

  "RevCreateEvent" should "be convertible into InputEvent" in {
    val event: RevCreate = IncomingStreams.REV_CREATE_CONV.apply(new RevisionCreateEvent(
      new EventsMeta(Instant.ofEpochMilli(123), "unused", "my-domain", "unused for now", "my_request_id"),
      "schema",
      1L,
      1234L,
      1233L,
      "Q123",
      1),
      resolver, Clock.systemUTC()).asInstanceOf[RevCreate]
    event.eventTime should equal(Instant.ofEpochMilli(123))
    event.item should equal("Q123")
    event.revision should equal(1234)
    event.parentRevision should equal(Some(1233))
    event.originalEventInfo.schema() should equal("schema")
    event.originalEventInfo.meta().requestId() should equal("my_request_id")
    event.originalEventInfo.meta().domain() should equal("my-domain")
  }

  "PageDelEvent" should "be convertible into InputEvent" in {
    val event = IncomingStreams.PAGE_DEL_CONV.apply(new PageDeleteEvent(
      new EventsMeta(Instant.ofEpochMilli(123), "unused", "my-domain", "unused for now", "my_request_id"),
      "schema",
      1,
      1234,
      "Q123",
      1),
      resolver, Clock.systemUTC())
    event.eventTime should equal(Instant.ofEpochMilli(123))
    event.item should equal("Q123")
    event.revision should equal(1234)
    event.originalEventInfo.schema() should equal("schema")
    event.originalEventInfo.meta().requestId() should equal("my_request_id")
    event.originalEventInfo.meta().domain() should equal("my-domain")
  }

  "PageUndeleteEvent" should "be convertible into InputEvent" in {
    val event = IncomingStreams.PAGE_UNDEL_CONV.apply(new PageUndeleteEvent(
      new EventsMeta(Instant.ofEpochMilli(123), "unused", "my-domain", "unused for now", "my_request_id"),
      "schema",
      1,
      1234,
      "Q123",
      1),
      resolver, Clock.systemUTC())
    event.eventTime should equal(Instant.ofEpochMilli(123))
    event.item should equal("Q123")
    event.revision should equal(1234)
    event.originalEventInfo.schema() should equal("schema")
    event.originalEventInfo.meta().requestId() should equal("my_request_id")
    event.originalEventInfo.meta().domain() should equal("my-domain")
  }
}


sealed case class FakeEvent(domain: String, title: String, namespace: Long = 0) extends ChangeEvent {
  override def revision(): Long = ???
  override def timestamp(): Instant = ???
}
