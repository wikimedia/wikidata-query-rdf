package org.wikidata.query.rdf.updater

import java.time.{Clock, Instant}

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.scalatest.{FlatSpec, Matchers}
import org.wikidata.query.rdf.tool.change.events.{ChangeEvent, EventsMeta, PageDeleteEvent, PageUndeleteEvent, RevisionCreateEvent}
import org.wikidata.query.rdf.updater.config.UpdaterPipelineInputEventStreamConfig

class IncomingStreamsUnitTest extends FlatSpec with Matchers {
  "IncomingStreams" should "create properly named streams" in {
    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = IncomingStreams.fromKafka(KafkaConsumerProperties("my-topic", "broker1", "group",
      DeserializationSchemaFactory.getDeserializationSchema(classOf[RevisionCreateEvent])),
      "my-hostname", IncomingStreams.REV_CREATE_CONV, 1, 40000, 40000, Clock.systemUTC())
    stream.name should equal ("Filtered(my-topic == my-hostname)")
  }

  "IncomingStreams" should "create regular incoming streams proper parallelism" in {
    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = IncomingStreams.buildIncomingStreams(
      UpdaterPipelineInputEventStreamConfig("broker1", "consumerGroup1", "rev-create-topic",
        "page-delete-topic", "page-undelete-topic", List(""), 3, 10, 10),
      "hostname",
      Clock.systemUTC()
    )
    stream.map(_.parallelism).toSet should contain only 3
  }

  "IncomingStreams" should "create regular incoming streams when using no prefixes" in {
    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = IncomingStreams.buildIncomingStreams(
      UpdaterPipelineInputEventStreamConfig("broker1", "consumerGroup1", "rev-create-topic",
        "page-delete-topic", "page-undelete-topic", List(""), 1, 10, 10),
      "hostname",
      Clock.systemUTC()
    )
    stream.map(_.name) should contain only("Filtered(rev-create-topic == hostname)",
                                           "Filtered(page-delete-topic == hostname)",
                                           "Filtered(page-undelete-topic == hostname)")
  }

  "IncomingStreams" should "create twice more incoming streams when using 2 prefixes" in {
    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = IncomingStreams.buildIncomingStreams(
      UpdaterPipelineInputEventStreamConfig("broker1", "consumerGroup1", "rev-create-topic",
        "page-delete-topic", "page-undelete-topic", List("cluster1.", "cluster2."), 1, 10, 10),
      "hostname",
      Clock.systemUTC()
    )
    stream.map(_.name) should contain only(
      "Filtered(cluster1.rev-create-topic == hostname)",
      "Filtered(cluster1.page-delete-topic == hostname)",
      "Filtered(cluster1.page-undelete-topic == hostname)",
      "Filtered(cluster2.rev-create-topic == hostname)",
      "Filtered(cluster2.page-delete-topic == hostname)",
      "Filtered(cluster2.page-undelete-topic == hostname)"
    )
  }

  "EventWithMetadataHostFilter" should "filter events by hostname" in {
    val filter = new EventWithMetadataHostFilter[FakeEvent]("my-host")
    filter.filter(FakeEvent("not-my-host", "Q123")) should equal(false)
    filter.filter(FakeEvent("my-host", "Unrelated")) should equal(false)
    filter.filter(FakeEvent("my-host", "Q123")) should equal(true)
  }

  "RevCreateEvent" should "be convertible into InputEvent" in {
    val event = IncomingStreams.REV_CREATE_CONV.apply(new RevisionCreateEvent(
      new EventsMeta(Instant.ofEpochMilli(123), "unused", "my-domain", "unused for now", "unused for now"),
      1234,
      "Q123",
      1),
      Clock.systemUTC())
    event.eventTime should equal(Instant.ofEpochMilli(123))
    event.item should equal("Q123")
    event.revision should equal(1234)
  }

  "PageDelEvent" should "be convertible into InputEvent" in {
    val event = IncomingStreams.PAGE_DEL_CONV.apply(new PageDeleteEvent(
      new EventsMeta(Instant.ofEpochMilli(123), "unused", "my-domain", "unused for now", "unused for now"),
      1234,
      "Q123",
      1),
      Clock.systemUTC())
    event.eventTime should equal(Instant.ofEpochMilli(123))
    event.item should equal("Q123")
    event.revision should equal(1234)
  }

  "PageUndeleteEvent" should "be convertible into InputEvent" in {
    val event = IncomingStreams.PAGE_UNDEL_CONV.apply(new PageUndeleteEvent(
      new EventsMeta(Instant.ofEpochMilli(123), "unused", "my-domain", "unused for now", "unused for now"),
      1234,
      "Q123",
      1),
      Clock.systemUTC())
    event.eventTime should equal(Instant.ofEpochMilli(123))
    event.item should equal("Q123")
    event.revision should equal(1234)
  }
}

sealed case class FakeEvent(domain: String, title: String) extends ChangeEvent {
  override def revision(): Long = ???
  override def namespace(): Long = if (title.startsWith("Q")) 0L else 1L
  override def timestamp(): Instant = ???
}
