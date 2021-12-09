package org.wikidata.query.rdf.updater

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.scalatest.{FlatSpec, Matchers}
import org.wikidata.query.rdf.tool.change.events._
import org.wikidata.query.rdf.tool.wikibase.WikibaseRepository.Uris
import org.wikidata.query.rdf.updater.config.{InputKafkaTopics, UpdaterPipelineInputEventStreamConfig}

import java.net.URI
import java.time.{Clock, Instant}
import scala.collection.JavaConverters.{mapAsJavaMapConverter, setAsJavaSetConverter}

class IncomingStreamsUnitTest extends FlatSpec with Matchers with TestFixtures{

  val resolver: IncomingStreams.EntityResolver = (_, title, _) => title

  val uris: Uris = new Uris(new URI("https://" + DOMAIN), Set(0, 2, 3, 5, 6).map(_.toLong).map(long2Long).asJava, "/unused", "/wiki/Special:EntityData/")
  "IncomingStreams" should "create properly named streams" in {
    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = IncomingStreams.fromKafka(KafkaConsumerProperties("my-topic", "broker1", "group",
      DeserializationSchemaFactory.getDeserializationSchema(classOf[RevisionCreateEvent])),
      uris, IncomingStreams.REV_CREATE_CONV, 40000, 40000, Clock.systemUTC(), resolver, None)
    stream.name should equal (s"Filtered(my-topic == $DOMAIN)")
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
        10, 10, Set(), "mediainfo"),
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
        10, 10, Set(), "mediainfo"),
      uris, Clock.systemUTC()
    )
    stream.map(_.name) should contain only(s"Filtered(rev-create-topic == $DOMAIN)",
                                           s"Filtered(page-delete-topic == $DOMAIN)",
                                           s"Filtered(page-undelete-topic == $DOMAIN)",
                                           s"Filtered(suppressed-delete-topic == $DOMAIN)"
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
        10, 10, Set(), "mediainfo"),
      uris, Clock.systemUTC())
    stream.map(_.name) should contain only(
      s"Filtered(cluster1.rev-create-topic == $DOMAIN)",
      s"Filtered(cluster1.page-delete-topic == $DOMAIN)",
      s"Filtered(cluster1.page-undelete-topic == $DOMAIN)",
      s"Filtered(cluster1.suppressed-delete-topic == $DOMAIN)",
      s"Filtered(cluster2.rev-create-topic == $DOMAIN)",
      s"Filtered(cluster2.page-delete-topic == $DOMAIN)",
      s"Filtered(cluster2.page-undelete-topic == $DOMAIN)",
      s"Filtered(cluster2.suppressed-delete-topic == $DOMAIN)"
    )
  }

  "EventWithMetadataHostFilter" should "filter events by hostname" in {
    val filter = new EventWithMetadataHostFilter[FakeEvent](uris)
    filter.filter(FakeEvent("not-my-host", "Q123")) should equal(false)
    filter.filter(FakeEvent(DOMAIN, "Unrelated", 10)) should equal(false)
    filter.filter(FakeEvent(DOMAIN, "Q123")) should equal(true)
  }

  "EventWithMetadataHostFilter" should "filter events by namespace" in {
    val filter = new EventWithMetadataHostFilter[FakeEvent](uris)
    filter.filter(FakeEvent(DOMAIN, "Q123", 2)) should equal(true)
    filter.filter(FakeEvent(DOMAIN, "Q123", 10)) should equal(false)
  }

  "IncomingStreams" should "filter out messages based on mediainfo namespace and slot" in {
    val filter = new RevisionCreateEventFilter(Set(6), "mediainfo")
    val mainSlot = "main" ->
      new RevisionSlot("main", "123fewfwe", 1571, 1)
    val mediainfoSlot = "mediainfo" -> new RevisionSlot("mediainfo", "123fewfwe", 1571, 1);

    val nonFileEvent = newRevCreateEvent("M1", 1, 2, 1, instant(4),
      0, DOMAIN, STREAM, ORIG_REQUEST_ID, Map(mainSlot))
    val nonMediaInfoEvent = newRevCreateEvent("M2", 1, 2, 1, instant(4),
      6, DOMAIN, STREAM, ORIG_REQUEST_ID, Map(mainSlot))
    val mediaInfoEvent = newRevCreateEvent("M3", 1, 2, 1, instant(4),
      6, DOMAIN, STREAM, ORIG_REQUEST_ID, Map(mainSlot, mediainfoSlot))
    val mediaInfoWrongDomainEvent = newRevCreateEvent("M4", 1, 2, 1, instant(4),
      6, "wrong", STREAM, ORIG_REQUEST_ID, Map(mainSlot, mediainfoSlot))
    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val output: Set[InputEvent] = IncomingStreams.fromStream(env.fromCollection(Seq(nonFileEvent, nonMediaInfoEvent, mediaInfoEvent,mediaInfoWrongDomainEvent))
      .setParallelism(1),
      uris,
      IncomingStreams.REV_CREATE_CONV, clock, resolver, Some(filter)).executeAndCollect().toSet

    output.map(_.item) should contain only ("M1", "M3")
  }

  "RevCreateEvent" should "be convertible into InputEvent" in {
    val revSlots = Map("main" -> new RevisionSlot("main", "1123a", 1, 1)).asJava
    val event: RevCreate = IncomingStreams.REV_CREATE_CONV.apply(new RevisionCreateEvent(
      new EventsMeta(Instant.ofEpochMilli(123), "unused", "my-domain", "unused for now", "my_request_id"),
      "schema",
      1L,
      1234L,
      1233L,
      "Q123",
      1,
      revSlots),
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
  val meta = new EventsMeta(Instant.now(), "ID", domain, "stream", "reqID")
  override def revision(): Long = ???
  override def timestamp(): Instant = ???
  override def schema(): String = ???
}
