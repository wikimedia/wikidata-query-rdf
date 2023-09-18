package org.wikidata.query.rdf.updater

import java.net.URI
import java.time.{Clock, Instant}
import java.util.Collections

import scala.collection.JavaConverters.{mapAsJavaMapConverter, setAsJavaSetConverter}

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.scalatest.{FlatSpec, Matchers}
import org.wikidata.query.rdf.tool.EntityId
import org.wikidata.query.rdf.tool.change.events._
import org.wikidata.query.rdf.tool.change.events.ReconcileEvent.Action
import org.wikidata.query.rdf.tool.wikibase.WikibaseRepository.Uris
import org.wikidata.query.rdf.updater.config.{FilteredReconciliationTopic, InputKafkaTopics, UpdaterPipelineInputEventStreamConfig}

class IncomingStreamsUnitTest extends FlatSpec with Matchers with TestFixtures {

  val resolver: IncomingStreams.EntityResolver = (_, title, _) => title

  val uris: Uris = new Uris(new URI("https://" + DOMAIN), Set(0, 2, 3, 5, 6).map(_.toLong).map(long2Long).asJava, "/unused", "/wiki/Special:EntityData/")
  "IncomingStreams" should "create properly named streams" in {
    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = IncomingStreams.fromKafka(KafkaConsumerProperties("my-topic", "broker1", "group",
      DeserializationSchemaFactory.getDeserializationSchema(classOf[RevisionCreateEvent])),
      uris, IncomingStreams.REV_CREATE_CONV, 40000, 40000, Clock.systemUTC(), resolver, None, useNewFlinkApi = true)
    stream.name should equal (s"Filtered(KafkaSource:my-topic == $DOMAIN)")
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
          reconciliationTopicName = None,
          topicPrefixes = List("")),
        10, 10, Set(), "mediainfo", useNewFlinkKafkaApi = true),
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
          reconciliationTopicName = None,
          topicPrefixes = List("")),
        10, 10, Set(), "mediainfo", useNewFlinkKafkaApi = true),
      uris, Clock.systemUTC()
    )
    stream.map(_.name) should contain only(s"Filtered(KafkaSource:rev-create-topic == $DOMAIN)",
                                           s"Filtered(KafkaSource:page-delete-topic == $DOMAIN)",
                                           s"Filtered(KafkaSource:page-undelete-topic == $DOMAIN)",
                                           s"Filtered(KafkaSource:suppressed-delete-topic == $DOMAIN)"
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
          reconciliationTopicName = Some(FilteredReconciliationTopic("rdf-streaming-updater.reconcile", None)),
          topicPrefixes = List("cluster1.", "cluster2.")),
        10, 10, Set(), "mediainfo", useNewFlinkKafkaApi = true),
      uris, Clock.systemUTC())
    stream.map(_.name) should contain only(
      s"Filtered(KafkaSource:cluster1.rev-create-topic == $DOMAIN)",
      s"Filtered(KafkaSource:cluster1.page-delete-topic == $DOMAIN)",
      s"Filtered(KafkaSource:cluster1.page-undelete-topic == $DOMAIN)",
      s"Filtered(KafkaSource:cluster1.suppressed-delete-topic == $DOMAIN)",
      s"Filtered(KafkaSource:cluster1.rdf-streaming-updater.reconcile == $DOMAIN)",
      s"Filtered(KafkaSource:cluster2.rev-create-topic == $DOMAIN)",
      s"Filtered(KafkaSource:cluster2.page-delete-topic == $DOMAIN)",
      s"Filtered(KafkaSource:cluster2.page-undelete-topic == $DOMAIN)",
      s"Filtered(KafkaSource:cluster2.suppressed-delete-topic == $DOMAIN)",
      s"Filtered(KafkaSource:cluster2.rdf-streaming-updater.reconcile == $DOMAIN)"
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

    val nonFileEvent = newRevCreateEvent(EntityId.parse("M1"), 1, 2, 1, instant(4),
      0, DOMAIN, STREAM, ORIG_REQUEST_ID, Map(mainSlot))
    val nonMediaInfoEvent = newRevCreateEvent(EntityId.parse("M2"), 1, 2, 1, instant(4),
      6, DOMAIN, STREAM, ORIG_REQUEST_ID, Map(mainSlot))
    val mediaInfoEvent = newRevCreateEvent(EntityId.parse("M3"), 1, 2, 1, instant(4),
      6, DOMAIN, STREAM, ORIG_REQUEST_ID, Map(mainSlot, mediainfoSlot))
    val mediaInfoWrongDomainEvent = newRevCreateEvent(EntityId.parse("M4"), 1, 2, 1, instant(4),
      6, "wrong", STREAM, ORIG_REQUEST_ID, Map(mainSlot, mediainfoSlot))
    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val output: Set[InputEvent] = IncomingStreams.fromStream(env.fromCollection(Seq(nonFileEvent, nonMediaInfoEvent, mediaInfoEvent,mediaInfoWrongDomainEvent))
      .setParallelism(1),
      uris,
      IncomingStreams.REV_CREATE_CONV, clock, resolver, Some(filter)).executeAndCollect().toSet

    output.map(_.item) should contain only ("M1", "M3")
  }

  "IncomingStreams" should "filter out ReconcileEvent based on their source" in {
    val filter = new ReconciliationSourceFilter("my_source")

    val fromCorrectSource = newReconcileEvent(EntityId.parse("Q1"), 1, Action.CREATION, instant(4), "my_source")
    val fromOtherSource = newReconcileEvent(EntityId.parse("Q2"), 1, Action.CREATION, instant(4), "other_source")
    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val output: Set[InputEvent] = IncomingStreams.fromStream(env.fromCollection(Seq(fromCorrectSource, fromOtherSource))
      .setParallelism(1),
      uris,
      IncomingStreams.RECONCILIATION_CONV, clock, resolver, Some(filter)).executeAndCollect().toSet

    output.map(_.item) should contain only "Q1"
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

  "ReconcileEvent" should "be convertible into InputEvent" in {
    val event = IncomingStreams.RECONCILIATION_CONV.apply(new ReconcileEvent(
      new EventsMeta(Instant.ofEpochMilli(123), "unused", "my-domain", "unused for now", "my_request_id"),
      "schema",
      EntityId.parse("Q123"),
      1234,
      "source",
      Action.CREATION,
      new EventInfo(new EventsMeta(Instant.ofEpochMilli(123), "unused", "my-domain", "unused for now", "my_request_id"), "schema")
      ),
      resolver, Clock.systemUTC())
    event.eventTime should equal(Instant.ofEpochMilli(123))
    event.item should equal("Q123")
    event.revision should equal(1234)
    event.originalEventInfo.schema() should equal("schema")
    event.originalEventInfo.meta().requestId() should equal("my_request_id")
    event.originalEventInfo.meta().domain() should equal("my-domain")
  }
}

class EventWithMetadataHostFilterUnitTest extends FlatSpec with Matchers {
  "RevisionCreateEvent" should "be filterable" in {
    val filter = new EventWithMetadataHostFilter[RevisionCreateEvent](new Uris(URI.create("https://my.wikidata.org/"),
      Set[java.lang.Long](0L, 1L, 2L).asJava, "/api", "/entity"))
    def revCreate(domain: String, ns: Long): RevisionCreateEvent = {
      new RevisionCreateEvent(
        new EventsMeta(Instant.ofEpochMilli(123), "unused", domain, "unused for now", "my_request_id"),
        "schema", 1L, 1234L, 1233L, "Q123", ns, Collections.emptyMap())
    }
    filter.filter(revCreate("unrelated", 3L)) shouldBe false
    filter.filter(revCreate("unrelated", 2L)) shouldBe false
    filter.filter(revCreate("my.wikidata.org", 3L)) shouldBe false
    filter.filter(revCreate("my.wikidata.org", 2L)) shouldBe true
  }

  "PageDeleteEvent" should "be filterable" in {
    val filter = new EventWithMetadataHostFilter[PageDeleteEvent](new Uris(URI.create("https://my.wikidata.org/"),
      Set[java.lang.Long](0L, 1L, 2L).asJava, "/api", "/entity"))
    def pageDelete(domain: String, ns: Long): PageDeleteEvent = {
      new PageDeleteEvent(
        new EventsMeta(Instant.ofEpochMilli(123), "unused", domain, "unused for now", "my_request_id"),
        "schema", 1L, 1234L, "Q123", ns)
    }
    filter.filter(pageDelete("unrelated", 3L)) shouldBe false
    filter.filter(pageDelete("unrelated", 2L)) shouldBe false
    filter.filter(pageDelete("my.wikidata.org", 3L)) shouldBe false
    filter.filter(pageDelete("my.wikidata.org", 2L)) shouldBe true
  }

  "PageUndeleteEvent" should "be filterable" in {
    val filter = new EventWithMetadataHostFilter[PageUndeleteEvent](new Uris(URI.create("https://my.wikidata.org/"),
      Set[java.lang.Long](0L, 1L, 2L).asJava, "/api", "/entity"))
    def pageUndelete(domain: String, ns: Long): PageUndeleteEvent = {
      new PageUndeleteEvent(
        new EventsMeta(Instant.ofEpochMilli(123), "unused", domain, "unused for now", "my_request_id"),
        "schema", 1L, 1234L, "Q123", ns)
    }
    filter.filter(pageUndelete("unrelated", 3L)) shouldBe false
    filter.filter(pageUndelete("unrelated", 2L)) shouldBe false
    filter.filter(pageUndelete("my.wikidata.org", 3L)) shouldBe false
    filter.filter(pageUndelete("my.wikidata.org", 2L)) shouldBe true
  }

  "ReconcileEvent" should "be filterable" in {
    val filter = new EventWithMetadataHostFilter[ReconcileEvent](new Uris(URI.create("https://my.wikidata.org/"),
      Set[java.lang.Long](0L, 1L, 2L).asJava, "/api", "/entity"))
    def reconcile(domain: String): ReconcileEvent = {
      new ReconcileEvent(
        new EventsMeta(Instant.ofEpochMilli(123), "unused", domain, "unused for now", "my_request_id"),
        "schema", EntityId.parse("Q123"), 1L, "source", Action.CREATION,
        new EventInfo(new EventsMeta(Instant.ofEpochMilli(123), "unused", "unused", "unused for now", "original_request_id"), "schema"))
    }
    filter.filter(reconcile("unrelated")) shouldBe false
    filter.filter(reconcile("my.wikidata.org")) shouldBe true
  }
}



sealed case class FakeEvent(domain: String, title: String, namespace: Long = 0) extends ChangeEvent {
  val meta = new EventsMeta(Instant.now(), "ID", domain, "stream", "reqID")
  override def revision(): Long = ???
  override def timestamp(): Instant = ???
  override def schema(): String = ???
}
