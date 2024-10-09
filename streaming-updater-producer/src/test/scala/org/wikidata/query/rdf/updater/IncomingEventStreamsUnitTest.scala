package org.wikidata.query.rdf.updater

import org.apache.flink.types.Row
import org.scalatest.{FlatSpec, Matchers}
import org.wikidata.query.rdf.tool.change.events.{EventInfo, EventsMeta}
import org.wikidata.query.rdf.tool.wikibase.WikibaseRepository.Uris
import org.wikimedia.eventutilities.core.event.{EventSchemaLoader, EventStreamConfig, EventStreamFactory, StaticEventStreamConfigLoader}
import org.wikimedia.eventutilities.core.json.{JsonLoader, JsonSchemaLoader}
import org.wikimedia.eventutilities.core.util.ResourceLoader
import org.wikimedia.eventutilities.flink.stream.EventDataStreamFactory

import java.net.URI
import java.time.{Clock, Instant, ZoneOffset}
import java.util
import java.util.Collections
import scala.collection.JavaConverters._

class IncomingEventStreamsUnitTest extends FlatSpec with Matchers {
  private val schemaRepos: List[String] = List(
    "https://schema.wikimedia.org/repositories/primary/jsonschema",
    "https://schema.wikimedia.org/repositories/secondary/jsonschema",
    // useful to put test schemas while changes are being reviewed on the schemas repos
    this.getClass.getResource("/schema_repo/").toString)

  private val resourceLoader = ResourceLoader.builder()
    .setBaseUrls(ResourceLoader.asURLs(schemaRepos.asJava))
    .build()
  private val jsonLoader: JsonLoader = new JsonLoader(resourceLoader)
  private val eventStreamConfigLoader = new StaticEventStreamConfigLoader(
    this.getClass.getResource("/IncomingEventStreamsUnitTest-stream-config.json").toURI,
    jsonLoader
  )
  private val eventStreamConfig: EventStreamConfig = EventStreamConfig.builder()
    .setEventStreamConfigLoader(eventStreamConfigLoader)
    .setEventServiceToUriMap(Collections.emptyMap(): java.util.Map[String, URI])
    .build()

  private val eventStreamFactory = EventStreamFactory.builder()
    .setEventStreamConfig(eventStreamConfig)
    .setEventSchemaLoader(EventSchemaLoader.builder().setJsonSchemaLoader(JsonSchemaLoader.build(resourceLoader)).build())
    .build()

  private val eventDataStreamFactory = EventDataStreamFactory.builder()
    .eventStreamFactory(eventStreamFactory)
    .build()

  "HostFilter" should "filter on hostname" in {
    val uris = Uris.withWikidataDefaults("https://myhostname.unittest.local/")
    val filter = new HostFilter(uris)
    val meta = Row.withNames()
    val row = Row.withNames()
    meta.setField("domain", "myhostname.unittest.local")
    row.setField("meta", meta)
    filter.filter(row) shouldBe true

    meta.setField("domain", "anotherhost.unittest.local")
    filter.filter(row) shouldBe false
  }

  "NamespaceFilter" should "filter on namespace" in {
    val uris = new Uris(new URI("https://unused.unittest.local/"), Set(2L, 4L).map(long2Long).asJava, "unused", "unused")
    val filter = new NamespaceFilter(uris)
    val page = Row.withNames()
    val row = Row.withNames()
    page.setField("namespace_id", 2L)
    row.setField("page", page)
    filter.filter(row) shouldBe true
    page.setField("namespace_id", 4L)
    filter.filter(row) shouldBe true
    page.setField("namespace_id", 3L)
    filter.filter(row) shouldBe false
  }

  "ReconciliationEventSourceFilter" should "filter on reconciliation_source" in {
    val filter = new ReconciliationEventSourceFilter("my_source")
    val row = Row.withNames()
    row.setField("reconciliation_source", "my_source")
    filter.filter(row) shouldBe true
    row.setField("reconciliation_source", "another_source")
    filter.filter(row) shouldBe false
  }

  "ContentModelFilter" should "filter on content model" in {
    val rowTypeInfo = eventDataStreamFactory.rowTypeInfo("mediawiki.page_change.v1")
    val filter = new ContentModelFilter(Set("content-model-1", "content-model-2"), Set(2L), "my-mediainfo-slot")
    val row = rowTypeInfo.createEmptyRow()

    withClue("events without the expected fields are excluded with a log message:") {
      // empty event
      filter.filter(row) shouldBe false
    }

    val revision = rowTypeInfo.createEmptySubRow("revision")
    row.setField("revision", revision)
    val page = rowTypeInfo.createEmptySubRow("page")
    row.setField("page", page)
    page.setField("namespace_id", 0L)

    val contentSlots = new util.HashMap[String, Row]()
    revision.setField("content_slots", contentSlots)

    val mainSlot = rowTypeInfo.createEmptySubRow("revision.content_slots");
    contentSlots.put("main", mainSlot)

    withClue("events with proper content_model in the main slot are accepted:") {
      mainSlot.setField("content_model", "content-model-1")
      filter.filter(row) shouldBe true

      mainSlot.setField("content_model", "content-model-2")
      filter.filter(row) shouldBe true
    }

    withClue("events with improper content_model in the main slot (or without a main one) are excluded:") {
      mainSlot.setField("content_model", "content-model-3")
      filter.filter(row) shouldBe false

      contentSlots.remove("main")
      filter.filter(row) shouldBe false
    }

    withClue("events in the mediainfo supported namespaces with a the configured slot are accepted:") {
      mainSlot.setField("content_model", "something")
      contentSlots.put("main", mainSlot)
      contentSlots.put("my-mediainfo-slot", rowTypeInfo.createEmptySubRow("revision.content_slots"))
      page.setField("namespace_id", 2L)
      filter.filter(row) shouldBe true
    }

    withClue("events not in the mediainfo supported namespaces with a the configured slot are excluded:") {
      page.setField("namespace_id", 1L)
      filter.filter(row) shouldBe false
    }
  }

  "InputEventStreams" should "convert a page_change event into a InputEvent" in {
    val eventTime = Instant.now()
    val messageTime = eventTime.plusSeconds(1)
    val rowType = eventDataStreamFactory.rowTypeInfo("mediawiki.page_change.v1")
    val row = rowType.createEmptyRow()
    val page = rowType.createEmptySubRow("page")
    row.setField("page", page)
    val meta = rowType.createEmptySubRow("meta")
    row.setField("meta", meta)
    val revision = rowType.createEmptySubRow("revision")
    row.setField("revision", revision)

    meta.setField("id", "my_id")
    meta.setField("dt", messageTime)
    meta.setField("domain", "my_domain")
    meta.setField("stream", "my_stream")
    meta.setField("request_id", "my_request_id")
    row.setField("$schema", "my_schema")
    revision.setField("rev_id", 123L)
    page.setField("namespace_id", 0L)
    page.setField("page_title", "Q123")
    page.setField("page_id", 321L)
    row.setField("dt", eventTime)

    val clock = Clock.fixed(Instant.EPOCH, ZoneOffset.UTC)
    val expectedEventInfo = new EventInfo(new EventsMeta(messageTime, "my_id", "my_domain", "my_stream", "my_request_id"), "my_schema")
    val converter = new PageChangeConverter(Set(6L), clock)
    Seq("create", "edit", "move").foreach { changeKind =>
      row.setField("page_change_kind", changeKind)
      converter.map(row) shouldBe
        RevCreate("Q123", eventTime, 123L, None, Instant.EPOCH, expectedEventInfo)
    }

    revision.setField("rev_parent_id", 122L)
    Seq("create", "edit", "move").foreach { changeKind =>
      row.setField("page_change_kind", changeKind)
      converter.map(row) shouldBe
        RevCreate("Q123", eventTime, 123L, Some(122L), Instant.EPOCH, expectedEventInfo)
    }

    page.setField("page_title", "Property:P123")
    page.setField("namespace_id", 120L)
    converter.map(row) shouldBe
      RevCreate("P123", eventTime, 123L, Some(122L), Instant.EPOCH, expectedEventInfo)

    page.setField("page_title", "File:Something")
    page.setField("namespace_id", 6L)

    converter.map(row) shouldBe
      RevCreate("M321", eventTime, 123L, Some(122L), Instant.EPOCH, expectedEventInfo)

    page.setField("page_title", "Q123")
    page.setField("namespace_id", 0L)

    row.setField("page_change_kind", "delete")
    converter.map(row) shouldBe
      PageDelete("Q123", eventTime, 123L, Instant.EPOCH, expectedEventInfo)

    row.setField("page_change_kind", "undelete")
    converter.map(row) shouldBe
      PageUndelete("Q123", eventTime, 123L, Instant.EPOCH, expectedEventInfo)
  }

  "InputEventStreams" should "convert a reconcile event into a InputEvent" in {
    val eventTime = Instant.now()
    val messageTime = eventTime.plusSeconds(1)
    val clock = Clock.fixed(Instant.EPOCH, ZoneOffset.UTC)
    val rowType = eventDataStreamFactory.rowTypeInfo("rdf-streaming-updater.reconcile")
    val row = rowType.createEmptyRow()
    val meta = rowType.createEmptySubRow("meta")
    row.setField("meta", meta)

    meta.setField("id", "my_id")
    meta.setField("dt", messageTime)
    meta.setField("domain", "my_domain")
    meta.setField("stream", "my_stream")
    meta.setField("request_id", "my_request_id")
    row.setField("$schema", "my_schema")

    row.setField("item", "Q123")
    row.setField("revision_id", 321L)
    row.setField("reconciliation_source", "only-used-for-filtering")
    row.setField("reconciliation_action", "CREATION")

    val converter = new ReconciliationRowConverter(clock)
    val expectedEventInfo = new EventInfo(new EventsMeta(messageTime, "my_id", "my_domain", "my_stream", "my_request_id"), "my_schema")
    converter.map(row) shouldBe
      ReconcileInputEvent("Q123", messageTime, 321L, ReconcileCreation, Instant.EPOCH, expectedEventInfo)

    row.setField("dt", eventTime)
    row.setField("reconciliation_action", "DELETION")
    converter.map(row) shouldBe
      ReconcileInputEvent("Q123", eventTime, 321L, ReconcileDeletion, Instant.EPOCH, expectedEventInfo)
  }
}
