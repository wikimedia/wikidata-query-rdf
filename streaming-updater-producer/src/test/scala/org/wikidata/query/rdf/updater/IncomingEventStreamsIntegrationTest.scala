package org.wikidata.query.rdf.updater

import org.apache.flink.api.connector.sink2.{Sink, SinkWriter}
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.scalatest.{FlatSpec, Matchers}
import org.wikidata.query.rdf.tool.change.events.{EventInfo, EventsMeta}
import org.wikidata.query.rdf.tool.wikibase.WikibaseRepository
import org.wikidata.query.rdf.tool.wikibase.WikibaseRepository.Uris
import org.wikidata.query.rdf.updater.config.{FilteredReconciliationStream, InputStreams, UpdaterPipelineInputEventStreamConfig}
import org.wikimedia.eventutilities.core.event.{EventSchemaLoader, EventStreamConfig, EventStreamFactory, StaticEventStreamConfigLoader}
import org.wikimedia.eventutilities.core.json.{JsonLoader, JsonSchemaLoader}
import org.wikimedia.eventutilities.core.util.ResourceLoader
import org.wikimedia.eventutilities.flink.stream.EventDataStreamFactory

import java.net.URI
import java.time.{Clock, Instant, ZoneOffset}
import java.util
import java.util.Collections
import scala.collection.JavaConverters._

class IncomingEventStreamsIntegrationTest extends FlatSpec with FlinkTestCluster with Matchers {
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

  private val clock: Clock = Clock.fixed(Instant.EPOCH, ZoneOffset.UTC)


  override def beforeEach(): Unit = {
    super.beforeEach()
    MyCollector.inputEvents.clear()
  }

  "IncomingEventStreams" should "parse and filter page_change events from wikidata like projects" in {
    val inputStreams = InputStreams(
      pageChangeStream = "mediawiki.page_change.v1",
      reconciliationStream = None,
      kafkaStartTimestamp = None,
      contentModels = Set("wikibase-item", "wikibase-property", "wikibase-lexeme")
    )
    val config = UpdaterPipelineInputEventStreamConfig(
      kafkaBrokers = "unused",
      consumerGroup = "unused",
      inputStreams = Right(inputStreams),
      maxLateness = 10,
      idleness = 10,
      mediaInfoEntityNamespaces = Set.empty,
      mediaInfoRevisionSlot = "unused",
      consumerProperties = Map.empty
    )
    val testWikidataUris = new Uris(new URI("https://test.wikidata.org"), Set(0L, 120L, 146L).map(long2Long).asJava,
      "unused", "unused")

    val paths = Seq(
      "item-create.json",
      "item-edit.json",
      "item-delete.json",
      "item-page-suppress.json",
      "item-undelete.json",
      "item-undelete-suppressed.json",
      "item-visibility-change.json",
      "non-item-create.json",
      "property-edit.json",
      "lexeme-create.json"
    ).map(e => new Path(this.getClass.getResource("/page_change_events/wikibase/" + e).toURI))

    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.disableGenericTypes()
    val source = eventDataStreamFactory.fileStreamSourceBuilder("mediawiki.page_change.v1", paths: _*).build()

    val incomingEventStreams = new IncomingEventStreams(config, testWikidataUris, eventDataStreamFactory, clock)
    val dataStream = incomingEventStreams.buildPageChangeSourceStream(source)

    dataStream.sinkTo(MyCollector.asSink())
    env.execute("test")
    val events = MyCollector.inputEvents.asScala.map(e => e.originalEventInfo.meta().id() -> e).toMap
    // item-create.json
    events("95cbd964-6e6e-420a-99c2-b774efc37796") shouldBe
      RevCreate("Q235989", Instant.parse("2024-10-04T15:09:55Z"), 686138, None, Instant.EPOCH,
        new EventInfo(new EventsMeta(Instant.parse("2024-10-04T15:09:55Z"), "95cbd964-6e6e-420a-99c2-b774efc37796",
          "test.wikidata.org", "mediawiki.page_change.v1", "fba1ef61-ac0e-46ad-be2b-cc5abe40d038"), "/mediawiki/page/change/1.2.0"))

    // item-edit.json
    events("4072ddcb-f522-45e5-9a66-6f0b5a495d1f") shouldBe
      RevCreate("Q235989", Instant.parse("2024-10-04T15:16:26Z"), 686139L, Some(686138L), Instant.EPOCH,
        new EventInfo(new EventsMeta(Instant.parse("2024-10-04T15:16:26Z"), "4072ddcb-f522-45e5-9a66-6f0b5a495d1f",
          "test.wikidata.org", "mediawiki.page_change.v1", "48e89d12-59b9-45bc-8183-a4462784f126"), "/mediawiki/page/change/1.2.0"))

    // item-delete.json
    events("b9da7c08-48ee-4f8b-8e57-3edbf650360e") shouldBe
      PageDelete("Q235989", Instant.parse("2024-10-04T15:26:14Z"), 686140, Instant.EPOCH,
        new EventInfo(new EventsMeta(Instant.parse("2024-10-04T15:26:14Z"), "b9da7c08-48ee-4f8b-8e57-3edbf650360e",
          "test.wikidata.org", "mediawiki.page_change.v1", "f4f7ae2c-b4b5-4ead-af53-78832f001c20"), "/mediawiki/page/change/1.2.0"))

    // item-page-suppress.json
    events("09e856d3-9901-4e97-95fb-650971492c45") shouldBe
      PageDelete("Q235989", Instant.parse("2024-10-04T15:32:58Z"), 686140, Instant.EPOCH,
        new EventInfo(new EventsMeta(Instant.parse("2024-10-04T15:32:58Z"), "09e856d3-9901-4e97-95fb-650971492c45",
          "test.wikidata.org", "mediawiki.page_change.v1", "ee3053e9-e2b8-4e7a-909c-6ae6a42126c2"), "/mediawiki/page/change/1.2.0"))

    // item-undelete.json
    events("5b7be9f7-208d-4ce6-b4a7-3fae6b7dea0a") shouldBe
      PageUndelete("Q235989", Instant.parse("2024-10-04T15:27:28Z"), 686140L, Instant.EPOCH,
        new EventInfo(new EventsMeta(Instant.parse("2024-10-04T15:27:28Z"), "5b7be9f7-208d-4ce6-b4a7-3fae6b7dea0a",
          "test.wikidata.org", "mediawiki.page_change.v1", "649988d1-f2f9-4ac0-9c57-56bbc3c51896"), "/mediawiki/page/change/1.2.0"))

    // item-undelete-suppressed.json
    events("20b289d5-5591-4e55-b051-423d655aaa52") shouldBe
      PageUndelete("Q235989", Instant.parse("2024-10-04T15:37:46Z"), 686140L, Instant.EPOCH,
        new EventInfo(new EventsMeta(Instant.parse("2024-10-04T15:37:46Z"), "20b289d5-5591-4e55-b051-423d655aaa52",
          "test.wikidata.org", "mediawiki.page_change.v1", "e2962c4d-13ae-40a1-a4b6-13d275183de4"), "/mediawiki/page/change/1.2.0"))

    // item-visibility-change
    events.get("3161448a-c58b-416f-b8a4-58f821a0ee8f") shouldBe None

    // non-item-create.json
    events.get("f555013c-c5f5-4aca-bd60-5dd437037b45") shouldBe None

    // property-edit.json
    events("4cd43ee0-417a-4b89-8626-79bc8cde8bee") shouldBe
      RevCreate("P115", Instant.parse("2024-10-04T16:36:56Z"), 686147L, Some(686146L), Instant.EPOCH,
        new EventInfo(new EventsMeta(Instant.parse("2024-10-04T16:36:56Z"), "4cd43ee0-417a-4b89-8626-79bc8cde8bee",
          "test.wikidata.org", "mediawiki.page_change.v1", "f7d711c8-5d2a-49e5-a044-2018bc7dac34"), "/mediawiki/page/change/1.2.0"))

    // lexeme-create.json
    events("5de98095-b359-4f50-ba53-b82bba8de6fe") shouldBe
      RevCreate("L4227", Instant.parse("2024-10-08T07:35:59Z"), 686857L, None, Instant.EPOCH,
        new EventInfo(new EventsMeta(Instant.parse("2024-10-08T07:35:59Z"), "5de98095-b359-4f50-ba53-b82bba8de6fe",
          "test.wikidata.org", "mediawiki.page_change.v1", "ace56041-4e6a-49be-a292-4bd12579ab86"), "/mediawiki/page/change/1.2.0"))
  }

  "IncomingEventStreams" should "parse and filter page_change events from MediaInfo projects" in {
    val inputStreams = InputStreams(
      pageChangeStream = "mediawiki.page_change.v1",
      reconciliationStream = None,
      kafkaStartTimestamp = None,
      contentModels = Set()
    )
    val config = UpdaterPipelineInputEventStreamConfig(
      kafkaBrokers = "unused",
      consumerGroup = "unused",
      inputStreams = Right(inputStreams),
      maxLateness = 10,
      idleness = 10,
      mediaInfoEntityNamespaces = Set(6),
      mediaInfoRevisionSlot = "mediainfo",
      consumerProperties = Map.empty
    )
    val commonsWikiUris = new WikibaseRepository.Uris(new URI(s"https://commons.wikimedia.org"),
      config.mediaInfoEntityNamespaces.map(long2Long).asJava,
      WikibaseRepository.Uris.DEFAULT_ENTITY_DATA_PATH, // unused by the pipeline
      "/unused")

    val paths = Seq(
      "edit-mediainfo-item.json",
      "move-mediainfo-item.json",
      "edit-non-mediainfo-item.json"
    ).map(e => new Path(this.getClass.getResource("/page_change_events/mediainfo/" + e).toURI))

    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.disableGenericTypes()
    val source = eventDataStreamFactory.fileStreamSourceBuilder("mediawiki.page_change.v1", paths: _*).build()

    val incomingEventStreams = new IncomingEventStreams(config, commonsWikiUris, eventDataStreamFactory, clock)
    val dataStream = incomingEventStreams.buildPageChangeSourceStream(source)

    dataStream.sinkTo(MyCollector.asSink())
    env.execute("test")
    val events = MyCollector.inputEvents.asScala.map(e => e.originalEventInfo.meta().id() -> e).toMap

    // edit-mediainfo-item.json
    events("98d46c7d-322d-408e-a27f-edc634186a5f") shouldBe
      RevCreate("M114491269", Instant.parse("2024-10-07T19:33:46Z"), 934984984L, Some(830021987L), Instant.EPOCH,
        new EventInfo(new EventsMeta(Instant.parse("2024-10-07T19:33:46Z"), "98d46c7d-322d-408e-a27f-edc634186a5f",
          "commons.wikimedia.org", "mediawiki.page_change.v1", "42788cff-7fc3-46c4-a02f-79905401e7ce"), "/mediawiki/page/change/1.2.0"))

    events("120bbbc5-0da9-4f73-bdb9-9a65e696f761") shouldBe
      RevCreate("M49988679", Instant.parse("2024-09-30T22:33:39Z"), 930818273L, Some(930818220L), Instant.EPOCH,
        new EventInfo(new EventsMeta(Instant.parse("2024-09-30T22:33:43Z"), "120bbbc5-0da9-4f73-bdb9-9a65e696f761",
          "commons.wikimedia.org", "mediawiki.page_change.v1", "c0f7b1db-5ac4-4730-91c5-de9bb83179c4"), "/mediawiki/page/change/1.2.0"))

    // edit-non-mediainfo-item.json
    events.get("b8347404-f361-4870-8586-406ce22f9cad") shouldBe None
  }

  "IncomingEventStreams" should "parse and filter reconciliation events" in {
    val testWikidataUris = Uris.withWikidataDefaults(new URI("https://test.wikidata.org"))
    val inputStreams = InputStreams(
      pageChangeStream = "unused",
      reconciliationStream = Some(FilteredReconciliationStream("rdf-streaming-updater.reconcile", Some("wdqs_sideoutputs_reconcile@eqiad"))),
      kafkaStartTimestamp = None,
      contentModels = Set.empty // unused
    )
    val config = UpdaterPipelineInputEventStreamConfig(
      kafkaBrokers = "unused",
      consumerGroup = "unused",
      inputStreams = Right(inputStreams),
      maxLateness = 10,
      idleness = 10,
      mediaInfoEntityNamespaces = Set.empty,
      mediaInfoRevisionSlot = "unused",
      consumerProperties = Map.empty
    )
    val paths = Seq(
      "reconcile-creation.json",
      "reconcile-creation-unrelated-source.json"
    ).map(e => new Path(this.getClass.getResource("/reconciliation_events/" + e).toURI))

    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.disableGenericTypes()
    val source = eventDataStreamFactory.fileStreamSourceBuilder("rdf-streaming-updater.reconcile", paths: _*).build()
    val incomingEventStreams = new IncomingEventStreams(config, testWikidataUris, eventDataStreamFactory, clock)
    val dataStream = incomingEventStreams.buildFilteredReconciliationDataStream("rdf-streaming-updater.reconcile",
      source, inputStreams.reconciliationStream.get.source)

    dataStream.sinkTo(MyCollector.asSink())
    env.execute("test")
    val events = MyCollector.inputEvents.asScala.map(e => e.originalEventInfo.meta().id() -> e).toMap

    // reconcile-creation
    events("2bf7aa16-2792-4487-afa3-8d11443c9298") shouldBe
      ReconcileInputEvent("Q92474238", Instant.parse("2024-10-08T01:29:46.634Z"), 2257669841L, ReconcileCreation, Instant.EPOCH,
        new EventInfo(new EventsMeta(Instant.parse("2024-10-08T01:29:46.634Z"), "2bf7aa16-2792-4487-afa3-8d11443c9298",
          "test.wikidata.org", "rdf-streaming-updater.reconcile", "ccf890fb-5b29-4dcb-9d06-d1a6fc0da254"), "/rdf_streaming_updater/reconcile/1.0.0"))

    // reconcile-creation-unrelated-source
    events.get("d5251d9a-27ae-4967-8c90-3d43c8504e8f") shouldBe None
  }

  "IncomingEventStreams" should "build a job graph" in {
    val inputStreams = InputStreams(
      pageChangeStream = "mediawiki.page_change.v1",
      reconciliationStream = Some(FilteredReconciliationStream("rdf-streaming-updater.reconcile", Some("wdqs_sideoutputs_reconcile@eqiad"))),
      kafkaStartTimestamp = None,
      contentModels = Set("wikibase-item", "wikibase-property")
    )
    val config = UpdaterPipelineInputEventStreamConfig(
      kafkaBrokers = "broker.unittest.local",
      consumerGroup = "my-consumer-group",
      inputStreams = Right(inputStreams),
      maxLateness = 10,
      idleness = 10,
      mediaInfoEntityNamespaces = Set(6L),
      mediaInfoRevisionSlot = "mediainfo",
      consumerProperties = Map("my-property" -> "my-value")
    )

    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.disableGenericTypes()
    val testWikidataUris = Uris.withWikidataDefaults(new URI("https://test.wikidata.org"))
    val incomingEventStreams = new IncomingEventStreams(config, testWikidataUris, eventDataStreamFactory, clock)

    incomingEventStreams.buildIncomingStreams()
    val plan = env.getExecutionPlan
    plan should include("Source: KafkaSource:mediawiki.page_change.v1")
    plan should include("Source: KafkaSource:rdf-streaming-updater.reconcile")
    plan should include("filtered:KafkaSource:mediawiki.page_change.v1")
    plan should include("filtered:KafkaSource:rdf-streaming-updater.reconcile")
    plan should include("converted:KafkaSource:mediawiki.page_change.v1")
    plan should include("converted:KafkaSource:rdf-streaming-updater.reconcile")
  }
}


object MyCollector {
  val inputEvents: util.List[InputEvent] = Collections.synchronizedList(new util.ArrayList[InputEvent]())
  def asSink(): Sink[InputEvent] = (_: Sink.InitContext) => new SinkWriter[InputEvent] {
    override def write(element: InputEvent, context: SinkWriter.Context): Unit = {
      MyCollector.inputEvents.add(element)
    }
    override def flush(endOfInput: Boolean): Unit = {}
    override def close(): Unit = {}
  }

}
