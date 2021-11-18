package org.wikidata.query.rdf.updater

import java.net.URI
import java.time.{Duration, Instant}
import java.util
import java.util.{Collections, UUID}
import java.util.function.Supplier

import scala.collection.JavaConverters._

import org.scalatest.{FlatSpec, Matchers}
import org.wikidata.query.rdf.tool.{HttpClientUtils, MapperUtils}
import org.wikidata.query.rdf.tool.change.events.{EventInfo, EventsMeta}
import org.wikidata.query.rdf.tool.exception.ContainedException
import org.wikidata.query.rdf.tool.wikibase.WikibaseEntityFetchException.Type
import org.wikidata.query.rdf.updater.config.HttpClientConfig
import org.wikimedia.eventutilities.core.event.{EventSchemaLoader, EventStreamConfig, JsonEventGenerator, StaticEventStreamConfigLoader, WikimediaDefaults}
import org.wikimedia.eventutilities.core.json.{JsonLoader, JsonSchemaLoader}
import org.wikimedia.eventutilities.core.util.ResourceLoader

class JsonEncodersUnitTest extends FlatSpec with Matchers with TestEventGenerator {
  private val item: String = "Q1"
  private val uniqueId: String = UUID.randomUUID().toString
  private val revision: Long = 123
  private val fromRevision: Long = 122
  private val eventTime: Instant = Instant.now().minus(Duration.ofHours(2))
  private val ingestionTime: Instant = Instant.now().minus(Duration.ofHours(1))
  private val processingTime: Instant = Instant.now()
  private val domain: String = "tested.domain"
  private val stream: String = "tested.stream"
  private val uuid: String = UUID.randomUUID().toString
  private val requestId: String = UUID.randomUUID().toString
  private val eventInfo: EventInfo = new EventInfo(new EventsMeta(eventTime, uuid, domain, stream, requestId), "schema")

  private val schemaRepos: List[String] = List(
    "https://schema.wikimedia.org/repositories/primary/jsonschema",
    "https://schema.wikimedia.org/repositories/secondary/jsonschema",
    // useful to put test schemas while changes are being reviewed on the schemas repos
    this.getClass.getResource("/schema_repo/").toString)

  private val jsonLoader: JsonLoader = new JsonLoader(ResourceLoader.builder()
    .setBaseUrls(ResourceLoader.asURLs(schemaRepos.asJava))
    .build())
  private val eventStreamConfigLoader = new StaticEventStreamConfigLoader(
    this.getClass.getResource("/JsonEncodersUnitTest-eventstream-config.json").toURI,
    jsonLoader
  )
  private val eventStreamConfig: EventStreamConfig = EventStreamConfig.builder()
    .setEventStreamConfigLoader(eventStreamConfigLoader)
    .setEventServiceToUriMap(Collections.emptyMap(): java.util.Map[String, URI])
    .build()

  private val processingTimeClock: Supplier[Instant] = new Supplier[Instant] {
    def get: Instant = processingTime
  }
  private val clock: () => Instant = () => processingTime
  private val jsonEventGeneratorSupplier: () => JsonEventGenerator = () => JsonEventGenerator.builder()
    .eventStreamConfig(eventStreamConfig)
    .schemaLoader(EventSchemaLoader.builder().setJsonSchemaLoader(new JsonSchemaLoader(jsonLoader)).build())
    .ingestionTimeClock(processingTimeClock).build()
  private val sideOutputDomain = "sideOutputDomain"
  private val eventStreamConfigEndpoint = WikimediaDefaults.EVENT_STREAM_CONFIG_URI
  private val httpClientConfig = HttpClientConfig(httpRoutes = None, httpTimeout = None, HttpClientUtils.WDQS_DEFAULT_UA)
  private val jsonEncoder = new JsonEncoders(sideOutputDomain, () => uniqueId)

  "RevCreateEvent" should "be encoded properly as a json record" in {
    val inputEvent = RevCreate(item, eventTime, revision, Some(revision-1), ingestionTime, eventInfo)
    val eventCreator = jsonEncoder.lapsedActionEvent(inputEvent)
    val jsonEvent = jsonEventGeneratorSupplier().generateEvent(JsonEncoders.lapsedActionStream, JsonEncoders.lapsedActionSchema,
      eventCreator, processingTime)
    val event = jsonEventGeneratorSupplier().serializeAsBytes(jsonEvent)
    val record: util.Map[String, Object] = MapperUtils.getObjectMapper.readValue(event, classOf[util.Map[String, Object]])
    assertBasicMetadata(record)
    assertNewMetadata(record.get("meta"), JsonEncoders.lapsedActionStream)
    record.get("$schema") shouldBe JsonEncoders.lapsedActionSchema
    record.get("action_type") shouldBe "revision-create"
    record.get("parent_revision_id").asInstanceOf[Number].longValue() shouldBe revision-1
  }

  "RevCreateEvent" should "be encoded properly as a json record even without a parent revision" in {
    val inputEvent = RevCreate(item, eventTime, revision, None, ingestionTime, eventInfo)
    val eventCreator = jsonEncoder.lapsedActionEvent(inputEvent)
    val jsonEvent = jsonEventGeneratorSupplier().generateEvent(JsonEncoders.lapsedActionStream, JsonEncoders.lapsedActionSchema,
      eventCreator, processingTime)
    val event = jsonEventGeneratorSupplier().serializeAsBytes(jsonEvent)

    val record: util.Map[String, Object] = MapperUtils.getObjectMapper.readValue(event, classOf[util.Map[String, Object]])
    assertBasicMetadata(record)
    assertNewMetadata(record.get("meta"), JsonEncoders.lapsedActionStream)
    record.get("$schema") shouldBe JsonEncoders.lapsedActionSchema
    record.get("action_type") shouldBe "revision-create"
    record.containsKey("parent_revision") shouldBe false
  }

  "PageDeleteEvent" should "be encoded properly as a json record" in {
    val inputEvent = PageDelete(item, eventTime, revision, ingestionTime, eventInfo)
    val eventCreator = jsonEncoder.lapsedActionEvent(inputEvent)
    val jsonEvent = jsonEventGeneratorSupplier().generateEvent(JsonEncoders.lapsedActionStream, JsonEncoders.lapsedActionSchema,
      eventCreator, processingTime)
    val event = jsonEventGeneratorSupplier().serializeAsBytes(jsonEvent)

    val record: util.Map[String, Object] = MapperUtils.getObjectMapper.readValue(event, classOf[util.Map[String, Object]])
    assertBasicMetadata(record)
    assertNewMetadata(record.get("meta"), JsonEncoders.lapsedActionStream)
    record.get("$schema") shouldBe JsonEncoders.lapsedActionSchema
    record.get("action_type") shouldBe "page-delete"
  }

  "PageUndelete" should "be encoded properly as a json record" in {
    val inputEvent = PageUndelete(item, eventTime, revision, ingestionTime, eventInfo)
    val eventCreator = jsonEncoder.lapsedActionEvent(inputEvent)
    val jsonEvent = jsonEventGeneratorSupplier().generateEvent(JsonEncoders.lapsedActionStream, JsonEncoders.lapsedActionSchema,
      eventCreator, processingTime)
    val event = jsonEventGeneratorSupplier().serializeAsBytes(jsonEvent)

    val record: util.Map[String, Object] = MapperUtils.getObjectMapper.readValue(event, classOf[util.Map[String, Object]])
    assertBasicMetadata(record)
    assertNewMetadata(record.get("meta"), JsonEncoders.lapsedActionStream)
    record.get("$schema") shouldBe JsonEncoders.lapsedActionSchema
    record.get("action_type") shouldBe "page-undelete"
  }

  "IgnoredMutation" should "be encoded properly as json" in {
    val inputEvent = RevCreate(item, eventTime, revision, Some(revision-1), ingestionTime, eventInfo)
    val state = State(Some(revision), EntityStatus.CREATED)
    val inconsistency = IgnoredMutation("Q1", eventTime, revision, inputEvent, ingestionTime, NewerRevisionSeen, state)
    val eventCreator = jsonEncoder.stateInconsistencyEvent(inconsistency)
    val jsonEvent = jsonEventGeneratorSupplier().generateEvent(JsonEncoders.stateInconsistencyStream, JsonEncoders.stateInconsistencySchema,
      eventCreator, processingTime)
    val event = jsonEventGeneratorSupplier().serializeAsBytes(jsonEvent)

    val record: util.Map[String, Object] = MapperUtils.getObjectMapper.readValue(event, classOf[util.Map[String, Object]])
    assertBasicMetadata(record)
    assertNewMetadata(record.get("meta"), JsonEncoders.stateInconsistencyStream)
    record.get("inconsistency") shouldBe NewerRevisionSeen.name
    record.get("state_revision_id").asInstanceOf[Number].longValue() shouldBe revision
    record.get("state_status") shouldBe "CREATED"
    record.get("action_type") shouldBe "revision-create"
    Some(record.get("parent_revision_id").asInstanceOf[Number].longValue()) shouldBe inputEvent.parentRevision
  }

  "ProblematicReconciliation" should "be encoded properly as json" in {
    val inputEvent = ReconcileInputEvent(item, eventTime, revision, ReconcileCreation, ingestionTime, eventInfo)
    val state = State(Some(revision), EntityStatus.DELETED)
    val inconsistency = ProblematicReconciliation("Q1", eventTime, revision, inputEvent, ingestionTime, ReconcileAmbiguousCreation, state,
      Reconcile("Q1", eventTime, revision, ingestionTime, eventInfo))
    val eventCreator = jsonEncoder.stateInconsistencyEvent(inconsistency)
    val jsonEvent = jsonEventGeneratorSupplier().generateEvent(JsonEncoders.stateInconsistencyStream, JsonEncoders.stateInconsistencySchema,
      eventCreator, processingTime)
    val event = jsonEventGeneratorSupplier().serializeAsBytes(jsonEvent)

    val record: util.Map[String, Object] = MapperUtils.getObjectMapper.readValue(event, classOf[util.Map[String, Object]])
    assertBasicMetadata(record)
    assertNewMetadata(record.get("meta"), JsonEncoders.stateInconsistencyStream)
    record.get("inconsistency") shouldBe ReconcileAmbiguousCreation.name
    record.get("state_revision_id").asInstanceOf[Number].longValue() shouldBe revision
    record.get("state_status") shouldBe "DELETED"
    record.get("action_type") shouldBe "reconcile-creation"
  }

  "FailedOp event" should "be encoded properly as an json" in {
    val e = new ContainedException("problem")
    val op = FailedOp(Diff(item, eventTime, revision, fromRevision, ingestionTime, eventInfo), e)
    val eventCreator = jsonEncoder.fetchFailureEvent(op)
    val jsonEvent = jsonEventGeneratorSupplier().generateEvent(JsonEncoders.fetchFailureStream, JsonEncoders.fetchFailureSchema,
      eventCreator, processingTime)
    val event = jsonEventGeneratorSupplier().serializeAsBytes(jsonEvent)

    val record: util.Map[String, Object] = MapperUtils.getObjectMapper.readValue(event, classOf[util.Map[String, Object]])
    assertBasicMetadata(record)
    assertNewMetadata(record.get("meta"), JsonEncoders.fetchFailureStream)
    record.get("exception_type") shouldBe e.getClass.getName
    record.get("exception_msg") shouldBe e.getMessage
    record.get("fetch_error_type") shouldBe Type.UNKNOWN.toString
    record.get("op_type") shouldBe "diff"
    record.get("from_revision_id").asInstanceOf[Number].longValue() shouldBe fromRevision
  }

  "JsonEncoders" should "provide a KafkaSerializationSchema for InputEvent" in {
    val kafkaSerSchema = new SideOutputSerializationSchema[InputEvent](Some(clock), "my_topic",
      JsonEncoders.lapsedActionStream, JsonEncoders.lapsedActionSchema, sideOutputDomain, eventStreamConfigEndpoint, schemaRepos, httpClientConfig)
    val inputEvent = RevCreate(item, eventTime, revision, Some(revision-1), ingestionTime, eventInfo)
    val record = kafkaSerSchema.serialize(inputEvent, Instant.now().toEpochMilli)
    record.timestamp() shouldBe processingTime.toEpochMilli
    record.topic() shouldBe "my_topic"
    val node = jsonLoader.parse(new String(record.value(), "UTF-8"))
    node.get("$schema").asText() shouldBe JsonEncoders.lapsedActionSchema
    node.get("meta").get("stream").asText() shouldBe JsonEncoders.lapsedActionStream
  }

  "JsonEncoders" should "provide a KafkaSerializationSchema for FailedOp" in {
    val kafkaSerSchema = new SideOutputSerializationSchema[FailedOp](Some(clock), "my_topic",
      JsonEncoders.fetchFailureStream, JsonEncoders.fetchFailureSchema, sideOutputDomain, eventStreamConfigEndpoint, schemaRepos, httpClientConfig)
    val e = new ContainedException("problem")
    val op = FailedOp(Reconcile(item, eventTime, revision, ingestionTime, eventInfo), e)
    val record = kafkaSerSchema.serialize(op, Instant.now().toEpochMilli)
    record.timestamp() shouldBe processingTime.toEpochMilli
    record.topic() shouldBe "my_topic"
    val node = jsonLoader.parse(new String(record.value(), "UTF-8"))
    node.get("$schema").asText() shouldBe JsonEncoders.fetchFailureSchema
    node.get("meta").get("stream").asText() shouldBe JsonEncoders.fetchFailureStream
  }

  "JsonEncoders" should "provide a KafkaSerializationSchema for IgnoredMutation" in {
    val kafkaSerSchema = new SideOutputSerializationSchema[IgnoredMutation](Some(clock), "my_topic",
      JsonEncoders.stateInconsistencyStream, JsonEncoders.stateInconsistencySchema, sideOutputDomain, eventStreamConfigEndpoint, schemaRepos, httpClientConfig)
    val inputEvent = RevCreate(item, eventTime, revision, Some(revision-1), ingestionTime, eventInfo)
    val state = State(Some(revision), EntityStatus.CREATED)
    val inconsistency = IgnoredMutation("Q1", eventTime, revision, inputEvent, ingestionTime, NewerRevisionSeen, state)
    val record = kafkaSerSchema.serialize(inconsistency, Instant.now().toEpochMilli)
    record.timestamp() shouldBe processingTime.toEpochMilli
    record.topic() shouldBe "my_topic"
    val node = jsonLoader.parse(new String(record.value(), "UTF-8"))
    node.get("$schema").asText() shouldBe JsonEncoders.stateInconsistencySchema
    node.get("meta").get("stream").asText() shouldBe JsonEncoders.stateInconsistencyStream
  }

  "JsonEncoders" should "provide a KafkaSerializationSchema that fails on an unsupported type" in {
    val kafkaSerSchema = new SideOutputSerializationSchema[String](Some(clock), "my_topic",
      JsonEncoders.stateInconsistencyStream, JsonEncoders.stateInconsistencySchema, sideOutputDomain, eventStreamConfigEndpoint, schemaRepos, httpClientConfig)
    assertThrows[IllegalArgumentException] { kafkaSerSchema.serialize("boom", Instant.now().toEpochMilli) }
  }

  private def assertBasicMetadata(record: util.Map[String, Object]): Unit = {
    record.get("item") shouldEqual item
    record.get("original_ingestion_dt") shouldEqual ingestionTime.toString
    record.get("revision_id").asInstanceOf[Number].longValue() shouldEqual revision.longValue()
    assertOriginalInfo(record.get("original_event_info"))
  }

  def assertNewMetadata(v: Any, newStream: String): Unit = {
    v match {
      case value: util.Map[_, _] =>
        value.get("id") shouldBe uniqueId
        value.get("dt") shouldBe processingTime.toString
        value.get("stream") shouldBe newStream
        value.get("domain") shouldBe sideOutputDomain
      case _ => fail("Unexpected type " + v.getClass)
    }
  }

  def assertOriginalInfo(v: Any): Unit = {
    v match {
      case value: util.Map[_, _] =>
        value.get("$schema") shouldBe "schema"
        value.get("dt") shouldBe eventTime.toString
        assertOriginalMeta(value.get("meta"))
      case _ => fail("Unexpected type " + v.getClass)
    }
  }

  def assertOriginalMeta(v: Any): Unit = {
    v match {
      case value: util.Map[_, _] =>
        value.get("id") shouldBe uuid
        value.get("dt") shouldBe eventTime.toString
        value.get("stream") shouldBe stream
        value.get("request_id") shouldBe requestId
        value.get("domain") shouldBe domain
      case _ => fail("Unexpected type " + v.getClass)
    }
  }
}
