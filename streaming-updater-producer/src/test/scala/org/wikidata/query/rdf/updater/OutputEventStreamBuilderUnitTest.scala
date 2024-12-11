package org.wikidata.query.rdf.updater

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema.KafkaSinkContext
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.types.Row
import org.apache.flink.util.InstantiationUtil
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FlatSpec, Matchers}
import org.wikidata.query.rdf.tool.change.events.{EventInfo, EventsMeta}
import org.wikidata.query.rdf.tool.exception.ContainedException
import org.wikidata.query.rdf.tool.wikibase.WikibaseEntityFetchException.Type
import org.wikidata.query.rdf.updater.config.{HttpClientConfig, UpdaterPipelineOutputStreamConfig}
import org.wikimedia.eventutilities.core.SerializableClock
import org.wikimedia.eventutilities.core.event.JsonEventGenerator
import org.wikimedia.eventutilities.flink.formats.json.JsonRowSerializationSchema

import java.time.{Clock, Duration, Instant, ZoneOffset}
import java.util.UUID

private object OutputEventStreamsBuilderTestFixtures extends Matchers {
  val domain: String = "tested.domain"
  val emitterId = "my_emitter_id"
  private val item: String = "Q1"
  val uniqueId: UUID = UUID.randomUUID()
  val revision: Long = 123
  private val fromRevision: Long = 122
  private val eventTime: Instant = Instant.now().minus(Duration.ofHours(2))
  val ingestionTime: Instant = Instant.now().minus(Duration.ofHours(1))
  private val stream: String = "tested.stream"
  private val uuid: String = UUID.randomUUID().toString
  private val requestId: String = UUID.randomUUID().toString
  private val sideOutputDomain = "sideOutputDomain"
  private val eventInfo: EventInfo = new EventInfo(new EventsMeta(eventTime, uuid, domain, stream, requestId), "schema")
  val eventPlatformFactory = new EventPlatformFactory(this.getClass.getResource("/OutputEventStreamsBuilderUnitTest-stream-config.json").toString,
    List("https://schema.wikimedia.org/repositories/primary/jsonschema",
      "https://schema.wikimedia.org/repositories/secondary/jsonschema",
      this.getClass.getResource("/schema_repo").toString
    ),
    HttpClientConfig(None, None, "ua"),
    Clock.systemUTC()
  )

  val jsonEventGenerator: JsonEventGenerator = JsonEventGenerator.builder()
    .eventStreamConfig(OutputEventStreamsBuilderTestFixtures.eventPlatformFactory.eventStreamConfig)
    .ingestionTimeClock(SerializableClock.frozenClock(OutputEventStreamsBuilderTestFixtures.ingestionTime))
    .withUuidSupplier(() => OutputEventStreamsBuilderTestFixtures.uniqueId)
    .schemaLoader(OutputEventStreamsBuilderTestFixtures.eventPlatformFactory.eventSchemaLoader)
    .build()

  val config = UpdaterPipelineOutputStreamConfig(
    kafkaBrokers = "somebroker",
    topic = "main-output.topic",
    partition = 0,
    checkpointingMode = CheckpointingMode.EXACTLY_ONCE,
    outputTopicPrefix = Some("datacenter."),
    sideOutputsDomain = sideOutputDomain,
    sideOutputsKafkaBrokers = None,
    ignoreFailuresAfterTransactionTimeout = false,
    produceSideOutputs = true,
    emitterId = Some(emitterId),
    subgraphKafkaTopics = Map.empty,
    producerProperties = Map.empty,
    mainStream = "main-stream",
    useEventStreamsApi = true,
    streamVersionSuffix = Some("v2")
  )

  val revCreateEvent: RevCreate = RevCreate(item, eventTime, revision, Some(revision - 1), ingestionTime, eventInfo)
  val revCreateEventNoParen: RevCreate = RevCreate(item, eventTime, revision, None, ingestionTime, eventInfo)
  val pageDeleteEvent: PageDelete = PageDelete(item, eventTime, revision, ingestionTime, eventInfo)
  val pageUnDeleteEvent: PageUndelete = PageUndelete(item, eventTime, revision, ingestionTime, eventInfo)
  val ignoredMutation: IgnoredMutation = IgnoredMutation(
    "Q1",
    eventTime,
    revision,
    revCreateEvent,
    ingestionTime,
    NewerRevisionSeen,
    State(Some(revision),
      EntityStatus.CREATED)
  )
  val problematicReconciliation: ProblematicReconciliation = ProblematicReconciliation(
    "Q1",
    eventTime,
    revision,
    ReconcileInputEvent(item, eventTime, revision, ReconcileCreation, ingestionTime, eventInfo),
    ingestionTime,
    ReconcileAmbiguousCreation,
    State(Some(revision), EntityStatus.DELETED),
    Reconcile("Q1", eventTime, revision, ingestionTime, eventInfo))
  val failedOp: FailedOp = FailedOp(Diff(item, eventTime, revision, fromRevision, ingestionTime, eventInfo), new ContainedException("problem"))
  private val objectMapper = new ObjectMapper()


  def assertBasicMetadata(record: Row): Unit = {
    record.getFieldAs[String]("item") shouldEqual item
    record.getFieldAs[Instant]("original_ingestion_dt") shouldEqual ingestionTime
    record.getFieldAs[Long]("revision_id").asInstanceOf[Number].longValue() shouldEqual revision.longValue()
    record.getFieldAs[String]("emitter_id") shouldEqual emitterId
    assertOriginalInfo(record.getFieldAs[Row]("original_event_info"))
  }

  def assertNewMetadata(value: Row, newStream: String): Unit = {
    Option(value.getFieldAs[String]("id")) shouldBe None // set via event utilities
    Option(value.getFieldAs[Instant]("dt")) shouldBe None // set via event utilities
    value.getFieldAs[String]("stream") shouldBe newStream
    value.getFieldAs[String]("domain") shouldBe sideOutputDomain
  }

  def assertOriginalInfo(value: Row): Unit = {
    value.getFieldAs[String]("$schema") shouldBe "schema"
    value.getFieldAs[Instant]("dt") shouldBe eventTime
    assertOriginalMeta(value.getFieldAs[Row]("meta"))
  }

  def assertOriginalMeta(value: Row): Unit = {
    value.getFieldAs[String]("id") shouldBe uuid
    value.getFieldAs[Instant]("dt") shouldBe eventTime
    value.getFieldAs[String]("stream") shouldBe stream
    value.getFieldAs[String]("request_id") shouldBe requestId
    value.getFieldAs[String]("domain") shouldBe domain
  }

  def assertEventStreamPlatformFeatures(jsonBytes: Array[Byte], schema: String): Unit = {
    val node: JsonNode = objectMapper.createParser(jsonBytes).readValueAsTree()
    node.get("meta").get("dt").asText() shouldBe OutputEventStreamsBuilderTestFixtures.ingestionTime.toString
    node.get("meta").get("id").asText() shouldBe OutputEventStreamsBuilderTestFixtures.uniqueId.toString
    node.get("$schema").asText() shouldBe schema
  }
}

class OutputEventStreamBuilderUnitTest extends FlatSpec with Matchers {
  "OutputEventStreamsBuilder" should "create a output streams without side-outputs if not required" in {

    val config = UpdaterPipelineOutputStreamConfig(
      kafkaBrokers = "somebroker",
      topic = "dc1.mutation",
      partition = 0,
      checkpointingMode = CheckpointingMode.EXACTLY_ONCE,
      outputTopicPrefix = Some("dc1."),
      sideOutputsDomain = "my-domain",
      sideOutputsKafkaBrokers = None,
      ignoreFailuresAfterTransactionTimeout = false,
      produceSideOutputs = false,
      emitterId = None,
      subgraphKafkaTopics = Map.empty,
      producerProperties = Map.empty,
      mainStream = "rdf-streaming-updater.mutation",
      useEventStreamsApi = true,
      streamVersionSuffix = Some("v2")
    )
    val outputStreams = new OutputEventStreamsBuilder(config,
      OutputEventStreamsBuilderTestFixtures.eventPlatformFactory, 1).build
    outputStreams.lateEventsSink shouldBe None
    outputStreams.failedOpsSink shouldBe None
    outputStreams.spuriousEventsSink shouldBe None
    outputStreams.mutationSink should not be None
  }

  "OutputEventStreamsBuilder" should "create a output streams with side-outputs if required" in {

    val config = UpdaterPipelineOutputStreamConfig(
      kafkaBrokers = "somebroker",
      topic = "dc1.mutation",
      partition = 0,
      checkpointingMode = CheckpointingMode.EXACTLY_ONCE,
      outputTopicPrefix = Some("dc1."),
      sideOutputsDomain = "my-domain",
      sideOutputsKafkaBrokers = None,
      ignoreFailuresAfterTransactionTimeout = false,
      produceSideOutputs = true,
      emitterId = Some("emitter"),
      subgraphKafkaTopics = Map.empty,
      producerProperties = Map.empty,
      mainStream = "rdf-streaming-updater.mutation",
      streamVersionSuffix = Some("v2")
    )
    val outputStreams = new OutputEventStreamsBuilder(config,
      OutputEventStreamsBuilderTestFixtures.eventPlatformFactory, 1).build
    outputStreams.lateEventsSink should not be None
    outputStreams.failedOpsSink should not be None
    outputStreams.spuriousEventsSink should not be None
    outputStreams.mutationSink should not be None
  }

  "OutputEventStreamsBuilder" should "should fail if the subgraphs setup is incorrect" in {

    val config = UpdaterPipelineOutputStreamConfig(
      kafkaBrokers = "somebroker",
      topic = "dc1.mutation",
      partition = 0,
      checkpointingMode = CheckpointingMode.EXACTLY_ONCE,
      outputTopicPrefix = Some("datacenter."),
      sideOutputsDomain = "my-domain",
      sideOutputsKafkaBrokers = None,
      ignoreFailuresAfterTransactionTimeout = false,
      produceSideOutputs = false,
      emitterId = None,
      subgraphKafkaTopics = Map(
        "unrelated" -> "dc1.mutation-subgraph-1",
        "rdf-streaming-updater.mutation-subgraph-2" -> "dc1.mutation-subgraph-bad-2"
      ),
      producerProperties = Map.empty,
      mainStream = "rdf-streaming-updater.mutation",
      useEventStreamsApi = true,
      streamVersionSuffix = Some("v2")
    )
    val exc = intercept[IllegalArgumentException] {
      new OutputEventStreamsBuilder(config,
        OutputEventStreamsBuilderTestFixtures.eventPlatformFactory, 1).build
    }
    exc.getMessage shouldBe "The following streams " +
      "unrelated.v2" +
      " are not compatible with mediawiki/wikibase/entity/rdf_change"
  }

  "OutputEventStreamsBuilder" should "should fail if the subgraphs streams have topics that are not allowed" in {

    val config = UpdaterPipelineOutputStreamConfig(
      kafkaBrokers = "somebroker",
      topic = "dc1.mutation",
      partition = 0,
      checkpointingMode = CheckpointingMode.EXACTLY_ONCE,
      outputTopicPrefix = Some("datacenter."),
      sideOutputsDomain = "my-domain",
      sideOutputsKafkaBrokers = None,
      ignoreFailuresAfterTransactionTimeout = false,
      produceSideOutputs = false,
      emitterId = None,
      subgraphKafkaTopics = Map(
        "rdf-streaming-updater.mutation-subgraph-1" -> "dc1.mutation-subgraph-1",
        "rdf-streaming-updater.mutation-subgraph-2" -> "dc1.mutation-subgraph-bad-2"
      ),
      producerProperties = Map.empty,
      mainStream = "rdf-streaming-updater.mutation",
      useEventStreamsApi = true,
      streamVersionSuffix = Some("v2")
    )
    val exc = intercept[IllegalArgumentException] {
      new OutputEventStreamsBuilder(config,
        OutputEventStreamsBuilderTestFixtures.eventPlatformFactory, 1).build
    }
    exc.getMessage shouldBe "The following stream to topic pairs " +
      "rdf-streaming-updater.mutation-subgraph-2 -> dc1.mutation-subgraph-bad-2" +
      " are forbidden"
  }
}

class MultiStreamKafkaRecordSerializerUnitTest extends FlatSpec with Matchers with MockFactory {
  private val outputRowTypeInfo = OutputEventStreamsBuilderTestFixtures
    .eventPlatformFactory
    .eventDataStreamFactory
    .rowTypeInfo("rdf-streaming-updater.mutation.v2", "2.0.0")

  "MultiStreamKafkaRecordSerializer" should "write to different topic based on meta.stream" in {
    val streamToTopic = Map("stream1" -> "topic1", "stream2" -> "topic2")
    val context = mock[KafkaSinkContext]
    val initContext = mock[SerializationSchema.InitializationContext]
    (context.getPartitionsForTopic _).expects("topic1").returning(Array(0))
    (context.getPartitionsForTopic _).expects("topic2").returning(Array(0))
    // n topics, n calls to the partitioner on open
    (context.getParallelInstanceId _).expects().repeat(streamToTopic.size).returning(1)
    (context.getNumberOfParallelInstances _).expects().repeat(streamToTopic.size).returning(2)

    val serializer = JsonRowSerializationSchema.builder()
      .withoutNormalization()
      .withTypeInfo(outputRowTypeInfo)
      .build()
    val partition = 1
    val kafkaTimestamp = Instant.now()
    val clock = Clock.fixed(kafkaTimestamp, ZoneOffset.UTC)
    val multiSer = MultiStreamKafkaRecordSerializer(streamToTopic, partition, serializer,
      outputRowTypeInfo, clock)
    InstantiationUtil.serializeObject(multiSer).length should not be 0
    multiSer.open(initContext, context)
    val row = outputRowTypeInfo.createEmptyRow()
    val meta = outputRowTypeInfo.createEmptySubRow("meta")
    row.setField("meta", meta)

    streamToTopic.foreach { case (stream, excpectedTopic) =>
      meta.setField("stream", stream)
      val record = multiSer.serialize(row, context, 123L)
      record.topic() shouldBe excpectedTopic
      record.partition() shouldBe 1
      record.timestamp() shouldBe kafkaTimestamp.toEpochMilli
    }
    assertThrows[IllegalArgumentException] {
      meta.setField("stream", "unknown")
      multiSer.serialize(row, context, 123L)
    }
  }
}

class MutationEventDataToRowUnitTest extends FlatSpec with Matchers {
  private val outputRowTypeInfo = OutputEventStreamsBuilderTestFixtures
    .eventPlatformFactory
    .eventDataStreamFactory
    .rowTypeInfo("rdf-streaming-updater.mutation.v2", "2.0.0")

  "MutationEventDataToRow" should "convert a MutationEventData delete event into a row" in {
    val inputEventMeta = new EventsMeta(Instant.now(), "my-orig-id", "my-domain", "my-orig-stream", "my-request-id");
    val input = MutationEventDataFactory.v2().getMutationBuilder
      .buildMutation(new EventsMeta(Instant.now(), "my-id", "my-domain", "my-stream", "my-request-id"),
         "Q123", 123, Instant.EPOCH, 0, 1, MutationEventData.DELETE_OPERATION)
    val deleteOp = DeleteItem("Q123", Instant.EPOCH, 123L, Instant.now(), new EventInfo(inputEventMeta, "my-input-event-schema"))
    val output = outputRowTypeInfo.createEmptyRow()

    val meta = outputRowTypeInfo.createEmptySubRow("meta")
    meta.setField("domain", "my-domain")
    meta.setField("request_id", "my-request-id")
    meta.setField("stream", "my-stream.v2")
    output.setField("meta", meta)
    output.setField("entity_id", "Q123")
    output.setField("rev_id", 123L)
    output.setField("operation", "delete")
    output.setField("dt", Instant.EPOCH)
    output.setField("sequence", 0)
    output.setField("sequence_length", 1)

    val mapper = new MutationDataChunkToRow(outputRowTypeInfo, Map("my-stream" -> "my-stream.v2"))
    mapper.map(MutationDataChunk(deleteOp, input)) should equal(output)
  }

  "MutationEventDataToRow" should "convert a DiffEventData event into a row" in {
    val inputEventMeta = new EventsMeta(Instant.now(), "my-orig-id", "my-domain", "my-orig-stream", "my-request-id");
    val input = MutationEventDataFactory.v2().getDiffBuilder
      .buildDiff(new EventsMeta(Instant.now(), "my-id", "my-domain", "my-stream", "my-request-id"),
         "Q123", 123, Instant.EPOCH, 0, 1, MutationEventData.DIFF_OPERATION,
      new RDFDataChunk("added-data", "my-mime"),
      new RDFDataChunk("deleted-data", "my-mime"),
      new RDFDataChunk("linked-shared-data", "my-mime"),
      new RDFDataChunk("unlinked-shared-data", "my-mime"))
    val operation = Diff("Q123", Instant.EPOCH, 123L, 122L, Instant.now(), new EventInfo(inputEventMeta, "my-input-event-schema"))

    val output = outputRowTypeInfo.createEmptyRow()

    val meta = outputRowTypeInfo.createEmptySubRow("meta")
    meta.setField("domain", "my-domain")
    meta.setField("request_id", "my-request-id")
    meta.setField("stream", "my-stream.v2")
    output.setField("meta", meta)
    output.setField("entity_id", "Q123")
    output.setField("rev_id", 123L)
    output.setField("operation", "diff")
    output.setField("dt", Instant.EPOCH)
    output.setField("sequence", 0)
    output.setField("sequence_length", 1)

    def dataChunk(data: String): Row = {
      val chunkRow = outputRowTypeInfo.createEmptySubRow("rdf_added_data")
      chunkRow.setField("mime_type", "my-mime")
      chunkRow.setField("data", data)
      chunkRow
    }
    output.setField("rdf_added_data", dataChunk("added-data"))
    output.setField("rdf_deleted_data", dataChunk("deleted-data"))
    output.setField("rdf_linked_shared_data", dataChunk("linked-shared-data"))
    output.setField("rdf_unlinked_shared_data", dataChunk("unlinked-shared-data"))

    val mapper = new MutationDataChunkToRow(outputRowTypeInfo, Map("my-stream" -> "my-stream.v2"))
    mapper.map(MutationDataChunk(operation, input)) should equal(output)
  }
}

class LapsedActionToRowUnitTest extends FlatSpec with Matchers {
  private val builder = new OutputEventStreamsBuilder(OutputEventStreamsBuilderTestFixtures.config,
    OutputEventStreamsBuilderTestFixtures.eventPlatformFactory, 1)
  private val wrapper = builder.lapsedActionOutputStream
  private val mapper = wrapper.mapFunction
  private val streamName = "rdf-streaming-updater.lapsed-action"
  private val eventStream = OutputEventStreamsBuilderTestFixtures.eventPlatformFactory.eventStreamFactory.createEventStream(streamName)
  private val schemaVersion = "1.1.0"
  private val serializer = JsonRowSerializationSchema.builder()
    .withNormalizationFunction(
      OutputEventStreamsBuilderTestFixtures.jsonEventGenerator
        .createEventStreamEventGenerator(streamName, eventStream.schemaUri(schemaVersion).toString))
    .withTypeInfo(OutputEventStreamsBuilderTestFixtures.eventPlatformFactory.eventDataStreamFactory.rowTypeInfo(streamName, schemaVersion))
    .build()

  private val expectedSchema = "/rdf_streaming_updater/lapsed_action/" + schemaVersion
  "LapsedActionToRow" should "convert a RevCreateEvent with a parent revision into a row" in {
    val output = mapper.map(OutputEventStreamsBuilderTestFixtures.revCreateEvent)
    OutputEventStreamsBuilderTestFixtures.assertBasicMetadata(output)
    OutputEventStreamsBuilderTestFixtures.assertNewMetadata(output.getFieldAs[Row]("meta"), streamName)
    output.getFieldAs[String]("action_type") shouldBe "revision-create"
    output.getFieldAs[Long]("parent_revision_id").asInstanceOf[Number].longValue() shouldBe OutputEventStreamsBuilderTestFixtures.revision-1
    OutputEventStreamsBuilderTestFixtures.assertEventStreamPlatformFeatures(serializer.serialize(output), expectedSchema)
  }

  "LapsedActionToRow" should "convert a RevCreateEvent without a parent revision into a row" in {
    val output = mapper.map(OutputEventStreamsBuilderTestFixtures.revCreateEventNoParen)
    OutputEventStreamsBuilderTestFixtures.assertBasicMetadata(output)
    OutputEventStreamsBuilderTestFixtures.assertNewMetadata(output.getFieldAs[Row]("meta"), streamName)
    output.getFieldAs[String]("action_type") shouldBe "revision-create"
    Option(output.getFieldAs[Long]("parent_revision_id")) shouldBe None
    OutputEventStreamsBuilderTestFixtures.assertEventStreamPlatformFeatures(serializer.serialize(output), expectedSchema)
  }

  "LapsedActionToRow" should "convert a PageDeleteEvent into a row" in {
    val output = mapper.map(OutputEventStreamsBuilderTestFixtures.pageDeleteEvent)
    OutputEventStreamsBuilderTestFixtures.assertBasicMetadata(output)
    OutputEventStreamsBuilderTestFixtures.assertNewMetadata(output.getFieldAs[Row]("meta"), streamName)
    output.getFieldAs[String]("action_type") shouldBe "page-delete"
    Option(output.getFieldAs[Long]("parent_revision_id")) shouldBe None
    OutputEventStreamsBuilderTestFixtures.assertEventStreamPlatformFeatures(serializer.serialize(output), expectedSchema)
  }

  "PageUndelete" should "convert a PageUndelete event into a row" in {
    val output = mapper.map(OutputEventStreamsBuilderTestFixtures.pageUnDeleteEvent)
    OutputEventStreamsBuilderTestFixtures.assertBasicMetadata(output)
    OutputEventStreamsBuilderTestFixtures.assertNewMetadata(output.getFieldAs[Row]("meta"), streamName)
    output.getFieldAs[String]("action_type") shouldBe "page-undelete"
    Option(output.getFieldAs[Long]("parent_revision_id")) shouldBe None
    OutputEventStreamsBuilderTestFixtures.assertEventStreamPlatformFeatures(serializer.serialize(output), expectedSchema)
  }
}

class InconsistencyToRowUnitUnitTest extends FlatSpec with Matchers {
  private val builder = new OutputEventStreamsBuilder(OutputEventStreamsBuilderTestFixtures.config,
    OutputEventStreamsBuilderTestFixtures.eventPlatformFactory, 1)
  private val wrapper = builder.spuriousEventsOutputStream
  private val mapper = wrapper.mapFunction
  private val streamName = "rdf-streaming-updater.state-inconsistency"
  private val eventStream = OutputEventStreamsBuilderTestFixtures.eventPlatformFactory.eventStreamFactory.createEventStream(streamName)
  private val schemaVersion = "1.1.0"
  private val serializer = JsonRowSerializationSchema.builder()
    .withNormalizationFunction(
      OutputEventStreamsBuilderTestFixtures.jsonEventGenerator
        .createEventStreamEventGenerator(streamName, eventStream.schemaUri(schemaVersion).toString))
    .withTypeInfo(OutputEventStreamsBuilderTestFixtures.eventPlatformFactory.eventDataStreamFactory.rowTypeInfo(streamName, schemaVersion))
    .build()

  private val expectedSchema = "/rdf_streaming_updater/state_inconsistency/" + schemaVersion
  "InconsistencyToRow" should "convert a IgnoredMutation into a row" in {
    val output = mapper.map(OutputEventStreamsBuilderTestFixtures.ignoredMutation)
    OutputEventStreamsBuilderTestFixtures.assertBasicMetadata(output)
    OutputEventStreamsBuilderTestFixtures.assertNewMetadata(output.getFieldAs[Row]("meta"), streamName)

    output.getFieldAs[String]("inconsistency") shouldBe NewerRevisionSeen.name
    output.getFieldAs[Long]("state_revision_id").asInstanceOf[Number].longValue() shouldBe OutputEventStreamsBuilderTestFixtures.revision
    output.getFieldAs[String]("state_status") shouldBe "CREATED"
    output.getFieldAs[String]("action_type") shouldBe "revision-create"
    Option(output.getFieldAs[Long]("parent_revision_id"))
      .map(_.asInstanceOf[Number].longValue()) shouldBe Some(OutputEventStreamsBuilderTestFixtures.revision-1)
    OutputEventStreamsBuilderTestFixtures.assertEventStreamPlatformFeatures(serializer.serialize(output), expectedSchema)
  }

  "InconsistencyToRow" should "convert a ProblematicReconciliation into a row" in {
    val output = mapper.map(OutputEventStreamsBuilderTestFixtures.problematicReconciliation)
    OutputEventStreamsBuilderTestFixtures.assertBasicMetadata(output)
    OutputEventStreamsBuilderTestFixtures.assertNewMetadata(output.getFieldAs[Row]("meta"), streamName)

    output.getFieldAs[String]("inconsistency") shouldBe ReconcileAmbiguousCreation.name
    output.getFieldAs[Long]("state_revision_id").asInstanceOf[Number].longValue() shouldBe OutputEventStreamsBuilderTestFixtures.revision
    output.getFieldAs[String]("state_status") shouldBe "DELETED"
    output.getFieldAs[String]("action_type") shouldBe "reconcile-creation"
    OutputEventStreamsBuilderTestFixtures.assertEventStreamPlatformFeatures(serializer.serialize(output), expectedSchema)
  }
}

class FailedOpToRowUnitTest extends FlatSpec with Matchers {
  private val builder = new OutputEventStreamsBuilder(OutputEventStreamsBuilderTestFixtures.config,
    OutputEventStreamsBuilderTestFixtures.eventPlatformFactory, 1)
  private val wrapper = builder.failedEventOutputStream
  private val mapper = wrapper.mapFunction
  private val streamName = "rdf-streaming-updater.fetch-failure"
  private val eventStream = OutputEventStreamsBuilderTestFixtures.eventPlatformFactory.eventStreamFactory.createEventStream(streamName)
  private val schemaVersion = "1.2.0"
  private val expectedSchema = "/rdf_streaming_updater/fetch_failure/" + schemaVersion
  private val serializer = JsonRowSerializationSchema.builder()
    .withNormalizationFunction(
      OutputEventStreamsBuilderTestFixtures.jsonEventGenerator
        .createEventStreamEventGenerator(streamName, eventStream.schemaUri(schemaVersion).toString))
    .withTypeInfo(OutputEventStreamsBuilderTestFixtures.eventPlatformFactory.eventDataStreamFactory.rowTypeInfo(streamName, schemaVersion))
    .build()

  "FailedOpToRow" should "convert a FailedOp into a row" in {

    val output = mapper.map(OutputEventStreamsBuilderTestFixtures.failedOp)
    OutputEventStreamsBuilderTestFixtures.assertBasicMetadata(output)
    OutputEventStreamsBuilderTestFixtures.assertNewMetadata(output.getFieldAs[Row]("meta"), streamName)
    output.getFieldAs[String]("exception_type") shouldBe OutputEventStreamsBuilderTestFixtures.failedOp.exception.getClass.getName
    output.getFieldAs[String]("exception_msg") shouldBe OutputEventStreamsBuilderTestFixtures.failedOp.exception.getMessage
    output.getFieldAs[String]("fetch_error_type") shouldBe Type.UNKNOWN.toString
    output.getFieldAs[String]("op_type") shouldBe "diff"
    Option(output.getFieldAs[Long]("from_revision_id")).map(_.asInstanceOf[Number].longValue()) shouldBe
      Some(OutputEventStreamsBuilderTestFixtures.revision - 1)
    OutputEventStreamsBuilderTestFixtures.assertEventStreamPlatformFeatures(serializer.serialize(output), expectedSchema)
  }
}

