package org.wikidata.query.rdf.updater

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala._
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.wikidata.query.rdf.common.uri.UrisConstants
import org.wikidata.query.rdf.tool.EntityId.cleanEntityId
import org.wikidata.query.rdf.tool.change.events.ReconcileEvent.Action.{CREATION, DELETION}
import org.wikidata.query.rdf.tool.change.events._
import org.wikidata.query.rdf.tool.wikibase.WikibaseRepository.Uris
import org.wikidata.query.rdf.updater.config.{FilteredReconciliationTopic, UpdaterPipelineInputEventStreamConfig}

import java.time.{Clock, Duration}

// FIXME rework parts this class do limit duplication
object IncomingStreams {
  // Our kafka topics have a single partition, force a parallelism of 1 for the input
  // so that we do not create useless kafka consumer but also only shuffle once we key
  // the stream.
  private val INPUT_PARALLELISM = 1

  type EntityResolver = (Long, String, Long) => String
  type Converter[E] = (E, EntityResolver, Clock) => InputEvent

  val REV_CREATE_CONV: Converter[RevisionCreateEvent] =
    (e, resolver, clock) => RevCreate(resolver(e.namespace(), e.title(), e.pageId()), e.timestamp(), e.revision(),
      Option(e.parentRevision(): java.lang.Long).map(_.toLong), clock.instant(), e.eventInfo())

  val PAGE_DEL_CONV: Converter[PageDeleteEvent] =
    (e, resolver, clock) => PageDelete(resolver(e.namespace(), e.title(), e.pageId()), e.timestamp(), e.revision(), clock.instant(), e.eventInfo())

  val PAGE_UNDEL_CONV: Converter[PageUndeleteEvent] =
    (e, resolver, clock) => PageUndelete(resolver(e.namespace(), e.title(), e.pageId()), e.timestamp(), e.revision(), clock.instant(), e.eventInfo())

  val RECONCILIATION_CONV: Converter[ReconcileEvent] =
    (e, resolver, clock) =>
      ReconcileInputEvent(
        e.getItem.toString,
        e.getEventInfo.meta().timestamp(),
        e.getRevision,
        e.getReconciliationAction match {
          case CREATION => ReconcileCreation
          case DELETION => ReconcileDeletion
        },
        clock.instant(),
        e.getEventInfo
      )

  def buildIncomingStreams(ievops: UpdaterPipelineInputEventStreamConfig,
                           uris: Uris, clock: Clock)
                          (implicit env: StreamExecutionEnvironment): List[DataStream[InputEvent]] = {

    val resolver: EntityResolver = (ns, title, pageId) => if (ievops.mediaInfoEntityNamespaces.contains(ns)) {
      UrisConstants.MEDIAINFO_INITIAL + pageId
    } else {
      cleanEntityId(title)
    }

    def build[E <: EventPlatformEvent](topic: String, clazz: Class[E], conv: Converter[E], filter: Option[FilterFunction[E]] = None): DataStream[InputEvent] = {
      val consumerProps = KafkaConsumerProperties(topic, ievops.kafkaBrokers, ievops.consumerGroup,
        DeserializationSchemaFactory.getDeserializationSchema(clazz), ievops.consumerProperties)
      fromKafka(consumerProps, uris, conv, ievops.maxLateness, ievops.idleness, clock, resolver, filter)
    }

    val revisionCreateEventFilter = new RevisionCreateEventFilter(ievops.mediaInfoEntityNamespaces, ievops.mediaInfoRevisionSlot)
    val inputKafkaTopics = ievops.inputStreams match {
      case Left(inputKafkaTopics) => inputKafkaTopics
      case Right(_) => throw new IllegalArgumentException("InputKafkaTopics expected")
    }
    inputKafkaTopics.topicPrefixes.flatMap(prefix => {
      List(
        build(prefix + inputKafkaTopics.revisionCreateTopicName, classOf[RevisionCreateEvent], REV_CREATE_CONV, Some(revisionCreateEventFilter)),
        build(prefix + inputKafkaTopics.pageDeleteTopicName, classOf[PageDeleteEvent], PAGE_DEL_CONV),
        build(prefix + inputKafkaTopics.pageUndeleteTopicName, classOf[PageUndeleteEvent], PAGE_UNDEL_CONV),
        build(prefix + inputKafkaTopics.suppressedDeleteTopicName, classOf[PageDeleteEvent], PAGE_DEL_CONV)
      ) ++: inputKafkaTopics.reconciliationTopicName.map {
        case FilteredReconciliationTopic(topic, filter) =>
          build(prefix + topic, classOf[ReconcileEvent], RECONCILIATION_CONV, filter.map(new ReconciliationSourceFilter(_)))
      }
    })
  }

  def fromKafka[E <: EventPlatformEvent](kafkaProps: KafkaConsumerProperties[E], uris: Uris, // scalastyle:ignore
                                         conv: Converter[E],
                                         maxLatenessMs: Int, idlenessMs: Int,
                                         clock: Clock,
                                         resolver: EntityResolver,
                                         filter: Option[FilterFunction[E]])
                                 (implicit env: StreamExecutionEnvironment): DataStream[InputEvent] = {
    val kafkaStream = withKafkaSource(kafkaProps, maxLatenessMs, idlenessMs)
    fromStream(kafkaStream, uris, conv, clock, resolver, filter)
  }

  private def withKafkaSource[E <: EventPlatformEvent](kafkaProps: KafkaConsumerProperties[E],
                                                       maxLatenessMs: Int,
                                                       idlenessMs: Int)
                                                      (implicit env: StreamExecutionEnvironment): DataStream[E] = {
    val nameAndUid = operatorUUID(kafkaProps.topic, Some("KafkaSource"));
    val kafkaSource: KafkaSource[E] = KafkaSource.builder[E]()
      .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST)) // latest if no group offsets are available
      .setProperties(kafkaProps.asProperties())
      .setClientIdPrefix(kafkaProps.consumerGroup + ":" + nameAndUid)
      .setTopics(kafkaProps.topic)
      .setValueOnlyDeserializer(kafkaProps.schema)
      .build();

    env.fromSource(kafkaSource, watermarkStrategy[E](maxLatenessMs, idlenessMs), nameAndUid)(kafkaSource.getProducedType)
      .uid(nameAndUid)
      .name(nameAndUid)
      .setParallelism(INPUT_PARALLELISM)
  }

  def operatorUUID[E <: EventPlatformEvent](topic: String, prefix: Option[String] = None): String = {
    prefix.fold(topic)(p => f"$p:$topic")
  }

  private def watermarkStrategy[E <: EventPlatformEvent](maxLatenessMs: Int, idlenessMs: Int): WatermarkStrategy[E] = {
    WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMillis(maxLatenessMs))
      .withIdleness(Duration.ofMillis(idlenessMs))
      .withTimestampAssigner(new SerializableTimestampAssigner[E] {
        override def extractTimestamp(element: E, recordTimestamp: Long): Long = element.meta().timestamp().toEpochMilli
      })
  }

  def fromStream[E <: EventPlatformEvent](stream: DataStream[E],
                                   uris: Uris,
                                   conv: Converter[E],
                                   clock: Clock,
                                   resolver: EntityResolver,
                                   filter: Option[FilterFunction[E]]
                                  )(implicit env: StreamExecutionEnvironment): DataStream[InputEvent] = {
    val initiallyFilteredStream = stream.filter(new EventWithMetadataHostFilter[E](uris)).name("Host&Namespace Filter")

    // force parallelism to one (mostly for unit tests here so that we don't mess-up their ordering)
    // filtering is also very simple
    initiallyFilteredStream.setParallelism(INPUT_PARALLELISM)
    val filteredStream =
      filter.map(f => {
        initiallyFilteredStream
          .filter(f)
          .setParallelism(INPUT_PARALLELISM)
          .name("MCR mediainfo slot filter")
      }).getOrElse(initiallyFilteredStream)

    val convertedStream = filteredStream
      .map(conv(_, resolver, clock))
      .name(s"Filtered(${stream.name} == ${uris.getHost})")
    // for parallelism to 1 again for the same reasons
    convertedStream.setParallelism(INPUT_PARALLELISM)
    convertedStream
  }
}

class EventWithMetadataHostFilter[E <: EventPlatformEvent](uris: Uris) extends FilterFunction[E] {
  override def filter(e: E): Boolean = {
    e match {
      case c: ChangeEvent =>
        uris.getHost.equals(c.domain()) && uris.isEntityNamespace(c.namespace())
      case r: ReconcileEvent =>
        uris.getHost.equals(r.meta().domain())
    }
  }
}

class RevisionCreateEventFilter(mediaInfoEntityNamespaces: Set[Long], mediaInfoRevSlot: String) extends FilterFunction[RevisionCreateEvent] {
  override def filter(value: RevisionCreateEvent): Boolean = {
    if (mediaInfoEntityNamespaces.contains(value.namespace())) {
      value.revSlots() != null && value.revSlots().containsKey(mediaInfoRevSlot)
    } else {
      true
    }
  }
}
class ReconciliationSourceFilter(source: String) extends FilterFunction[ReconcileEvent] {
  override def filter(value: ReconcileEvent): Boolean = {
    source == value.getReconciliationSource
  }
}
