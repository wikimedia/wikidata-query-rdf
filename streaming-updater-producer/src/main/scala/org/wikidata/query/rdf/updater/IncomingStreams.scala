package org.wikidata.query.rdf.updater

import java.time.{Clock, Duration}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.wikidata.query.rdf.common.uri.UrisConstants
import org.wikidata.query.rdf.tool.EntityId
import org.wikidata.query.rdf.tool.change.events.{ChangeEvent, EventPlatformEvent, PageDeleteEvent, PageUndeleteEvent, ReconcileEvent, RevisionCreateEvent}
import org.wikidata.query.rdf.tool.change.events.ReconcileEvent.Action.{CREATION, DELETION}
import EntityId.cleanEntityId
import org.wikidata.query.rdf.tool.wikibase.WikibaseRepository.Uris
import org.wikidata.query.rdf.updater.config.{FilteredReconciliationTopic, UpdaterPipelineInputEventStreamConfig}

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
      fromKafka(KafkaConsumerProperties(topic, ievops.kafkaBrokers, ievops.consumerGroup, DeserializationSchemaFactory.getDeserializationSchema(clazz)),
        uris, conv, ievops.maxLateness, ievops.idleness, clock, resolver, filter)
    }
    val revisionCreateEventFilter = new RevisionCreateEventFilter(ievops.mediaInfoEntityNamespaces, ievops.mediaInfoRevisionSlot)
    ievops.inputKafkaTopics.topicPrefixes.flatMap(prefix => {
      List(
        build(prefix + ievops.inputKafkaTopics.revisionCreateTopicName, classOf[RevisionCreateEvent], REV_CREATE_CONV, Some(revisionCreateEventFilter)),
        build(prefix + ievops.inputKafkaTopics.pageDeleteTopicName, classOf[PageDeleteEvent], PAGE_DEL_CONV),
        build(prefix + ievops.inputKafkaTopics.pageUndeleteTopicName, classOf[PageUndeleteEvent], PAGE_UNDEL_CONV),
        build(prefix + ievops.inputKafkaTopics.suppressedDeleteTopicName, classOf[PageDeleteEvent], PAGE_DEL_CONV)
      ) ++: ievops.inputKafkaTopics.reconciliationTopicName.map {
        case FilteredReconciliationTopic(topic, filter) =>
          build(prefix + topic, classOf[ReconcileEvent], RECONCILIATION_CONV, filter.map(new ReconciliationSourceFilter(_)))
      }
    })
  }

  def fromKafka[E <: EventPlatformEvent](kafkaProps: KafkaConsumerProperties[E], uris: Uris, // scalastyle:ignore
                                  conv: Converter[E],
                                  maxLatenessMs: Int, idlenessMs: Int, clock: Clock, resolver: EntityResolver, filter: Option[FilterFunction[E]])
                                 (implicit env: StreamExecutionEnvironment): DataStream[InputEvent] = {
    val nameAndUid = operatorUUID(kafkaProps.topic)
    val kafkaStream = env
      .addSource(new FlinkKafkaConsumer[E](kafkaProps.topic, kafkaProps.schema, kafkaProps.asProperties()))(kafkaProps.schema.getProducedType)
      .setParallelism(INPUT_PARALLELISM)
      .assignTimestampsAndWatermarks(watermarkStrategy[E](maxLatenessMs, idlenessMs))
      .uid(nameAndUid)
      .name(nameAndUid)
      .setParallelism(INPUT_PARALLELISM)
    fromStream(kafkaStream, uris, conv, clock, resolver, filter)
  }

  def operatorUUID[E <: EventPlatformEvent](topic: String): String = {
    topic
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
