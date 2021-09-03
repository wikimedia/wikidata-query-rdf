package org.wikidata.query.rdf.updater

import java.time.{Clock, Duration}

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.wikidata.query.rdf.common.uri.UrisConstants
import org.wikidata.query.rdf.tool.change.events.{ChangeEvent, PageDeleteEvent, PageUndeleteEvent, RevisionCreateEvent}
import org.wikidata.query.rdf.tool.utils.EntityUtil.cleanEntityId
import org.wikidata.query.rdf.tool.wikibase.WikibaseRepository.Uris
import org.wikidata.query.rdf.updater.config.UpdaterPipelineInputEventStreamConfig

// FIXME rework parts this class do limit duplication
object IncomingStreams {
  // Our kafka topics have a single partition, force a parallelism of 1 for the input
  // so that we do not create useless kafka consumer but also only shuffle once we key
  // the stream.
  private val INPUT_PARALLELISM = 1;

  type EntityResolver = (Long, String, Long) => String
  type Converter[E] = (E, EntityResolver, Clock) => InputEvent

  val REV_CREATE_CONV: Converter[RevisionCreateEvent] =
    (e, resolver, clock) => RevCreate(resolver(e.namespace(), e.title(), e.pageId()), e.timestamp(), e.revision(),
      Option(e.parentRevision(): java.lang.Long).map(_.toLong), clock.instant(), e.eventInfo())

  val PAGE_DEL_CONV: Converter[PageDeleteEvent] =
    (e, resolver, clock) => PageDelete(resolver(e.namespace(), e.title(), e.pageId()), e.timestamp(), e.revision(), clock.instant(), e.eventInfo())

  val PAGE_UNDEL_CONV: Converter[PageUndeleteEvent] =
    (e, resolver, clock) => PageUndelete(resolver(e.namespace(), e.title(), e.pageId()), e.timestamp(), e.revision(), clock.instant(), e.eventInfo())

  def buildIncomingStreams(ievops: UpdaterPipelineInputEventStreamConfig,
                           uris: Uris, clock: Clock)
                                  (implicit env: StreamExecutionEnvironment): List[DataStream[InputEvent]] = {

    val resolver: EntityResolver = (ns, title, pageId) => if (ievops.mediaInfoEntityNamespaces.contains(ns)) {
      UrisConstants.MEDIAINFO_INITIAL + pageId
    } else {
      cleanEntityId(title)
    }

    def build[E <: ChangeEvent](topic: String, clazz: Class[E], conv: Converter[E]) = {
      fromKafka(KafkaConsumerProperties(topic, ievops.kafkaBrokers, ievops.consumerGroup, DeserializationSchemaFactory.getDeserializationSchema(clazz)),
        uris, conv, ievops.maxLateness, ievops.idleness, clock, resolver)
    }

    ievops.inputKafkaTopics.topicPrefixes.flatMap(prefix => {
      List(
        build(prefix + ievops.inputKafkaTopics.revisionCreateTopicName, classOf[RevisionCreateEvent], REV_CREATE_CONV),
        build(prefix + ievops.inputKafkaTopics.pageDeleteTopicName, classOf[PageDeleteEvent], PAGE_DEL_CONV),
        build(prefix + ievops.inputKafkaTopics.pageUndeleteTopicName, classOf[PageUndeleteEvent], PAGE_UNDEL_CONV),
        build(prefix + ievops.inputKafkaTopics.suppressedDeleteTopicName, classOf[PageDeleteEvent], PAGE_DEL_CONV)
      )
    })
  }

  def fromKafka[E <: ChangeEvent](kafkaProps: KafkaConsumerProperties[E], uris: Uris,
                                  conv: Converter[E],
                                  maxLatenessMs: Int, idlenessMs: Int, clock: Clock, resolver: EntityResolver)
                                 (implicit env: StreamExecutionEnvironment): DataStream[InputEvent] = {
    val nameAndUid = operatorUUID(kafkaProps.topic)
    val kafkaStream = env
      .addSource(new FlinkKafkaConsumer[E](kafkaProps.topic, kafkaProps.schema, kafkaProps.asProperties()))(kafkaProps.schema.getProducedType)
      .setParallelism(INPUT_PARALLELISM)
      .assignTimestampsAndWatermarks(watermarkStrategy[E](maxLatenessMs, idlenessMs))
      .uid(nameAndUid)
      .name(nameAndUid)
      .setParallelism(INPUT_PARALLELISM)
    fromStream(kafkaStream, uris, conv, clock, resolver)
  }

  def operatorUUID[E <: ChangeEvent](topic: String): String = {
    topic
  }

  private def watermarkStrategy[E <: ChangeEvent](maxLatenessMs: Int, idlenessMs: Int): WatermarkStrategy[E] = {
    WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMillis(maxLatenessMs))
      .withIdleness(Duration.ofMillis(idlenessMs))
      .withTimestampAssigner(new SerializableTimestampAssigner[E] {
        override def extractTimestamp(element: E, recordTimestamp: Long): Long = element.timestamp().toEpochMilli
      })
  }

  def fromStream[E <: ChangeEvent](stream: DataStream[E],
                                   uris: Uris,
                                   conv: Converter[E],
                                   clock: Clock,
                                   resolver: EntityResolver
                                  )(implicit env: StreamExecutionEnvironment): DataStream[InputEvent] = {
    val filteredStream = stream.filter(new EventWithMetadataHostFilter[E](uris))
    // force parallelism to one (mostly for unit tests here so that we don't mess-up their ordering)
    // filtering is also very simple
    filteredStream.setParallelism(INPUT_PARALLELISM)
    val convertedStream = filteredStream
      .map(conv(_, resolver, clock))
      .name(s"Filtered(${stream.name} == ${uris.getHost})")
    // for parallelism to 1 again for the same reasons
    convertedStream.setParallelism(INPUT_PARALLELISM)
    convertedStream
  }
}

class EventWithMetadataHostFilter[E <: ChangeEvent](uris: Uris) extends FilterFunction[E] {
  override def filter(e: E): Boolean = {
    uris.getHost.equals(e.domain()) && uris.isEntityNamespace(e.namespace())
  }
}
