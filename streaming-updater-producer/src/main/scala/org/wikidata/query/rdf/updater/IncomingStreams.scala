package org.wikidata.query.rdf.updater

import java.time.{Clock, Duration}

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
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

  val REV_CREATE_CONV: (RevisionCreateEvent, Clock) => InputEvent =
    (e, clock) => RevCreate(cleanEntityId(e.title()), e.timestamp(), e.revision(),
      Option(e.parentRevision(): java.lang.Long).map(_.toLong), clock.instant(), e.eventInfo())

  val PAGE_DEL_CONV: (PageDeleteEvent, Clock) => InputEvent =
    (e, clock) => PageDelete(cleanEntityId(e.title()), e.timestamp(), e.revision(), clock.instant(), e.eventInfo())

  val PAGE_UNDEL_CONV: (PageUndeleteEvent, Clock) => InputEvent =
    (e, clock) => PageUndelete(cleanEntityId(e.title()), e.timestamp(), e.revision(), clock.instant(), e.eventInfo())

  def buildIncomingStreams(ievops: UpdaterPipelineInputEventStreamConfig,
                           uris: Uris, clock: Clock)
                                  (implicit env: StreamExecutionEnvironment): List[DataStream[InputEvent]] = {
    ievops.inputKafkaTopics.topicPrefixes.flatMap(prefix => {
      List(
        IncomingStreams.fromKafka(KafkaConsumerProperties(prefix + ievops.inputKafkaTopics.revisionCreateTopicName, ievops.kafkaBrokers, ievops.consumerGroup,
          DeserializationSchemaFactory.getDeserializationSchema(classOf[RevisionCreateEvent])),
          uris, IncomingStreams.REV_CREATE_CONV, ievops.maxLateness, ievops.idleness, clock),
        IncomingStreams.fromKafka(KafkaConsumerProperties(prefix + ievops.inputKafkaTopics.pageDeleteTopicName, ievops.kafkaBrokers, ievops.consumerGroup,
          DeserializationSchemaFactory.getDeserializationSchema(classOf[PageDeleteEvent])),
          uris, IncomingStreams.PAGE_DEL_CONV, ievops.maxLateness, ievops.idleness, clock),
        IncomingStreams.fromKafka(KafkaConsumerProperties(prefix + ievops.inputKafkaTopics.pageUndeleteTopicName, ievops.kafkaBrokers, ievops.consumerGroup,
          DeserializationSchemaFactory.getDeserializationSchema(classOf[PageUndeleteEvent])),
          uris, IncomingStreams.PAGE_UNDEL_CONV, ievops.maxLateness, ievops.idleness, clock),
        IncomingStreams.fromKafka(KafkaConsumerProperties(prefix + ievops.inputKafkaTopics.suppressedDeleteTopicName, ievops.kafkaBrokers, ievops.consumerGroup,
          DeserializationSchemaFactory.getDeserializationSchema(classOf[PageDeleteEvent])),
          uris, IncomingStreams.PAGE_DEL_CONV, ievops.maxLateness, ievops.idleness, clock)
      )
    })
  }

  def fromKafka[E <: ChangeEvent](kafkaProps: KafkaConsumerProperties[E], uris: Uris,
                                  conv: (E, Clock) => InputEvent,
                                  maxLatenessMs: Int, idlenessMs: Int, clock: Clock)
                                 (implicit env: StreamExecutionEnvironment): DataStream[InputEvent] = {
    val nameAndUid = operatorUUID(kafkaProps.topic)
    val kafkaStream = env
      .addSource(new FlinkKafkaConsumer[E](kafkaProps.topic, kafkaProps.schema, kafkaProps.asProperties()))(kafkaProps.schema.getProducedType)
      .setParallelism(INPUT_PARALLELISM)
      .assignTimestampsAndWatermarks(watermarkStrategy[E](maxLatenessMs, idlenessMs))
      .uid(nameAndUid)
      .name(nameAndUid)
      .setParallelism(INPUT_PARALLELISM)
    fromStream(kafkaStream, uris, conv, clock)
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
                                   conv: (E, Clock) => InputEvent,
                                   clock: Clock
                                  )(implicit env: StreamExecutionEnvironment): DataStream[InputEvent] = {
    val filteredStream = stream.filter(new EventWithMetadataHostFilter[E](uris))
    // force parallelism to one (mostly for unit tests here so that we don't mess-up their ordering)
    // filtering is also very simple
    filteredStream.setParallelism(INPUT_PARALLELISM)
    val convertedStream = filteredStream
      .map(conv(_, clock))
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
