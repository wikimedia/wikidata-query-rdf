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
  val REV_CREATE_CONV: (RevisionCreateEvent, Clock) => InputEvent =
    (e, clock) => RevCreate(cleanEntityId(e.title()), e.timestamp(), e.revision(),
      Option(e.parentRevision(): java.lang.Long).map(_.toLong), clock.instant(), e.meta())

  val PAGE_DEL_CONV: (PageDeleteEvent, Clock) => InputEvent =
    (e, clock) => PageDelete(cleanEntityId(e.title()), e.timestamp(), e.revision(), clock.instant(), e.meta())

  val PAGE_UNDEL_CONV: (PageUndeleteEvent, Clock) => InputEvent =
    (e, clock) => PageUndelete(cleanEntityId(e.title()), e.timestamp(), e.revision(), clock.instant(), e.meta())

  def buildIncomingStreams(ievops: UpdaterPipelineInputEventStreamConfig,
                           hostname: String, clock: Clock)
                                  (implicit env: StreamExecutionEnvironment): List[DataStream[InputEvent]] = {
    ievops.topicPrefixes.flatMap(prefix => {
      List(
        IncomingStreams.fromKafka(KafkaConsumerProperties(prefix + ievops.revisionCreateTopicName, ievops.kafkaBrokers, ievops.consumerGroup,
          DeserializationSchemaFactory.getDeserializationSchema(classOf[RevisionCreateEvent])),
          hostname, IncomingStreams.REV_CREATE_CONV, ievops.parallelism, ievops.maxLateness, ievops.idleness, clock),
        IncomingStreams.fromKafka(KafkaConsumerProperties(prefix + ievops.pageDeleteTopicName, ievops.kafkaBrokers, ievops.consumerGroup,
          DeserializationSchemaFactory.getDeserializationSchema(classOf[PageDeleteEvent])),
          hostname, IncomingStreams.PAGE_DEL_CONV, ievops.parallelism, ievops.maxLateness, ievops.idleness, clock),
        IncomingStreams.fromKafka(KafkaConsumerProperties(prefix + ievops.pageUndeleteTopicName, ievops.kafkaBrokers, ievops.consumerGroup,
          DeserializationSchemaFactory.getDeserializationSchema(classOf[PageUndeleteEvent])),
          hostname, IncomingStreams.PAGE_UNDEL_CONV, ievops.parallelism, ievops.maxLateness, ievops.idleness, clock),
        IncomingStreams.fromKafka(KafkaConsumerProperties(prefix + ievops.suppressedDeleteTopicName, ievops.kafkaBrokers, ievops.consumerGroup,
          DeserializationSchemaFactory.getDeserializationSchema(classOf[PageDeleteEvent])),
          hostname, IncomingStreams.PAGE_DEL_CONV, ievops.parallelism, ievops.maxLateness, ievops.idleness, clock)
      )
    })
  }

  def fromKafka[E <: ChangeEvent](kafkaProps: KafkaConsumerProperties[E], hostname: String,
                                  conv: (E, Clock) => InputEvent, parallelism: Int,
                                  maxLatenessMs: Int, idlenessMs: Int, clock: Clock)
                                 (implicit env: StreamExecutionEnvironment): DataStream[InputEvent] = {

    val nameAndUid = s"${kafkaProps.topic}"
    val kafkaStream = env
      .addSource(new FlinkKafkaConsumer[E](kafkaProps.topic, kafkaProps.schema, kafkaProps.asProperties()))(kafkaProps.schema.getProducedType)
      .setParallelism(parallelism)
      .assignTimestampsAndWatermarks(watermarkStrategy[E](maxLatenessMs, idlenessMs))
      .uid(nameAndUid)
      .name(nameAndUid)
      .setParallelism(parallelism)
    fromStream(kafkaStream, hostname, conv, clock, Some(parallelism), Some(parallelism))
  }

  private def watermarkStrategy[E <: ChangeEvent](maxLatenessMs: Int, idlenessMs: Int): WatermarkStrategy[E] = {
    WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMillis(maxLatenessMs))
      .withIdleness(Duration.ofMillis(idlenessMs))
      .withTimestampAssigner(new SerializableTimestampAssigner[E] {
        override def extractTimestamp(element: E, recordTimestamp: Long): Long = element.timestamp().toEpochMilli
      })
  }

  def fromStream[E <: ChangeEvent](stream: DataStream[E],
                                   hostname: String,
                                   conv: (E, Clock) => InputEvent,
                                   clock: Clock,
                                   filterParallelism: Option[Int] = None,
                                   mapperParallelism: Option[Int] = None)
                                  (implicit env: StreamExecutionEnvironment): DataStream[InputEvent] = {
    val filteredStream = stream.filter(new EventWithMetadataHostFilter[E](hostname));
    filterParallelism.foreach(filteredStream.setParallelism)

    val convertedStream = filteredStream
      .map(conv(_, clock))
      .name(s"Filtered(${stream.name} == $hostname)")
    mapperParallelism.foreach(convertedStream.setParallelism)
    convertedStream
  }
}

class EventWithMetadataHostFilter[E <: ChangeEvent](hostname: String) extends FilterFunction[E] {
  lazy val uris = Uris.fromString(s"https://$hostname")
  override def filter(e: E): Boolean = {
    e.domain() == uris.getHost && uris.isEntityNamespace(e.namespace())
  }
}
