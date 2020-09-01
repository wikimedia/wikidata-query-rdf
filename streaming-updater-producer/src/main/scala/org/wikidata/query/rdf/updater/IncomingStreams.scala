package org.wikidata.query.rdf.updater

import java.time.Clock

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.wikidata.query.rdf.tool.change.events.{ChangeEvent, RevisionCreateEvent, PageDeleteEvent}
import org.wikidata.query.rdf.tool.utils.EntityUtil.cleanEntityId
import org.wikidata.query.rdf.tool.wikibase.WikibaseRepository.Uris


object IncomingStreams {
  val REV_CREATE_CONV: (RevisionCreateEvent, Clock) => InputEvent =
    (e, clock) => RevCreate(cleanEntityId(e.title()), e.timestamp(), e.revision(), clock.instant(), e.meta())

  val PAGE_DEL_CONV: (PageDeleteEvent, Clock) => InputEvent =
    (e, clock) => PageDelete(cleanEntityId(e.title()), e.timestamp(), e.revision(), clock.instant(), e.meta())

  def fromKafka[E <: ChangeEvent](kafkaProps: KafkaConsumerProperties[E], hostname: String,
                                  conv: (E, Clock) => InputEvent, maxLatenessMs: Int, clock: Clock)
                                 (implicit env: StreamExecutionEnvironment): DataStream[InputEvent] = {

    val nameAndUid = s"${kafkaProps.topic}"
    val kafkaStream = env
      .addSource(new FlinkKafkaConsumer[E](kafkaProps.topic, kafkaProps.schema, kafkaProps.asProperties()))(kafkaProps.schema.getProducedType)
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[E](Time.milliseconds(maxLatenessMs)) {
        override def extractTimestamp(eventWithMeta: E): Long = eventWithMeta.timestamp().toEpochMilli
      })
      .uid(nameAndUid)
      .name(nameAndUid)
    fromStream(kafkaStream, hostname, conv, clock)
  }

  def fromStream[E <: ChangeEvent](stream: DataStream[E],
                                   hostname: String,
                                   conv: (E, Clock) => InputEvent,
                                   clock: Clock,
                                   filterParallelism: Option[Int] = None,
                                   mapperParallelism: Option[Int] = None)
                                  (implicit env: StreamExecutionEnvironment): DataStream[InputEvent] = {
    stream.filter(new EventWithMetadataHostFilter[E](hostname))
      .setParallelism(filterParallelism.getOrElse(env.getParallelism))
      .map(conv(_, clock))
      .setParallelism(mapperParallelism.getOrElse(env.getParallelism))
      .name(s"Filtered(${stream.name} == $hostname)")
  }
}

class EventWithMetadataHostFilter[E <: ChangeEvent](hostname: String) extends FilterFunction[E] {
  lazy val uris = Uris.fromString(s"https://$hostname")
  override def filter(e: E): Boolean = {
    e.domain() == uris.getHost && uris.isEntityNamespace(e.namespace())
  }
}
