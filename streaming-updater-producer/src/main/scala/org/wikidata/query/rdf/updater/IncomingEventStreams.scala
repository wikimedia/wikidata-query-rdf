package org.wikidata.query.rdf.updater

import com.google.common.annotations.VisibleForTesting
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.{FilterFunction, MapFunction}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.connector.source.{Source, SourceSplit}
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala._
import org.apache.flink.types.Row
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.slf4j.{Logger, LoggerFactory}
import org.wikidata.query.rdf.common.uri.UrisConstants
import org.wikidata.query.rdf.tool.EntityId.cleanEntityId
import org.wikidata.query.rdf.tool.change.events._
import org.wikidata.query.rdf.tool.wikibase.WikibaseRepository.Uris
import org.wikidata.query.rdf.updater.IncomingEventStreams._
import org.wikidata.query.rdf.updater.config.{FilteredReconciliationStream, InputStreams, UpdaterPipelineInputEventStreamConfig}
import org.wikimedia.eventutilities.flink.stream.EventDataStreamFactory

import java.time.{Clock, Duration, Instant}
import java.util.Properties
import java.{lang, util}
import scala.collection.JavaConverters._

class IncomingEventStreams(inputEventConfig: UpdaterPipelineInputEventStreamConfig,
                           uris: Uris,
                           eventDataStreamFactory: EventDataStreamFactory,
                           clock: Clock = Clock.systemUTC()) {

  private val inputStreams: InputStreams = inputEventConfig.inputStreams match {
    case Left(_) => throw new IllegalArgumentException("This class does not support IncomingTopics")
    case Right(ie) => ie
  }

  def buildIncomingStreams()(implicit env: StreamExecutionEnvironment): List[DataStream[InputEvent]] = {

    val consumerProperties = new Properties()
    consumerProperties.putAll(inputEventConfig.consumerProperties.asJava)

    val pageChangeSource: KafkaSource[Row] = buildKafkaSource(consumerProperties, inputStreams.pageChangeStream)
    val pageChangeStream = buildPageChangeSourceStream(pageChangeSource)

    val reconciliationStream = inputStreams.reconciliationStream.map { case FilteredReconciliationStream(streamName, optionalSourceFilter) =>
      val reconciliationSource: KafkaSource[Row] = buildKafkaSource(consumerProperties, streamName)

      buildFilteredReconciliationDataStream(streamName, reconciliationSource, optionalSourceFilter)
    }

    List(pageChangeStream) ++ reconciliationStream
  }


  @VisibleForTesting
  def buildPageChangeSourceStream(source: Source[Row, _ <: SourceSplit, _])(implicit env: StreamExecutionEnvironment): DataStream[InputEvent] = {

    val filters = Seq(
      new HostFilter(uris),
      PAGE_CHANGE_TYPE_FILTER,
      new NamespaceFilter(uris),
      new ContentModelFilter(inputStreams.contentModels, inputEventConfig.mediaInfoEntityNamespaces, inputEventConfig.mediaInfoRevisionSlot)
    )
    val nameAndUuid = s"KafkaSource:${inputStreams.pageChangeStream}"
    buildDataStream(source,
      nameAndUuid,
      filters,
      watermarkStrategy(inputEventConfig.maxLateness, inputEventConfig.idleness, EVENT_TIME_EXTRACTOR),
      new PageChangeConverter(inputEventConfig.mediaInfoEntityNamespaces, clock))
  }

  def buildFilteredReconciliationDataStream(streamName: String,
                                            source: Source[Row, _ <: SourceSplit, _],
                                            optionalSourceFilter: Option[String])
                                           (implicit env: StreamExecutionEnvironment): DataStream[InputEvent] = {

    val nameAndUuid = s"KafkaSource:$streamName"
    val filters = Seq(new HostFilter(uris)) ++ optionalSourceFilter.map(new ReconciliationEventSourceFilter(_))
    buildDataStream(source, nameAndUuid, filters,
      watermarkStrategy(inputEventConfig.maxLateness, inputEventConfig.idleness, META_TIMESTAMP_ASSIGNER), new ReconciliationRowConverter(clock))
  }

  private def buildDataStream(source: Source[Row, _ <: SourceSplit, _],
                              nameAndUuid: String,
                              filters: Seq[FilterFunction[Row]],
                              watermarkStrategy: WatermarkStrategy[Row],
                              converter: Converter)
                             (implicit env: StreamExecutionEnvironment): DataStream[InputEvent] = {
    val typeInfo = source match {
      case s: ResultTypeQueryable[_] => s.getProducedType.asInstanceOf[TypeInformation[Row]]
      case _ => throw new IllegalStateException("The source must implement ResultTypeQueryable")
    }
    env.fromSource(source, watermarkStrategy, nameAndUuid)(typeInfo)
      .name(nameAndUuid)
      .uid(nameAndUuid)
      .setParallelism(INPUT_PARALLELISM)
      .filter(new Filters(filters))
      .name(s"filtered:$nameAndUuid")
      .uid(s"filtered:$nameAndUuid")
      .setParallelism(INPUT_PARALLELISM)
      .map(converter)(InputEventSerializer.typeInfo())
      .name(s"converted:$nameAndUuid")
      .uid(s"converted:$nameAndUuid")
      .setParallelism(INPUT_PARALLELISM)
  }

  private def buildKafkaSource(
                               consumerProperties: Properties,
                               streamName: String
                              ) = {
    eventDataStreamFactory.kafkaSourceBuilder(streamName, inputEventConfig.kafkaBrokers, inputEventConfig.consumerGroup)
      .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST)) // latest if no group offsets are available
      .setProperties(consumerProperties)
      .setClientIdPrefix(inputEventConfig.consumerGroup + ":" + streamName)
      .build()
  }

  private def watermarkStrategy(maxLatenessMs: Int,
                                idlenessMs: Int,
                                timestampAssigner: SerializableTimestampAssigner[Row]
                               ): WatermarkStrategy[Row] = {
    WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMillis(maxLatenessMs))
      .withIdleness(Duration.ofMillis(idlenessMs))
      .withTimestampAssigner(timestampAssigner)
  }
}

object IncomingEventStreams {
  @transient
  lazy val log: Logger = LoggerFactory.getLogger(IncomingEventStreams.getClass)
  // Set to 1 to match the single-partition Kafka topics, avoiding redundant
  // consumers and minimizing shuffles until the stream is keyed.
  private val INPUT_PARALLELISM = 1

  private val PAGE_CHANGE_TYPE_FILTER: FilterFunction[Row] = new FilterFunction[Row] {
    override def filter(value: Row): Boolean = value.getFieldAs[String]("page_change_kind") match {
      case "create" | "edit" | "move" | "undelete" | "delete" => true
      case _ => false
    }
  }

  val META_DT_EXTRACTOR: Row => Instant = row => row.getFieldAs[Row]("meta").getFieldAs[Instant]("dt")

  private val META_CONV: Row => EventsMeta = row => new EventsMeta(
    row.getFieldAs("dt"), row.getFieldAs("id"), row.getFieldAs("domain"), row.getFieldAs("stream"), row.getFieldAs("request_id")
  )
  val EVENT_INFO_CONV: Row => EventInfo = row => new EventInfo(META_CONV(row.getFieldAs("meta")), row.getFieldAs("$schema"))

  private val META_TIMESTAMP_ASSIGNER: SerializableTimestampAssigner[Row] = new SerializableTimestampAssigner[Row] {
    override def extractTimestamp(element: Row, recordTimestamp: Long): Long = META_DT_EXTRACTOR.apply(element).toEpochMilli
  }

  val PAGE_NS_EXTRACTOR: Row => Long = r => r.getFieldAs[Row]("page").getFieldAs[Long]("namespace_id")

  private val EVENT_TIME_EXTRACTOR = new SerializableTimestampAssigner[Row] {
    override def extractTimestamp(element: Row, recordTimestamp: Long): Long = element.getFieldAs[Instant]("dt").toEpochMilli
  }

  def apply(config: UpdaterPipelineInputEventStreamConfig, uris: Uris, eventDataStreamFactory: EventDataStreamFactory, clock: Clock)
           (implicit env: StreamExecutionEnvironment): List[DataStream[InputEvent]] = {
    new IncomingEventStreams(config, uris, eventDataStreamFactory, clock)
      .buildIncomingStreams()
  }
}

class Filters(filters: Seq[FilterFunction[Row]]) extends FilterFunction[Row] {
  override def filter(value: Row): Boolean = filters.forall(_.filter(value))
}

class HostFilter(uris: Uris) extends FilterFunction[Row] {
  override def filter(e: Row): Boolean = uris.getHost.equals(e.getFieldAs[Row]("meta").getFieldAs[String]("domain"))
}

class NamespaceFilter(uris: Uris) extends FilterFunction[Row] {
  override def filter(e: Row): Boolean = uris.isEntityNamespace(IncomingEventStreams.PAGE_NS_EXTRACTOR(e))
}

class ContentModelFilter(mainContentModels: Set[String], mediaInfoEntityNamespaces: Set[Long], mediaInfoRevSlot: String) extends FilterFunction[Row] {
  override def filter(value: Row): Boolean = {
    Option(value.getFieldAs[Row]("revision")) match {
      case Some(r) => Option(r.getFieldAs[util.Map[String, Row]]("content_slots")) match {
        case Some(m) if isMediaInfoEvent(value, m) => true
        case Some(m) if m.containsKey("main") => mainContentModels.contains(m.get("main").getFieldAs("content_model"))
        // deleted suppress do not have the content_slots set
        case None if "delete".equals(value.getFieldAs[String]("page_change_kind")) => true
        case _ => false
      }
      // no revision subfield is weird...
      case _ =>
        IncomingEventStreams.log.warn("Received page_change event without a revision field, meta.id = {}",
          Option(value.getFieldAs[Row]("meta")).map(_.getFieldAs[String]("id")).getOrElse("unknown"))
        false
    }
  }

  private def isMediaInfoEvent(value: Row, m: util.Map[String, Row]) =
    mediaInfoEntityNamespaces.contains(IncomingEventStreams.PAGE_NS_EXTRACTOR(value)) && m.containsKey(mediaInfoRevSlot)
}

trait Converter extends MapFunction[Row, InputEvent]


class ReconciliationEventSourceFilter(source: String) extends FilterFunction[Row] {
  override def filter(value: Row): Boolean = source.equals(value.getFieldAs[String]("reconciliation_source"))
}

class PageChangeConverter(mediaInfoNs: Set[Long], clock: Clock) extends Converter {
  override def map(row: Row): InputEvent = {
    val page: Row = row.getFieldAs("page")
    val ns: Long = page.getFieldAs("namespace_id")
    val title: String = page.getFieldAs("page_title")
    val pageId: Long = page.getFieldAs("page_id")
    val entityId: String = if (mediaInfoNs.contains(ns)) {
      UrisConstants.MEDIAINFO_INITIAL + pageId
    } else {
      cleanEntityId(title)
    }
    val dt: Instant = row.getFieldAs("dt")
    val revision: Row = row.getFieldAs("revision")
    val revId: Long = revision.getFieldAs("rev_id")
    val eventInfo: EventInfo = IncomingEventStreams.EVENT_INFO_CONV.apply(row)
    row.getFieldAs[String]("page_change_kind") match {
      case "create" | "edit" | "move" =>
        RevCreate(
          entityId,
          dt,
          revId,
          Option(revision.getFieldAs[lang.Long]("rev_parent_id")).map(_.toLong),
          clock.instant(),
          eventInfo
        )
      case "undelete" => PageUndelete(entityId, dt, revId, clock.instant(), eventInfo)
      case "delete" => PageDelete(entityId, dt, revId, clock.instant(), eventInfo)
    }
  }
}

class ReconciliationRowConverter(clock: Clock) extends Converter {
  override def map(row: Row): ReconcileInputEvent = ReconcileInputEvent(
    row.getFieldAs[String]("item"),
    // The root dt field is not mandatory and might not set by the reconcile spark job
    Option(row.getFieldAs[Instant]("dt")).getOrElse(IncomingEventStreams.META_DT_EXTRACTOR.apply(row)),
    row.getFieldAs("revision_id"),
    row.getFieldAs[String]("reconciliation_action") match {
      case "CREATION" => ReconcileCreation
      case "DELETION" => ReconcileDeletion
    },
    clock.instant(),
    EVENT_INFO_CONV(row)
  )
}
