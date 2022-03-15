package org.wikidata.query.rdf.updater.reconcile

import java.{lang, time, util}
import java.net.URI
import java.time.{Clock, Instant}
import java.util.{Optional, UUID}
import java.util.function.UnaryOperator
import scala.annotation.tailrec
import scala.collection.JavaConverters.{asScalaIteratorConverter, collectionAsScalaIterableConverter, mapAsScalaMapConverter, setAsJavaSetConverter}
import scala.collection.immutable
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}
import com.codahale.metrics.MetricRegistry
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.slf4j.LoggerFactory
import org.wikidata.query.rdf.common.uri.UrisConstants
import org.wikidata.query.rdf.spark.utils.SparkUtils
import org.wikidata.query.rdf.tool.change.events.{EventInfo, EventsMeta, ReconcileEvent}
import org.wikidata.query.rdf.tool.change.events.ReconcileEvent.Action
import org.wikidata.query.rdf.tool.exception.{ContainedException, RetryableException}
import org.wikidata.query.rdf.tool.wikibase.WikibaseRepository
import org.wikidata.query.rdf.tool.HttpClientUtils
import org.wikidata.query.rdf.tool.rdf.RDFParserSuppliers
import org.wikidata.query.rdf.tool.utils.NullStreamDumper
import org.wikidata.query.rdf.tool.wikibase.WikibaseRepository.Uris
import scopt.OptionParser

object UpdaterReconcile {
  case class Params(
                     domain: String = "",
                     initialToNamespace: String = "Q=,P=120",
                     entityNamespaces: Seq[Long] = WikibaseRepository.Uris.DEFAULT_ENTITY_NAMESPACES.asScala map Long2long toSeq,
                     apiPath: String = WikibaseRepository.Uris.DEFAULT_API_PATH,
                     entityDataPath: String = WikibaseRepository.Uris.DEFAULT_ENTITY_DATA_PATH,
                     eventGateEndpoint: URI = None.orNull,
                     eventGateBatchSize: Int = 20,
                     reconciliationSource: String = "",
                     stream: String = "rdf-streaming-updater.reconcile",
                     lateEventPartitionSpec: String = "",
                     inconsistenciesPartitionSpec: String = "",
                     failuresPartitionSpec: String = "",
                     httpRoutes: Option[String] = None,
                     httpRequestTimeout: Duration = 5 seconds
                   )
  val argsParser: OptionParser[Params] = new OptionParser[Params]("UpdaterReconcile") {
    head("RDF Streaming Updater Reconciliation tool", "")
    help("help") text "Prints this usage text"
    opt[String]("domain") required() valueName "<domain>" action {(x,p) =>
      p.copy(domain = x)
    } text "project domains to consider"
    opt[String]("reconciliation-source") required() valueName "<reconciliation-source>" action {(x,p) =>
      p.copy(reconciliationSource = x)
    } text "Name of the source of the reconciliation to tag generated events"
    opt[String]("event-gate") required() valueName "<event-gate>" action {(x,p) =>
      p.copy(eventGateEndpoint = URI.create(x))
    } text "event-gate endpoint"
    opt[String]("late-events-partition") required() valueName "<late-events-partition>" action {(x,p) =>
      p.copy(lateEventPartitionSpec = x)
    } text "hive partition spec for collecting late events"
    opt[String]("inconsistencies-partition") required() valueName "<inconsistencies-partition>" action {(x,p) =>
      p.copy(inconsistenciesPartitionSpec = x)
    } text "hive partition spec for collecting inconsistencies"
    opt[String]("failures-partition") required() valueName "<failures-partition>" action {(x,p) =>
      p.copy(failuresPartitionSpec = x)
    } text "hive partition spec for collecting failed operations"
    opt[String]("stream") optional() valueName "<stream>" action {(x,p) =>
      p.copy(stream = x)
    } text "stream to produce to"
    opt[Seq[Long]]("entity-namespaces") optional() valueName "<entity-namespaces>" action {(x,p) =>
      p.copy(entityNamespaces = x.toList)
    } text "Entity namespaces as integers"
    opt[String]("initial-to-namespace") optional() valueName "<initial-to-namespace>" action {(x,p) =>
      p.copy(initialToNamespace = x)
    } text "map of entity initials to corresponding namespace text (e.g. Q=,P=120,L=146)"
    opt[String]("api-path") optional() valueName "<api-path>" action {(x,p) =>
      p.copy(apiPath = x)
    } text s"Path to the MW API (default: ${WikibaseRepository.Uris.DEFAULT_API_PATH})"
    opt[String]("entity-data-path") optional() valueName "<entity-data-path>" action {(x,p) =>
      p.copy(entityDataPath = x)
    } text s"Path to Special:EntityData (default: ${WikibaseRepository.Uris.DEFAULT_ENTITY_DATA_PATH})"
    opt[Int]("event-gate-batch-size") optional() valueName "<event-gate-batch-size>" action {(x,p) =>
      p.copy(eventGateBatchSize = x)
    } text "max number of events to send to event-gate in a single batch"
    opt[String]("http-routes") optional() valueName "<http-routes>" action {(x,p) =>
      p.copy(httpRoutes = Some(x))
    } text "HTTP routes: hostname=scheme://IP:PORT[,others routes]"
    opt[Int]("http-request-timeout") optional() valueName "<http-request-timeout>" action {(x,p) =>
      p.copy(httpRequestTimeout = x seconds)
    } text "HTTP request timeout (seconds, default: 5)"
  }

  def main(args: Array[String]): Unit = {
    argsParser.parse(args, Params()) match {
      case Some(params) => reconcile(params)
      case None => sys.exit(1)
    }
  }

  def reconcile(params: Params): Unit = {
    val httpClient: CloseableHttpClient = HttpClientUtils.createHttpClient(
      HttpClientUtils.createPooledConnectionManager(params.httpRequestTimeout.toMillis.intValue()),
      None.orNull,
      params.httpRoutes.orNull,
      params.httpRequestTimeout.toMillis.intValue())

    val uris = new Uris(URI.create("https://" + params.domain), params.entityNamespaces.map(long2Long).toSet.asJava,
      params.apiPath, params.entityDataPath)
    val cutoff: time.Duration = None.orNull
    val wikibaseRepository = new WikibaseRepository(uris, false, new MetricRegistry(), new NullStreamDumper(),
      cutoff, RDFParserSuppliers.defaultRdfParser(), httpClient)
    val initialToNamespace: UnaryOperator[String] = WikibaseRepository.entityIdToMediaWikiTitle(params.initialToNamespace)

    val collector = new ReconcileCollector(
      reconciliationSource = params.reconciliationSource,
      stream = params.stream,
      domain = params.domain,
      latestRevisionForEntities = ids => wikibaseRepository.fetchLatestRevisionForEntities(ids, initialToNamespace),
      latestRevisionForMediaInfoItems = ids => wikibaseRepository.fetchLatestRevisionForMediainfoItems(ids)
    )

    implicit val spark: SparkSession = SparkSession
      .builder()
      .getOrCreate()
    val sender = new ReconciliationSender(httpClient, params.eventGateEndpoint, params.eventGateBatchSize)
    sender.send(collector.collectLateEvents(params.lateEventPartitionSpec))
    sender.send(collector.collectFailures(params.failuresPartitionSpec))
    sender.send(collector.collectInconsistencies(params.inconsistenciesPartitionSpec))
  }
}

class ReconcileCollector(reconciliationSource: String,
                         stream: String,
                         domain: String,
                         latestRevisionForEntities: util.Set[String] => util.Map[String, Optional[lang.Long]],
                         latestRevisionForMediaInfoItems: util.Set[String] => util.Map[String, Optional[lang.Long]],
                         now: () => Instant = () => Clock.systemUTC().instant(),
                         idGen: () => String = () => UUID.randomUUID().toString,
                         requestIdGen: () =>  String = () => UUID.randomUUID().toString
               ) {

  private val logger = LoggerFactory.getLogger(this.getClass)
  private val schema: String = ReconcileEvent.SCHEMA
  private val lateEventActionMap: Map[String, Action] = Map(
    "revision-create" -> Action.CREATION,
    "page-delete" -> Action.DELETION,
    "page-undelete" -> Action.CREATION,
    "reconcile-deletion" -> Action.DELETION,
    "reconcile-creation" -> Action.CREATION)

  private val inconsistenciesActionMap: Map[String, Action] = Map[String, Action]("unmatched_delete" -> Action.CREATION)

  def collectLateEvents(partitionSpec: String)(implicit spark: SparkSession): List[ReconcileEvent] = {
    // https://schema.wikimedia.org/repositories/secondary/jsonschema/rdf_streaming_updater/lapsed_action/current.yaml
    val events = SparkUtils.readTablePartition(partitionSpec)
      .filter(col("action_type").isInCollection(lateEventActionMap.keys))
      .filter(col("meta.domain").equalTo(domain))
      .select(
        col("meta"),
        col("item"),
        col("revision_id"),
        col("original_event_info"),
        col("action_type"))
      .toLocalIterator().asScala
      .map(e => {
        val origEventInfo = rowToEventInfo(e.getAs[Row]("original_event_info"))
        val actionType: String = e.getAs("action_type")
        if (actionType == "reconcile-creation" || actionType == "reconcile-deletion") {
          warnMissedReconciliation(e)
        }
        new ReconcileEvent(
          new EventsMeta(now(), idGen(), origEventInfo.meta().domain(), stream, requestIdGen()),
          schema,
          e.getAs("item"),
          e.getAs("revision_id"),
          reconciliationSource,
          lateEventActionMap.getOrElse(e.getAs("action_type"), Action.CREATION), origEventInfo)
      }) toList

    logger.info(s"Collected ${events.length} late events from $partitionSpec")
    events
  }

  def collectFailures(partitionSpec: String)(implicit spark: SparkSession): List[ReconcileEvent] = {
    // https://schema.wikimedia.org/repositories//secondary/jsonschema/rdf_streaming_updater/fetch_failure/current.yaml
    val rows: List[(EventInfo, String, Long)] = SparkUtils.readTablePartition(partitionSpec)
      .filter(col("meta.domain").equalTo(domain))
      .select(
        col("meta"),
        col("item"),
        col("revision_id"),
        col("original_event_info"))
      .toLocalIterator().asScala.map(e => {
        (rowToEventInfo(e.getAs[Row]("original_event_info")), e.getAs[String]("item"), e.getAs[Long]("revision_id"))
      }).toList

    val mediainfo: immutable.Seq[(EventInfo, String, Long)] = rows filter {
      case (_, item, _) => item.startsWith(UrisConstants.MEDIAINFO_INITIAL)
    } match {
      case e: Any => fetchLatestRevision(e, latestRevisionForMediaInfoItems)
    }
    val entities = rows filterNot {
      case (_, item, _) => item.startsWith(UrisConstants.MEDIAINFO_INITIAL)
    } match {
      case e: Any => fetchLatestRevision(e, latestRevisionForEntities)
    }

    val filtered: List[ReconcileEvent] = (mediainfo ++ entities) map {
      case (eventInfo, item, revision) =>
        new ReconcileEvent(
          new EventsMeta(now(), idGen(), eventInfo.meta().domain(), stream, requestIdGen()),
          schema,
          item,
          revision,
          reconciliationSource,
          Action.CREATION,
          eventInfo)
    } toList

    logger.info(s"Kept ${rows.length} out of ${filtered.length} fetch-failure events from $partitionSpec")
    filtered
  }

  /**
   * consolidate the list of revision we have to reconcile by checking the MW Api to:
   * - ignore revisions that have been deleted
   * - choose most recent revision between the one returned by MW and the one present in the event, should always be MW)
   */
  private def fetchLatestRevision(data: List[(EventInfo, String, Long)],
                                  fetcher: util.Set[String] => util.Map[String, Optional[lang.Long]]
                                 ): List[(EventInfo, String, Long)] = {
    val perItemMap: Map[String, (EventInfo, String, Long)] = data groupBy { _._2 } mapValues { e => e.reduceLeft {(a, b) => if (a._3 > b._3) a else b} }

    perItemMap.grouped(WikibaseRepository.MAX_ITEMS_PER_ACTION_REQUEST) flatMap { chunk =>
      val revMap: Map[String, Optional[lang.Long]] = withRetry()(() => fetcher(chunk.keySet.asJava).asScala.toMap)
      chunk.toSeq map {
        // (key, (EventInfo, item, revision)) -> (revFromMWApi, (EventInfo, item, revision))
        case (k, v) => (revMap.getOrElse(k, Optional.empty()), v)
      } filter {
        // Ignore revisions that have been deleted, the pipeline certainly received a delete event afterward so no
        // need to replay this event (we would be unable to fetch its content anyways).
        case (rev, _) => rev.isPresent
      } map {
        // (Optional revFromMWApi, (EventInfo, item, revision)) -> (rev, (EventInfo, item, revision))
        case (rev, evt) => (Long2long(rev.orElse(evt._3)), evt)
      } map {
        // We should choose the most recent revision between the one returned by the MW Api and the one present in the event
        case (rev, (origEvent, key, evtRevision)) =>
          if (rev < evtRevision) {
            // Something really weird is happening if MW is returning something older than what the pipeline already
            // received. Nothing we could automatically do so just log something in case it might help debug something.
            logger.warn(s"MW returned an older revision than the one already received by the flink pipeline: " +
              s"$key with MW revision: $rev < event revision: $evtRevision (event metadata: ${origEvent.meta()})")
          }
          (origEvent, key, if (rev > evtRevision) rev else evtRevision)
      }
    } toList
  }

  def collectInconsistencies(partitionSpec: String)(implicit spark: SparkSession): List[ReconcileEvent] = {
    // https://schema.wikimedia.org/repositories/secondary/jsonschema/rdf_streaming_updater/state_inconsistency/current.yaml
    val events = SparkUtils.readTablePartition(partitionSpec)
      .filter(col("meta.domain").equalTo(domain))
      .filter(col("inconsistency").isInCollection(inconsistenciesActionMap.keys))
      .select(
        col("meta"),
        col("item"),
        col("revision_id"),
        col("original_event_info"),
        col("inconsistency"),
        col("action_type"))
    .toLocalIterator().asScala
      .map(e => {
        val origEventInfo = rowToEventInfo(e.getAs[Row]("original_event_info"))
        if (e.getAs[String]("action_type") == "reconcile") {
          warnMissedReconciliation(e)
        }
        new ReconcileEvent(
          new EventsMeta(now(), idGen(), origEventInfo.meta().domain(), stream, requestIdGen()),
          schema,
          e.getAs("item"),
          e.getAs("revision_id"),
          reconciliationSource,
          inconsistenciesActionMap.getOrElse(e.getAs("inconsistency"), Action.CREATION),
          origEventInfo)
      }).toList

    logger.info(s"Collected ${events.length} inconsistencies from $partitionSpec", events.length, partitionSpec)
    events
  }

  val MW_CALL_RETRY_WAIT_MS = 500
  val MW_CALL_RETRIES = 3

  @tailrec
  private def withRetry[E](nretry: Int = MW_CALL_RETRIES)(func: () => E): E = {
    Try {
      func()
    } match {
      case Success(value) => value
      case Failure(_: RetryableException) if nretry > 0 => Thread.sleep(MW_CALL_RETRY_WAIT_MS); withRetry(nretry - 1)(func)
      case Failure(e) => throw new ContainedException("Failed to apply function", e)
    }
  }

  private def warnMissedReconciliation(e: Row): Unit = {
    val meta = rowToEventMeta(e.getAs[Row]("meta"))
    logger.warn(s"Reconciling a late reconciliation event, event: $meta item: ${e.getAs[String]("item")}, revision: ${e.getAs[Long]("revision_id")}")
  }


  def rowToEventInfo(row: Row): EventInfo = {
    new EventInfo(
      rowToEventMeta(row.getAs[Row]("meta")),
      "unused"
    )
  }

  def rowToEventMeta(row: Row): EventsMeta = {
    new EventsMeta(
      Option(row.getAs[String]("dt")).map(Instant.parse(_)).orNull,
      row.getAs[String]("id"),
      row.getAs[String]("domain"),
      row.getAs[String]("stream"),
      row.getAs[String]("request_id")
    )
  }
}

