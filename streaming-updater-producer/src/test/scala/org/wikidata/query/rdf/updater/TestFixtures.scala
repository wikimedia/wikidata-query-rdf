package org.wikidata.query.rdf.updater

import java.time.{Clock, Instant, ZoneOffset}
import java.util
import java.util.{Collections, UUID}
import java.util.function.Supplier

import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

import scala.collection.JavaConverters._
import org.openrdf.model.Statement
import org.openrdf.model.impl.{StatementImpl, ValueFactoryImpl}
import org.openrdf.rio.{RDFFormat, RDFParserRegistry, RDFWriterRegistry}
import org.wikidata.query.rdf.common.uri.{PropertyType, SchemaDotOrg, UrisSchemeFactory}
import org.wikidata.query.rdf.tool.change.events.{EventWithMeta, EventsMeta}
import org.wikidata.query.rdf.tool.rdf.RDFParserSuppliers

case class MetaStatement(entityDataNS: Statement, entityNS: Statement)
case class MetaStatements(revision: MetaStatement, lastModified: MetaStatement) {
  val entityDataNS = Set(revision.entityDataNS, lastModified.entityDataNS)
  val entityNS = Set(revision.entityNS, lastModified.entityNS)
}
trait TestFixtures extends TestEventGenerator {
  val REORDERING_WINDOW_LENGTH = 60000
  val DOMAIN = "tested.domain"
  val WATERMARK_DOMAIN = "generate.watermark"
  val OUTPUT_EVENT_UUID_GENERATOR: () => String = () => "UNIQUE FOR TESTING"
  val OUTPUT_EVENT_STREAM_NAME = "wdqs_streaming_updater_test_stream"
  val STREAM: String = "input_stream"
  val ORIG_REQUEST_ID: String = UUID.randomUUID().toString


  val WATERMARK_1 = REORDERING_WINDOW_LENGTH
  val WATERMARK_2 = REORDERING_WINDOW_LENGTH*2
  val urisScheme = UrisSchemeFactory.forHost(DOMAIN)
  val valueFactory = new ValueFactoryImpl()
  val instantNow: Instant = instant(5)
  val clock: Clock = Clock.fixed(instantNow, ZoneOffset.UTC)

  val eventsMetaData: Supplier[EventsMeta] = new Supplier[EventsMeta]() {
    override def get(): EventsMeta = new EventsMeta(clock.instant(),
      OUTPUT_EVENT_UUID_GENERATOR.apply(), DOMAIN, OUTPUT_EVENT_STREAM_NAME, ORIG_REQUEST_ID)
  }

  private val eventTimes = Map (
    ("Q1", 1L) -> instant(4),
    ("Q1", 2L) -> instant(3),
    ("Q1", 5L) -> instant(WATERMARK_1 + 1),
    ("Q1", 6L) -> instant(WATERMARK_2 + 1)
  )

  val revCreateEvents = Seq(
        newRevCreateEvent("Q1", 2, eventTimes("Q1", 2), 0, DOMAIN, STREAM, ORIG_REQUEST_ID),
        newRevCreateEvent("Q1", 1, eventTimes("Q1", 1), 0, DOMAIN, STREAM, ORIG_REQUEST_ID),
        newRevCreateEvent("Q2", -1, instant(WATERMARK_1), 0, WATERMARK_DOMAIN,
          STREAM, ORIG_REQUEST_ID), //unrelated event, test filtering and triggers watermark
        newRevCreateEvent("Q1", 5, eventTimes("Q1", 5), 0, DOMAIN, STREAM, ORIG_REQUEST_ID),
        newRevCreateEvent("Q1", 3, instant(5), 0, DOMAIN, STREAM, ORIG_REQUEST_ID), // ignored late event
        newRevCreateEvent("Q2", -1, instant(WATERMARK_2), 0, WATERMARK_DOMAIN, STREAM,
          ORIG_REQUEST_ID), //unrelated event, test filter and triggers watermark
        newRevCreateEvent("Q1", 4, instant(WATERMARK_2 + 1), 0, DOMAIN, STREAM,
          ORIG_REQUEST_ID), // spurious event, rev 4 arrived after WM2 but rev5 was handled at WM1
        newRevCreateEvent("Q1", 6, eventTimes("Q1", 6), 0, DOMAIN, STREAM, ORIG_REQUEST_ID)
  )

  val revCreateEventsForPageDeleteTest = Seq(
    newRevCreateEvent("Q1", 1, instant(4), 0, DOMAIN, STREAM, ORIG_REQUEST_ID)
  )

  val pageDeleteEvents = Seq(
    newPageDeleteEvent("Q1", 1, instant(5), 0, DOMAIN, STREAM, ORIG_REQUEST_ID)
  )
  private val statement1: Statement = createStatement("Q1", PropertyType.QUALIFIER, "Statement_1")
  private val statement2: Statement = createStatement("Q1", PropertyType.QUALIFIER, "Statement_2")
  private val statement3: Statement = createStatement("Q1", PropertyType.QUALIFIER, "Statement_3")

  private val mSt1 = metaStatements("Q1", 1L)
  private val mSt2 = metaStatements("Q1", 2L)
  private val mSt5 = metaStatements("Q1", 5L)
  private val mSt6 = metaStatements("Q1", 6L)

  val ignoredRevision = RevCreate("Q1", instant(5), 3, instantNow, newEventMeta(instant(5), DOMAIN, STREAM, ORIG_REQUEST_ID))
  val ignoredMutations = Set(
    IgnoredMutation("Q1", instant(WATERMARK_2 + 1), 4,
      RevCreate("Q1", instant(WATERMARK_2 + 1), 4, instant(5),
        newEventMeta(instant(WATERMARK_2 + 1), DOMAIN, STREAM, ORIG_REQUEST_ID)
      ), instantNow, NewerRevisionSeen))
  val rdfChunkSer: RDFChunkSerializer = new RDFChunkSerializer(RDFWriterRegistry.getInstance())
  val rdfChunkDeser: RDFChunkDeserializer = new RDFChunkDeserializer(new RDFParserSuppliers(RDFParserRegistry.getInstance()))
  val dataEventGenerator = new MutationEventDataGenerator(rdfChunkSer,
    RDFFormat.TURTLE.getDefaultMIMEType, Int.MaxValue)

  private val testData = Map(
    ("Q1", 1L) -> RevisionData("Q1", eventTimes("Q1", 1), mSt1.entityDataNS.toSeq, mSt1.entityNS),
    ("Q1", 2L) -> RevisionData("Q1", eventTimes("Q1", 2), Seq(statement1) ++ mSt2.entityDataNS, Set(statement1) ++ mSt2.entityNS, mSt1.entityNS),
    ("Q1", 5L) -> RevisionData("Q1", eventTimes("Q1", 5), Seq(statement2, statement3) ++ mSt5.entityDataNS,
      Set(statement2, statement3) ++ mSt5.entityNS, Set(statement1) ++ mSt2.entityNS),
    ("Q1", 6L) -> RevisionData("Q1", eventTimes("Q1", 6), Seq(statement2) ++ mSt6.entityDataNS, mSt6.entityNS, Set(statement3) ++ mSt5.entityNS)
  )

  def getMockRepository: MockWikibaseEntityRevRepository =
    testData.foldLeft(MockWikibaseEntityRevRepository()) {
      (repo, elem) => repo.withResponse(elem._1, elem._2.inputTriples)
    }

  def expectedOperations: Seq[MutationDataChunk] = {
    Seq(
      getExpectedTripleDiff("Q1", 1L),
      getExpectedTripleDiff("Q1", 2L, 1L),
      getExpectedTripleDiff("Q1", 5L, 2L),
      getExpectedTripleDiff("Q1", 6L, 5L)
    )
  }

  def expectedOperationsForPageDeleteTest: Seq[MutationDataChunk] = {
    Seq(
      getExpectedTripleDiff("Q1", 1L),
      getExpectedDelete("Q1", 1L)
    )
  }

  def getExpectedTripleDiff(entityId: String, revisionTo: Long, revisionFrom: Long = 0L): MutationDataChunk = {
    val data: RevisionData = testData((entityId, revisionTo))
    val eventTime: Instant = eventTimes(entityId, revisionTo)
    val origEventMeta: EventsMeta = new EventsMeta(eventTime, "unused", DOMAIN, STREAM, ORIG_REQUEST_ID)
    val operation: MutationOperation = if (revisionFrom == 0L) {
      FullImport(entityId, eventTime, revisionTo, instantNow, origEventMeta)
    } else {
      Diff(entityId, eventTime, revisionTo, revisionFrom, instantNow, origEventMeta)
    }
    val dataEvent = operation match {
      case _: FullImport =>
        dataEventGenerator.fullImportEvent(eventsMetaData, entityId, revisionTo, eventTime, new util.ArrayList[Statement](data.expectedAdds.asJavaCollection),
          Collections.emptyList())
      case _: Diff =>
        dataEventGenerator.diffEvent(eventsMetaData, entityId, revisionTo, eventTime, new util.ArrayList[Statement](data.expectedAdds.asJavaCollection),
          new util.ArrayList[Statement](data.expectedRemoves.asJavaCollection), Collections.emptyList(), Collections.emptyList())
    }
    MutationDataChunk(operation, dataEvent.get(0))
  }

  def getExpectedDelete(entityId: String, revision: Long): MutationDataChunk = {
    val eventTime: Instant = instant(5)
    val origEventMeta: EventsMeta = new EventsMeta(eventTime, "unused", DOMAIN, STREAM, ORIG_REQUEST_ID)
    val operation = DeleteItem(entityId, eventTime, revision, instantNow, origEventMeta)
    val dataEvent = dataEventGenerator.deleteEvent(eventsMetaData, entityId, revision, eventTime)
    MutationDataChunk(operation, dataEvent.get(0))
  }

  def metaStatements(entityId: String, revision: Long, eventTime: Option[Long] = None): MetaStatements =
    MetaStatements(revisionStatement(entityId, revision), lastModifiedStatement(entityId, revision, eventTime))


  def revisionStatement(entityId: String, revision: Long): MetaStatement =
    MetaStatement(
      valueFactory.createStatement(entityDataUriFromResourceId(entityId), valueFactory.createURI(SchemaDotOrg.VERSION),
        valueFactory.createLiteral(revision)),
      valueFactory.createStatement(uriFromResourceId(entityId), valueFactory.createURI(SchemaDotOrg.VERSION),
        valueFactory.createLiteral(revision))
    )

  private def lastModifiedStatement(entityId: String, revision: Long, eventTime: Option[Long] = None): MetaStatement = {
    val ts = eventTime.getOrElse(eventTimes((entityId, revision)).toEpochMilli)
    MetaStatement(
      valueFactory.createStatement(entityDataUriFromResourceId(entityId), valueFactory.createURI(SchemaDotOrg.DATE_MODIFIED),
          valueFactory.createLiteral(ts)),
      valueFactory.createStatement(uriFromResourceId(entityId), valueFactory.createURI(SchemaDotOrg.DATE_MODIFIED),
        valueFactory.createLiteral(ts))
    )
  }

  private def createStatement(resourceId: String, propertyType: PropertyType, valueId: String): Statement =
    new StatementImpl(uriFromResourceId(resourceId), valueFactory.createURI(urisScheme.property(propertyType)), uriFromResourceId(valueId))

  private def uriFromResourceId(resourceId: String) = valueFactory.createURI(urisScheme.entityIdToURI(resourceId))

  private def entityDataUriFromResourceId(resourceId: String) = valueFactory.createURI(urisScheme.entityData(), resourceId)

  private case class RevisionData(entityId: String, eventTime: Instant, inputTriples: Seq[Statement],
                                  expectedAdds: Set[Statement], expectedRemoves: Set[Statement] = Set())

  def watermarkAssigner[E <: EventWithMeta](): AssignerWithPunctuatedWatermarks[E] = new AssignerWithPunctuatedWatermarks[E] {
    val wm_domain = WATERMARK_DOMAIN
    override def checkAndGetNextWatermark(t: E, l: Long): Watermark = {
      val ret = t match {
        case a: Any if a.domain() == wm_domain => Some(new Watermark(a.timestamp().toEpochMilli))
        case _: Any => None
      }
      ret.orNull
    }
    override def extractTimestamp(t: E, l: Long): Long = t.timestamp().toEpochMilli
  }
}

