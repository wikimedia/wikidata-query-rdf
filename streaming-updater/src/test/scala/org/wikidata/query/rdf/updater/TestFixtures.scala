package org.wikidata.query.rdf.updater

import java.time.{Clock, Instant, ZoneOffset}

import org.openrdf.model.Statement
import org.openrdf.model.impl.{StatementImpl, ValueFactoryImpl}
import org.wikidata.query.rdf.common.uri.{PropertyType, SchemaDotOrg, UrisSchemeFactory}

case class MetaStatement(entityDataNS: Statement, entityNS: Statement)
case class MetaStatements(revision: MetaStatement, lastModified: MetaStatement) {
  val entityDataNS = Set(revision.entityDataNS, lastModified.entityDataNS)
  val entityNS = Set(revision.entityNS, lastModified.entityNS)
}
trait TestFixtures extends TestEventGenerator {
  val REORDERING_WINDOW_LENGTH = 60000
  val DOMAIN = "tested.domain"
  val WATERMARK_1 = REORDERING_WINDOW_LENGTH
  val WATERMARK_2 = REORDERING_WINDOW_LENGTH*2
  val urisScheme = UrisSchemeFactory.forHost(DOMAIN)
  val valueFactory = new ValueFactoryImpl()
  val instantNow: Instant = instant(5)
  val clock: Clock = Clock.fixed(instantNow, ZoneOffset.UTC)

  private val eventTimes = Map (
    ("Q1", 1L) -> instant(4),
    ("Q1", 2L) -> instant(3),
    ("Q1", 5L) -> instant(WATERMARK_1 + 1),
    ("Q1", 6L) -> instant(WATERMARK_2 + 1)
  )

  val inputEvents = Seq(
        newEvent("Q1", 2, eventTimes("Q1", 2), 0, DOMAIN),
        newEvent("Q1", 1, eventTimes("Q1", 1), 0, DOMAIN),
        newEvent("Q2", -1, instant(WATERMARK_1), 0, "unrelated.domain"), //unrelated event, test filtering and triggers watermark
        newEvent("Q1", 5, eventTimes("Q1", 5), 0, DOMAIN),
        newEvent("Q1", 3, instant(5), 0, DOMAIN), // ignored late event
        newEvent("Q2", -1, instant(WATERMARK_2), 0, "unrelated.domain"), //unrelated event, test filter and triggers watermark
        newEvent("Q1", 4, instant(WATERMARK_2 + 1), 0, DOMAIN), // spurious event, rev 4 arrived after WM2 but rev5 was handled at WM1
        newEvent("Q1", 6, eventTimes("Q1", 6), 0, DOMAIN)
  )

  private val statement1: Statement = createStatement("Q1", PropertyType.QUALIFIER, "Statement 1")
  private val statement2: Statement = createStatement("Q1", PropertyType.QUALIFIER, "Statement 2")
  private val statement3: Statement = createStatement("Q1", PropertyType.QUALIFIER, "Statement 3")

  private val mSt1 = metaStatements("Q1", 1L)
  private val mSt2 = metaStatements("Q1", 2L)
  private val mSt5 = metaStatements("Q1", 5L)
  private val mSt6 = metaStatements("Q1", 6L)

  val ignoredRevision = Rev("Q1", instant(5), 3, instantNow)
  val ignoredMutations = Set(IgnoredMutation("Q1", instant(WATERMARK_2 + 1), 4,
    Rev("Q1", instant(WATERMARK_2 + 1), 4, instant(5)), instantNow))

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

  def expectedTripleDiffs: Seq[EntityTripleDiffs] = {
    Seq(
      getExpectedTripleDiff("Q1", 1L),
      getExpectedTripleDiff("Q1", 2L, 1L),
      getExpectedTripleDiff("Q1", 5L, 2L),
      getExpectedTripleDiff("Q1", 6L, 5L)
    )
  }

  def getExpectedTripleDiff(entityId: String, revisionTo: Long, revisionFrom: Long = 0L): EntityTripleDiffs = {
    val data: RevisionData = testData((entityId, revisionTo))
    val eventTime: Instant = eventTimes(entityId, revisionTo)
    val operation: MutationOperation = if (revisionFrom == 0L) {
      FullImport(entityId, eventTime, revisionTo, instantNow)
    } else {
      Diff(entityId, eventTime, revisionTo, revisionFrom, instantNow)
    }
    EntityTripleDiffs(operation, data.expectedAdds, data.expectedRemoves)
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
}

