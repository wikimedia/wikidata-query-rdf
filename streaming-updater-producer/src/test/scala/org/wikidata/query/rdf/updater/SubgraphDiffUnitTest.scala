package org.wikidata.query.rdf.updater

import org.openrdf.model
import org.openrdf.model.impl.ValueFactoryImpl
import org.openrdf.model.{Statement, ValueFactory}
import org.scalatest.{FlatSpec, Matchers}
import org.wikidata.query.rdf.common.uri._
import org.wikidata.query.rdf.test.StatementHelper
import org.wikidata.query.rdf.test.StatementHelper.statement
import org.wikidata.query.rdf.tool.change.events.{EventInfo, EventsMeta}
import org.wikidata.query.rdf.tool.rdf.{EntityDiff, Patch}
import org.wikidata.query.rdf.tool.subgraph.{SubgraphDefinitions, SubgraphDefinitionsParser}

import java.net.URI
import java.time.Instant
import java.util.{Collections, UUID}
import scala.collection.JavaConverters._

class SubgraphDiffUnitTest extends FlatSpec with Matchers {
  val valueFactory: ValueFactory = new ValueFactoryImpl()
  val uriScheme: UrisScheme = UrisSchemeFactory.forWikidata(URI.create("https://unittest.local"))
  val now: Instant = Instant.now()
  val eventInfo = new EventInfo(new EventsMeta(now, "unused", "unused", "unused", "unused"), "unused")

  val Q1_R1: Iterable[Statement] = Seq(
    rev("Q1", 1),
    wdt("Q1", "P31", "Q10")
  ) ++ propWithRef("Q1", "P50", "Q42", "P143", "Q8447")

  val Q1_R2: Iterable[Statement] = Seq(
    rev("Q1", 2),
    wdt("Q1", "P31", "Q10")
  ) ++ propWithRef("Q1", "P50", "Q42", "P143", "Q8447")

  val Q1_R3: Iterable[Statement] = Seq(
    rev("Q1", 3),
    wdt("Q1", "P31", "Q11")
  ) ++ propWithRef("Q1", "P50", "Q42", "P143", "Q8447")

  val Q1_R4: Iterable[Statement] = Seq(
    rev("Q1", 4),
    wdt("Q1", "P31", "Q10"),
    wdt("Q1", "P31", "Q11")
  ) ++ propWithRef("Q1", "P50", "Q42", "P143", "Q8447")

  val Q1_R5: Iterable[Statement] = Seq(
    rev("Q1", 5),
    wdt("Q1", "P31", "Q10")
  ) ++ propWithRef("Q1", "P50", "Q42", "P143", "Q8447")

  val Q1_R6: Iterable[Statement] = Seq(
    rev("Q1", 6),
    wdt("Q1", "P31", "Q11"),
    wdt("Q1", "P31", "Q12")
  ) ++ propWithRef("Q1", "P50", "Q42", "P143", "Q8447")

  val Q1_R7: Iterable[Statement] = Seq(
    rev("Q1", 6),
    wdt("Q1", "P31", "Q11"),
    wdt("Q1", "P31", "Q12")
  ) ++ propWithRef("Q1", "P50", "Q42", "P143", "Q8447")

  val import_Q1_R1: FullImport = FullImport("Q1", now, 1, now, eventInfo)
  val import_Q1_R6: FullImport = FullImport("Q1", now, 6, now, eventInfo)
  val diff_Q1_R1_R2: Diff = Diff("Q1", now, 2, 1, now, eventInfo)
  val diff_Q1_R2_R3: Diff = Diff("Q1", now, 3, 2, now, eventInfo)
  val diff_Q1_R3_R4: Diff = Diff("Q1", now, 4, 3, now, eventInfo)
  val diff_Q1_R4_R5: Diff = Diff("Q1", now, 5, 4, now, eventInfo)
  val diff_Q1_R5_R6: Diff = Diff("Q1", now, 6, 5, now, eventInfo)
  val diff_Q1_R6_R7: Diff = Diff("Q1", now, 7, 6, now, eventInfo)
  val reconcile_Q1_R1: Reconcile = Reconcile("Q1", now, 1, now, eventInfo)
  val reconcile_Q1_R4: Reconcile = Reconcile("Q1", now, 4, now, eventInfo)
  val reconcile_Q1_R6: Reconcile = Reconcile("Q1", now, 6, now, eventInfo)
  val delete_Q1_R1: DeleteItem = DeleteItem("Q1", now, 1, now, eventInfo)

  val entityDiff: EntityDiff = EntityDiff.withWikibaseSharedElements(uriScheme)
  // 1 graph: full only
  val singleGraphDef: SubgraphDefinitions = SubgraphDefinitionsParser.parseYaml(
    this.getClass.getResourceAsStream("SubgraphDiffUnitTest-single-graph-definition.yaml"))
  // 3 graphs: full, graph1 (pass ?entity wdt:P31 wd:Q10), graph2 (block ?entity wdt:P31 wd:Q10)
  val threeGraphsDef: SubgraphDefinitions = SubgraphDefinitionsParser.parseYaml(
    this.getClass.getResourceAsStream("SubgraphDiffUnitTest-three-graphs-definitions.yaml"))
  val subgraphOneUri: model.URI = valueFactory.createURI("https://query.unittest.local/subgraph/graph1")
  val subgraphTwoUri: model.URI = valueFactory.createURI("https://query.unittest.local/subgraph/graph2")
  // 4 graphs: full, graph1 (pass ?entity wdt:P31 wd:Q10), graph2 (block ?entity wdt:P31 wd:Q10) graph3 (pass ?entity wdt:P31 Q11)

  val fourGraphsDef: SubgraphDefinitions = SubgraphDefinitionsParser.parseYaml(
    this.getClass.getResourceAsStream("SubgraphDiffUnitTest-four-graphs-definitions.yaml"))
  val fourGraphsDefMixedStubSource: SubgraphDefinitions = SubgraphDefinitionsParser.parseYaml(
    this.getClass.getResourceAsStream("SubgraphDiffUnitTest-four-graphs-definitions-mixed-stub-source.yaml"))
  val subgraphThreeUri: model.URI = valueFactory.createURI("https://query.unittest.local/subgraph/graph3")

  val singleGraphDiff: SubgraphDiff = new SubgraphDiff(entityDiff, new SubgraphAssigner(singleGraphDef), uriScheme)
  val threeGraphsDiff: SubgraphDiff = new SubgraphDiff(entityDiff, new SubgraphAssigner(threeGraphsDef), uriScheme)
  val fourGraphsDiff: SubgraphDiff = new SubgraphDiff(entityDiff, new SubgraphAssigner(fourGraphsDef), uriScheme)
  val fourGraphsDiffMixedStubSource: SubgraphDiff = new SubgraphDiff(entityDiff, new SubgraphAssigner(fourGraphsDefMixedStubSource), uriScheme)

  "SubgraphDiff" should "fullyImport an entity with a single graph definition" in {
    val result = singleGraphDiff.fullImport(import_Q1_R1, Q1_R1)
    result should contain only EntityPatchOp(import_Q1_R1, imp(Q1_R1), None)
  }

  "SubgraphDiff" should "diff an entity with a single graph definition" in {
    val result = singleGraphDiff.diff(diff_Q1_R1_R2, Q1_R1, Q1_R2)
    result should contain only EntityPatchOp(diff_Q1_R1_R2, diff(Q1_R1, Q1_R2), None)
  }

  "SubgraphDiff" should "delete an entity with a single graph definition" in {
    val result = singleGraphDiff.delete(delete_Q1_R1)
    result should contain only DeleteOp(delete_Q1_R1, None)
  }

  "SubgraphDiff" should "reconcile an entity with a single graph definition" in {
    val result = singleGraphDiff.reconcile(reconcile_Q1_R1, Q1_R1)
    result should contain only ReconcileOp(reconcile_Q1_R1, imp(Q1_R1), None)
  }

  "SubgraphDiff" should "fullyImport an entity with three graph definitions" in {
    val result = threeGraphsDiff.fullImport(import_Q1_R1, Q1_R1)
    result.size shouldBe 3
    result.filter(_.subgraph.isEmpty) should contain only EntityPatchOp(import_Q1_R1, imp(Q1_R1), None)
    result.filter(_.subgraph.contains(subgraphOneUri)) should contain only
      EntityPatchOp(import_Q1_R1, imp(Q1_R1), Some(subgraphOneUri))
    result.filter(_.subgraph.contains(subgraphTwoUri)) should contain only
      EntityPatchOp(import_Q1_R1, addStubs(stubs("Q1", subgraphOneUri)), Some(subgraphTwoUri))
  }

  "SubgraphDiff" should "diff an entity with a three graph definitions without moving between subgraphs" in {
    val result = threeGraphsDiff.diff(diff_Q1_R1_R2, Q1_R1, Q1_R2)
    result.size shouldBe 3
    result.filter(_.subgraph.isEmpty) should contain only EntityPatchOp(diff_Q1_R1_R2, diff(Q1_R1, Q1_R2), None)
    result.filter(_.subgraph.contains(subgraphOneUri)) should contain only
      EntityPatchOp(diff_Q1_R1_R2, diff(Q1_R1, Q1_R2), Some(subgraphOneUri))
    result.filter(_.subgraph.contains(subgraphTwoUri)) should contain only
      EntityPatchOp(diff_Q1_R1_R2, addStubs(stubs("Q1", subgraphOneUri)), Some(subgraphTwoUri))
  }

  "SubgraphDiff" should "diff an entity with a three graph definitions and moving between subgraphs" in {
    // Q1 moves from graph1 to graph2
    val result = threeGraphsDiff.diff(diff_Q1_R2_R3, Q1_R2, Q1_R3)
    result.size shouldBe 5
    result.filter(_.subgraph.isEmpty) should contain only EntityPatchOp(diff_Q1_R2_R3, diff(Q1_R2, Q1_R3), None)

    // we delete Q1 from graph1 and insert the stubs pointing to graph2
    result.filter(_.subgraph.contains(subgraphOneUri)).toList should contain theSameElementsInOrderAs Seq(
      DeleteOp(diff_Q1_R2_R3, Some(subgraphOneUri)),
      EntityPatchOp(diff_Q1_R2_R3, addStubs(stubs("Q1", subgraphTwoUri)), Some(subgraphOneUri)))

    // we delete delete the stubs pointing to graph1 and import Q1
    result.filter(_.subgraph.contains(subgraphTwoUri)).toList should contain theSameElementsInOrderAs Seq(
      // we re-use DeleteOp to cleanup the stubs so that there's no need to do need any book keeping there
      DeleteOp(diff_Q1_R2_R3, Some(subgraphTwoUri)),
      EntityPatchOp(diff_Q1_R2_R3, imp(Q1_R3), Some(subgraphTwoUri)))
  }

  "SubgraphDiff" should "delete an entity with a three graphs definition" in {
    val result = threeGraphsDiff.delete(delete_Q1_R1)
    result.size shouldBe 3
    result should contain only (DeleteOp(delete_Q1_R1, None), DeleteOp(delete_Q1_R1, Some(subgraphOneUri)), DeleteOp(delete_Q1_R1, Some(subgraphTwoUri)))
  }

  "SubgraphDiff" should "reconcile an entity with a three graphs definition" in {
    // We reconcile Q1 R1 which is in full and subgraph1
    val result = threeGraphsDiff.reconcile(reconcile_Q1_R1, Q1_R1)
    result.size shouldBe 3
    // reconcile full
    result.filter(_.subgraph.isEmpty) should contain only ReconcileOp(reconcile_Q1_R1, imp(Q1_R1), None)

    // reconcile subgraph1
    result.filter(_.subgraph.contains(subgraphOneUri)) should contain only ReconcileOp(reconcile_Q1_R1, imp(Q1_R1), Some(subgraphOneUri))

    // Reconcile stubs
    result.filter(_.subgraph.contains(subgraphTwoUri)).toList should contain only
      ReconcileOp(reconcile_Q1_R1, addStubs(stubs("Q1", subgraphOneUri)), Some(subgraphTwoUri))
  }

  "SubgraphDiff" should "diff an entity with a four graphs definitions and partially moving between subgraphs" in {
    // Q1 R3 is in subgraph2 and 3, R4 forces it to enter graph1 and leave graph2 it should stay in graph3
    val result = fourGraphsDiff.diff(diff_Q1_R3_R4, Q1_R3, Q1_R4)
    result.size shouldBe 6
    result.filter(_.subgraph.isEmpty) should contain only EntityPatchOp(diff_Q1_R3_R4, diff(Q1_R3, Q1_R4), None)
    result.filter(_.subgraph.contains(subgraphThreeUri)) should contain only EntityPatchOp(diff_Q1_R3_R4, diff(Q1_R3, Q1_R4), Some(subgraphThreeUri))

    // we delete delete the stubs and import Q1
    result.filter(_.subgraph.contains(subgraphOneUri)).toList should contain theSameElementsInOrderAs Seq(
      // we re-use DeleteOp to cleanup the stubs so that there's no need to do need any book keeping there
      DeleteOp(diff_Q1_R3_R4, Some(subgraphOneUri)),
      EntityPatchOp(diff_Q1_R3_R4, imp(Q1_R4), Some(subgraphOneUri)))
    // we delete Q1 from graph2 and insert the stubs pointing to graph1 and graph3
    result.filter(_.subgraph.contains(subgraphTwoUri)).toList should contain theSameElementsInOrderAs Seq(
      DeleteOp(diff_Q1_R3_R4, Some(subgraphTwoUri)),
      EntityPatchOp(diff_Q1_R3_R4, addStubs(stubs("Q1", subgraphOneUri) ++ stubs("Q1", subgraphThreeUri)), Some(subgraphTwoUri))
    )
  }

  "SubgraphDiff" should "diff an entity with a four graphs definitions and moving between subgraphs but stays out of graph2" in {
    // Q1 R4 is in subgraph1 and 3, R5 forces it to leave graph3
    val result = fourGraphsDiff.diff(diff_Q1_R4_R5, Q1_R4, Q1_R5)
    result.size shouldBe 6
    result.filter(_.subgraph.isEmpty) should contain only EntityPatchOp(diff_Q1_R4_R5, diff(Q1_R4, Q1_R5), None)
    // Q1 stays in graph1 so we diff
    result.filter(_.subgraph.contains(subgraphOneUri)) should contain only EntityPatchOp(diff_Q1_R4_R5, diff(Q1_R4, Q1_R5), Some(subgraphOneUri))

    // Q1 stays out of graph2 but stubs have changed so we clean them up first
    result.filter(_.subgraph.contains(subgraphTwoUri)).toList should contain theSameElementsInOrderAs Seq(
      // we re-use DeleteOp to cleanup the stubs so that there's no need to do need any book keeping there
      DeleteOp(diff_Q1_R4_R5, Some(subgraphTwoUri)),
      EntityPatchOp(diff_Q1_R4_R5, addStubs(stubs("Q1", subgraphOneUri)), Some(subgraphTwoUri)))
    // we delete Q1 from graph3 and insert the stubs pointing to graph1
    result.filter(_.subgraph.contains(subgraphThreeUri)).toList should contain theSameElementsInOrderAs Seq(
      DeleteOp(diff_Q1_R4_R5, Some(subgraphThreeUri)),
      EntityPatchOp(diff_Q1_R4_R5, addStubs(stubs("Q1", subgraphOneUri)), Some(subgraphThreeUri))
    )
  }

  "SubgraphDiff" should "diff an entity with a four graphs definitions and mixed stub source enters only graph3" in {
    // Q1 R5 is in subgraph1 and 3, R5 forces it to leave graph1
    val result = fourGraphsDiffMixedStubSource.diff(diff_Q1_R5_R6, Q1_R5, Q1_R6)
    result.size shouldBe 5
    result.filter(_.subgraph.isEmpty) should contain only EntityPatchOp(diff_Q1_R5_R6, diff(Q1_R5, Q1_R6), None)
    // Q1 is removed from graph1 without stubs added
    result.filter(_.subgraph.contains(subgraphOneUri)).toList should contain only DeleteOp(diff_Q1_R5_R6, Some(subgraphOneUri))
    // Q1 has no longer stubs from graph2 -> graph1 so we delete
    result.filter(_.subgraph.contains(subgraphTwoUri)).toList should contain only DeleteOp(diff_Q1_R5_R6, Some(subgraphTwoUri))
    // Q1 enters subgraph3
    result.filter(_.subgraph.contains(subgraphThreeUri)).toList should contain theSameElementsInOrderAs Seq(
      // reuse delete op to cleanup the stubs
      DeleteOp(diff_Q1_R5_R6, Some(subgraphThreeUri)),
      EntityPatchOp(diff_Q1_R5_R6, imp(Q1_R6), Some(subgraphThreeUri)))
  }

  "SubgraphDiff" should "import an entity with four graphs definitions and mixed stub sources" in {
    // Q1 R6 is only in subgraph3
    val result = fourGraphsDiffMixedStubSource.fullImport(import_Q1_R6, Q1_R6)
    result.size shouldBe 2
    result.filter(_.subgraph.isEmpty) should contain only EntityPatchOp(import_Q1_R6, imp(Q1_R6), None)
    result.filter(_.subgraph.contains(subgraphThreeUri)) should contain only EntityPatchOp(import_Q1_R6, imp(Q1_R6), Some(subgraphThreeUri))
    // subgraph3 is not a source for stubs so we have no events with stubs for subgraph1 & subgraph2
  }

  "SubgraphDiff" should "diff an entity with four graphs definitions and mixed stub source and partially moving between subgraphs" in {
    // Q1 R4 is in subgraph1 and 3, R5 forces it to leave graph3
    val result = fourGraphsDiffMixedStubSource.diff(diff_Q1_R4_R5, Q1_R4, Q1_R5)
    result.size shouldBe 5
    result.filter(_.subgraph.isEmpty) should contain only EntityPatchOp(diff_Q1_R4_R5, diff(Q1_R4, Q1_R5), None)
    result.filter(_.subgraph.contains(subgraphOneUri)) should contain only EntityPatchOp(diff_Q1_R4_R5, diff(Q1_R4, Q1_R5), Some(subgraphOneUri))
    // Q1 is removed from graph3 and have the stubs to graph1 added
    result.filter(_.subgraph.contains(subgraphThreeUri)).toList should contain theSameElementsInOrderAs Seq(
      DeleteOp(diff_Q1_R4_R5, Some(subgraphThreeUri)),
      EntityPatchOp(diff_Q1_R4_R5, addStubs(stubs("Q1", subgraphOneUri)), Some(subgraphThreeUri))
    )

    // Q1 stays out of graph2
    result.filter(_.subgraph.contains(subgraphTwoUri)) should contain only
      EntityPatchOp(diff_Q1_R4_R5, addStubs(stubs("Q1", subgraphOneUri)), Some(subgraphTwoUri))
  }

  "SubgraphDiff" should "diff an entity with four graphs definitions and mixed stub source staying in graph3" in {
    // Q1 R6 is in subgraph3, R7 does nothing
    val result = fourGraphsDiffMixedStubSource.diff(diff_Q1_R6_R7, Q1_R6, Q1_R7)
    result.size shouldBe 2
    result.filter(_.subgraph.isEmpty) should contain only EntityPatchOp(diff_Q1_R6_R7, diff(Q1_R6, Q1_R7), None)
    // Q1 stays in graph3
    result.filter(_.subgraph.contains(subgraphThreeUri)) should contain only EntityPatchOp(diff_Q1_R6_R7, diff(Q1_R6, Q1_R7), Some(subgraphThreeUri))
    // nothing happens on graph1 and graph2 since graph3 is not a source for stubs
  }

  "SubgraphDiff" should "reconcile an entity with four graphs definitions and mixed stub source belonging to one graph allowing stubs" in {
    // Q1 R4 is in subgraph1 and 3
    val result = fourGraphsDiffMixedStubSource.reconcile(reconcile_Q1_R4, Q1_R4)

    result.size shouldBe 4
    result.filter(_.subgraph.isEmpty) should contain only ReconcileOp(reconcile_Q1_R4, imp(Q1_R4), None)
    result.filter(_.subgraph.contains(subgraphOneUri)) should contain only ReconcileOp(reconcile_Q1_R4, imp(Q1_R4), Some(subgraphOneUri))
    result.filter(_.subgraph.contains(subgraphThreeUri)) should contain only ReconcileOp(reconcile_Q1_R4, imp(Q1_R4), Some(subgraphThreeUri))
    // graph3 is not a stub source and thus not adding any related stubs to graph3 for Q1
    result.filter(_.subgraph.contains(subgraphTwoUri)).toList should contain only
      ReconcileOp(reconcile_Q1_R4, addStubs(stubs("Q1", subgraphOneUri)), Some(subgraphTwoUri))
  }

  "SubgraphDiff" should "reconcile an entity with four graphs definitions and mixed stub source belonging to no graph allowing stubs" in {
    // Q1 R6 is in subgraph 3
    val result = fourGraphsDiffMixedStubSource.reconcile(reconcile_Q1_R6, Q1_R6)

    result.size shouldBe 4
    result.filter(_.subgraph.isEmpty) should contain only ReconcileOp(reconcile_Q1_R6, imp(Q1_R6), None)
    // We delete the entity from graph1 & 2 without adding stubs
    result.filter(_.subgraph.contains(subgraphOneUri)) should contain only DeleteOp(reconcile_Q1_R6, Some(subgraphOneUri))
    result.filter(_.subgraph.contains(subgraphTwoUri)) should contain only DeleteOp(reconcile_Q1_R6, Some(subgraphTwoUri))
    // We reconcile on graph3
    result.filter(_.subgraph.contains(subgraphThreeUri)) should contain only ReconcileOp(reconcile_Q1_R6, imp(Q1_R6), Some(subgraphThreeUri))
  }

  private def wdt(entity: String, prop: String, value: String) = {
    statement(uriScheme.entityIdToURI(entity), uriScheme.property(PropertyType.DIRECT) + prop, uriScheme.entityIdToURI(value))
  }

  private def rev(entity: String, rev: Long): Statement = {
    statement(uriScheme.entityIdToURI(entity), SchemaDotOrg.VERSION, rev)
  }

  private def propWithRef(entity: String, prop: String, value: String, refProp: String, refValue: String): Iterable[Statement] = {
    val stmt = uriScheme.statement() + entity + "-" + UUID.randomUUID().toString
    val ref = uriScheme.reference() + UUID.randomUUID().toString
    Seq(
      statement(uriScheme.entityIdToURI(entity), uriScheme.property(PropertyType.CLAIM) + prop, stmt),
      statement(stmt, uriScheme.property(PropertyType.STATEMENT), value),
      statement(stmt, Provenance.WAS_DERIVED_FROM, ref),
      statement(ref, uriScheme.property(PropertyType.REFERENCE) + refProp, refValue))
  }

  private def diff(from: Iterable[Statement], to: Iterable[Statement]): Patch = {
    entityDiff.diff(from.asJava, to.asJava)
  }

  private def imp(data: Iterable[Statement]): Patch = {
    entityDiff.diff(Collections.emptyList(), data.asJava)
  }

  private def addStubs(data: Iterable[Statement]): Patch = {
    // Hack: we want stubs to added via linkedSharedElements so that they do not cause divergences to increase
    // Current strategy is to always send stubs even when the entity does not move between graphs
    new Patch(Collections.emptyList(), data.toList.asJava, Collections.emptyList(), Collections.emptyList())
  }

  private def stubs(entity: String, toSubgraph: model.URI): Iterable[Statement] = {
    Seq(StatementHelper.statement(uriScheme.entityIdToURI(entity), Ontology.QueryService.SUBGRAPH, toSubgraph))
  }
}
