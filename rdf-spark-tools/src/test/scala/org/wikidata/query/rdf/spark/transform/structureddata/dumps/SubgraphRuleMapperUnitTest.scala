package org.wikidata.query.rdf.spark.transform.structureddata.dumps

import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.openrdf.model.Resource
import org.openrdf.model.impl.ValueFactoryImpl
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.wikidata.query.rdf.common.uri.{Ontology, UrisSchemeFactory}
import org.wikidata.query.rdf.spark.SparkSessionProvider
import org.wikidata.query.rdf.tool.subgraph.SubgraphRule.Outcome
import org.wikidata.query.rdf.tool.subgraph.SubgraphRule.TriplePattern.parseTriplePattern
import org.wikidata.query.rdf.tool.subgraph.{SubgraphDefinition, SubgraphDefinitions, SubgraphRule}

import java.util
import scala.collection.JavaConverters._

class SubgraphRuleMapperUnitTest extends AnyFlatSpec with SparkSessionProvider with Matchers{
  private val subgraphPred = "<" + Ontology.QueryService.SUBGRAPH + ">"
  private val scheme = UrisSchemeFactory.forWikidataHost("ut")
  private val prefixes = Map("e" -> scheme.entityURIs().iterator().next()).asJava
  private val valueFactory = new ValueFactoryImpl()
  private val bindings: util.Map[String, util.Collection[Resource]] = Map(
    "e100e200" -> Seq(valueFactory.createURI(scheme.entityIdToURI("E100")), valueFactory.createURI(scheme.entityIdToURI("E200"))
    ).asJava.asInstanceOf[util.Collection[Resource]]).asJava

  private val pass_P1_E100_rule = new SubgraphRule(Outcome.pass, parseTriplePattern(s"?entity <pred:P1> e:E100",
    valueFactory, prefixes, bindings))
  private val pass_P1_E200_rule = new SubgraphRule(Outcome.pass, parseTriplePattern(s"?entity <pred:P1> e:E200",
    valueFactory, prefixes, bindings))
  private val pass_P1_E100_E200_rule = new SubgraphRule(Outcome.pass, parseTriplePattern(s"?entity <pred:P1> ?e100e200",
    valueFactory, prefixes, bindings))
  private val block_P1_E100_rule = new SubgraphRule(Outcome.block, parseTriplePattern(s"?entity <pred:P1> e:E100",
    valueFactory, prefixes, bindings))
  private val pass_all_rule = new SubgraphRule(Outcome.pass, parseTriplePattern(s"?entity <pred:P1> []",
    valueFactory, prefixes, bindings))
  private val block_P1_P300_rule = new SubgraphRule(Outcome.block, parseTriplePattern(s"?entity <pred:P1> e:E300",
    valueFactory, prefixes, bindings))

  private val pass_P1_E100_default_block = SubgraphDefinition.builder
    .name("pass_P1_E100_default_block").stream("unused")
    .rules(List(pass_P1_E100_rule).asJava).ruleDefault(Outcome.block)
    .stubsSource(false).build()
  private val pass_P1_E100_E200_default_block = SubgraphDefinition.builder()
    .name("pass_P1_E100_E200_default_block").stream("unused")
    .rules(List(pass_P1_E100_E200_rule).asJava).ruleDefault(Outcome.block)
    .stubsSource(false).build()
  private val block_P1_E100_default_pass = SubgraphDefinition.builder()
    .name("block_P1_E100_default_pass").stream("unused")
    .rules(List(block_P1_E100_rule).asJava).ruleDefault(Outcome.pass)
    .stubsSource(false).build()
  private val block_P1_E100_then_pass_all_default_block = SubgraphDefinition.builder()
    .name("block_P1_E100_then_pass_all_default_block").stream("unused")
    .rules(List(block_P1_E100_rule, pass_all_rule).asJava).ruleDefault(Outcome.block)
    .stubsSource(false).build()
  private val pass_all_pass_P1_E100_rule = SubgraphDefinition.builder()
    .name("pass_all_pass_P1_E100_rule").stream("unused")
    .rules(List(pass_P1_E100_rule).asJava).ruleDefault(Outcome.pass)
    .stubsSource(false).build()
  private val block_all_block_P1_E100_rule = SubgraphDefinition.builder()
    .name("block_all_block_P1_E100_rule").stream("unused")
    .rules(List(block_P1_E100_rule).asJava).ruleDefault(Outcome.block)
    .stubsSource(false).build()

  private val SCHEMA = StructType(Array(
    StructField("context", StringType),
    StructField("subject", StringType),
    StructField("predicate", StringType),
    StructField("object", StringType)
  ))

  private def entityUri(entityId: String): String = "<" + scheme.entityIdToURI(entityId) + ">"

  private def dataset(): DataFrame = {
    spark.createDataFrame(List[Row](
      Row(entityUri("E1"), entityUri("E1"), "<pred:P1>", entityUri("E100")),
      Row(entityUri("E1"), "<random:triple>", "<random:triple>", "<random:triple>"),
      Row(entityUri("E2"), entityUri("E2"), "<pred:P1>", entityUri("E200")),
      Row(entityUri("E2"), "<random:triple>", "<random:triple>", "<random:triple>"),
      Row(entityUri("E3"), entityUri("E3"), "<pred:P1>", entityUri("E300")),
      Row(entityUri("E3"), "<random:triple>", "<random:triple>", "<random:triple>"),
      Row(entityUri("E4"), entityUri("E4"), "<pred:P1>", entityUri("E400")),
      Row(entityUri("E4"), "<random:triple>", "<random:triple>", "<random:triple>")
    ).asJava, SCHEMA)
  }

  "SubgraphRuleMapper" should "extract correct entities using pass rules and default block outcome" in {
    val definitions = new SubgraphDefinitions(List(pass_P1_E100_default_block).asJava)
    val mapper = new SubgraphRuleMapper(scheme, definitions, List(pass_P1_E100_default_block.getName))
    val mappedSubgraphs = mapper.mapSubgraphs(dataset())
    mappedSubgraphs.size shouldBe 1
    val mappedSubgraph = mappedSubgraphs(pass_P1_E100_default_block)
    mappedSubgraph.collect() should contain only Entity(entityUri("E1"))
  }

  "SubgraphRuleMapper" should "extract correct entities using 2 pass rules and default block outcome" in {
    val definitions = new SubgraphDefinitions(List(pass_P1_E100_E200_default_block).asJava)
    val mapper = new SubgraphRuleMapper(scheme, definitions, List(pass_P1_E100_E200_default_block.getName))
    val mappedSubgraphs = mapper.mapSubgraphs(dataset())
    mappedSubgraphs.size shouldBe 1
    val mappedSubgraph = mappedSubgraphs(pass_P1_E100_E200_default_block)
    mappedSubgraph.collect() should contain only (Entity(entityUri("E1")), Entity(entityUri("E2")))
  }

  "SubgraphRuleMapper" should "extract correct entities using block rules and default pass outcome" in {
    val definitions = new SubgraphDefinitions(List(block_P1_E100_default_pass).asJava)
    val mapper = new SubgraphRuleMapper(scheme, definitions, List(block_P1_E100_default_pass.getName))
    val mappedSubgraphs = mapper.mapSubgraphs(dataset())
    mappedSubgraphs.size shouldBe 1
    val mappedSubgraph = mappedSubgraphs(block_P1_E100_default_pass)
    mappedSubgraph.collect() should contain only (Entity(entityUri("E2")), Entity(entityUri("E3")), Entity(entityUri("E4")))
  }

  "SubgraphRuleMapper" should "extract correct entities using pass all and a block rule and default block outcome" in {
    val definitions = new SubgraphDefinitions(List(block_P1_E100_then_pass_all_default_block).asJava)
    val mapper = new SubgraphRuleMapper(scheme, definitions, List(block_P1_E100_then_pass_all_default_block.getName))
    val mappedSubgraphs = mapper.mapSubgraphs(dataset())
    mappedSubgraphs.size shouldBe 1
    val mappedSubgraph = mappedSubgraphs(block_P1_E100_then_pass_all_default_block)
    mappedSubgraph.collect() should contain only (Entity(entityUri("E2")), Entity(entityUri("E3")), Entity(entityUri("E4")))
  }

  "SubgraphRuleMapper" should "extract all entities using any pass all and a default pass outcome" in {
    val definitions = new SubgraphDefinitions(List(pass_all_pass_P1_E100_rule).asJava)
    val mapper = new SubgraphRuleMapper(scheme, definitions, List(pass_all_pass_P1_E100_rule.getName))
    val mappedSubgraphs = mapper.mapSubgraphs(dataset())
    mappedSubgraphs.size shouldBe 1
    val mappedSubgraph = mappedSubgraphs(pass_all_pass_P1_E100_rule)
    mappedSubgraph.collect() should contain only (Entity(entityUri("E1")), Entity(entityUri("E2")), Entity(entityUri("E3")), Entity(entityUri("E4")))
  }

  "SubgraphRuleMapper" should "extract all entities using any block all and a default block outcome" in {
    val definitions = new SubgraphDefinitions(List(block_all_block_P1_E100_rule).asJava)
    val mapper = new SubgraphRuleMapper(scheme, definitions, List(block_all_block_P1_E100_rule.getName))
    val mappedSubgraphs = mapper.mapSubgraphs(dataset())
    mappedSubgraphs.size shouldBe 1
    val mappedSubgraph = mappedSubgraphs(block_all_block_P1_E100_rule)
    mappedSubgraph.collect() shouldBe empty
  }

  "SubgraphRuleMapper" should "be able to map multiple subgraphs" in {
    val definitions = new SubgraphDefinitions(List(pass_P1_E100_E200_default_block, block_P1_E100_default_pass).asJava)
    val mapper = new SubgraphRuleMapper(scheme, definitions, List(pass_P1_E100_E200_default_block.getName, block_P1_E100_default_pass.getName))
    val mappedSubgraphs = mapper.mapSubgraphs(dataset())
    mappedSubgraphs.size shouldBe 2
    val mappedSubgraph1 = mappedSubgraphs(pass_P1_E100_E200_default_block)
    mappedSubgraph1.collect() should contain only (Entity(entityUri("E1")), Entity(entityUri("E2")))

    val mappedSubgraph2 = mappedSubgraphs(block_P1_E100_default_pass)
    mappedSubgraph2.collect() should contain only (Entity(entityUri("E2")), Entity(entityUri("E3")), Entity(entityUri("E4")))
  }

  private def graphDefinitionsForStubsTest(withStubs: Boolean) = {
    // A has [E1,E2] (E3 -> [C], E4 -> [B,C])
    // B has [E1,E2,E4] (E3 -> [C])
    // C has [E2,E3,E4] (E1 ->[A,B])
    val definitions = new SubgraphDefinitions(List(
      SubgraphDefinition.builder()
        .name("A").subgraphUri(valueFactory.createURI("subgraph:A")).stream("unused")
        .rules(List(pass_P1_E100_rule, pass_P1_E200_rule).asJava).ruleDefault(Outcome.block).
        stubsSource(withStubs).build(),
      SubgraphDefinition.builder()
        .name("B").subgraphUri(valueFactory.createURI("subgraph:B")).stream("unused")
        .rules(List(block_P1_P300_rule).asJava).ruleDefault(Outcome.pass)
        .stubsSource(withStubs).build(),
      SubgraphDefinition.builder()
        .name("C").subgraphUri(valueFactory.createURI("subgraph:C")).stream("unused")
        .rules(List(block_P1_E100_rule).asJava).ruleDefault(Outcome.pass)
        .stubsSource(withStubs).build()
    ).asJava)
    definitions
  }

  private def extractStubs(stubs: Option[DataFrame]): DataFrame = {
    stubs.get.filter(col("predicate") === lit("<" + Ontology.QueryService.SUBGRAPH + ">"))
  }

  "SubgraphRuleMapper" should "generate stubs when required" in {
    val definitions: SubgraphDefinitions = graphDefinitionsForStubsTest(true)
    val mapper = new SubgraphRuleMapper(scheme, definitions, List("A", "B", "C"))
    val mappedSubgraphs = mapper.mapSubgraphs(dataset())
    val stubs = mapper.buildStubs(mappedSubgraphs)
    stubs.size shouldBe 3
    val stubsGraphAtoBC = stubs(definitions.getDefinitionByName("A"))
    val stubsGraphBtoAC = stubs(definitions.getDefinitionByName("B"))
    val stubsGraphCtoAB = stubs(definitions.getDefinitionByName("C"))
    stubsGraphAtoBC should not be None
    stubsGraphBtoAC should not be None
    stubsGraphCtoAB should not be None

    extractStubs(stubsGraphAtoBC).collect() should contain only (
      Row(entityUri("E4"), entityUri("E4"), subgraphPred, "<subgraph:B>"),
      Row(entityUri("E4"), entityUri("E4"), subgraphPred, "<subgraph:C>"),
      Row(entityUri("E3"), entityUri("E3"), subgraphPred, "<subgraph:C>")
    )
    extractStubs(stubsGraphBtoAC).collect() should contain only Row(entityUri("E3"), entityUri("E3"), subgraphPred, "<subgraph:C>")
    extractStubs(stubsGraphCtoAB).collect() should contain only (
      Row(entityUri("E1"), entityUri("E1"), subgraphPred, "<subgraph:A>"),
      Row(entityUri("E1"), entityUri("E1"), subgraphPred, "<subgraph:B>")
    )
  }

  "SubgraphRuleMapper" should "not generate stubs when not required" in {
    val definitions: SubgraphDefinitions = graphDefinitionsForStubsTest(false)
    val mapper = new SubgraphRuleMapper(scheme, definitions, List("A", "B", "C"))
    val mappedSubgraphs = mapper.mapSubgraphs(dataset())
    val stubs = mapper.buildStubs(mappedSubgraphs)
    stubs.size shouldBe 3
    stubs foreach { case (_, stubs) =>
      stubs shouldBe None
    }
  }
}
