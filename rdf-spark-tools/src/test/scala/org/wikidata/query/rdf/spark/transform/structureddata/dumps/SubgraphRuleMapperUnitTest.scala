package org.wikidata.query.rdf.spark.transform.structureddata.dumps

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.openrdf.model.impl.ValueFactoryImpl
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.wikidata.query.rdf.common.uri.UrisSchemeFactory
import org.wikidata.query.rdf.spark.SparkSessionProvider
import org.wikidata.query.rdf.tool.subgraph.SubgraphRule.{Outcome, TriplePattern}
import org.wikidata.query.rdf.tool.subgraph.{SubgraphDefinition, SubgraphDefinitions, SubgraphRule}

import scala.collection.JavaConverters._

class SubgraphRuleMapperUnitTest extends AnyFlatSpec with SparkSessionProvider with Matchers{
  private val scheme = UrisSchemeFactory.forWikidataHost("ut")
  private val prefixes = Map("e" -> scheme.entityURIs().iterator().next()).asJava
  private val valueFactory = new ValueFactoryImpl()

  private val pass_P1_E100_rule = new SubgraphRule(Outcome.pass, TriplePattern.parseTriplePattern(s"?entity <pred:P1> e:E100", valueFactory, prefixes))
  private val pass_P1_E200_rule = new SubgraphRule(Outcome.pass, TriplePattern.parseTriplePattern(s"?entity <pred:P1> e:E200", valueFactory, prefixes))
  private val block_P1_E100_rule = new SubgraphRule(Outcome.block, TriplePattern.parseTriplePattern(s"?entity <pred:P1> e:E100", valueFactory, prefixes))
  private val pass_all_rule = new SubgraphRule(Outcome.pass, TriplePattern.parseTriplePattern(s"?entity <pred:P1> []", valueFactory, prefixes))

  private val pass_P1_E100_default_block = new SubgraphDefinition("pass_P1_E100_default_block", Option.empty.orNull, "unused",
    List(pass_P1_E100_rule).asJava, Outcome.block, false)
  private val pass_P1_E100_E200_default_block = new SubgraphDefinition("pass_P1_E100_E200_default_block", Option.empty.orNull, "unused",
    List(pass_P1_E100_rule, pass_P1_E200_rule).asJava, Outcome.block, false)
  private val block_P1_E100_default_pass = new SubgraphDefinition("block_P1_E100_default_pass", Option.empty.orNull, "unused",
    List(block_P1_E100_rule).asJava, Outcome.pass, false)
  private val pass_all_block_P1_E100_default_block = new SubgraphDefinition("pass_all_block_P1_E100_default_block", Option.empty.orNull, "unused",
    List(pass_all_rule, block_P1_E100_rule).asJava, Outcome.block, false)

  private val pass_all_pass_P1_E100_rule = new SubgraphDefinition("pass_all_pass_P1_E100_rule", Option.empty.orNull, "unused",
    List(pass_P1_E100_rule).asJava, Outcome.pass, false)
  private val block_all_block_P1_E100_rule = new SubgraphDefinition("block_all_block_P1_E100_rule", Option.empty.orNull, "unused",
    List(block_P1_E100_rule).asJava, Outcome.block, false)

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
      Row(entityUri("E4"), entityUri("E4"), "<pred:P1>", entityUri("E300")),
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
    val definitions = new SubgraphDefinitions(List(pass_all_block_P1_E100_default_block).asJava)
    val mapper = new SubgraphRuleMapper(scheme, definitions, List(pass_all_block_P1_E100_default_block.getName))
    val mappedSubgraphs = mapper.mapSubgraphs(dataset())
    mappedSubgraphs.size shouldBe 1
    val mappedSubgraph = mappedSubgraphs(pass_all_block_P1_E100_default_block)
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
}
