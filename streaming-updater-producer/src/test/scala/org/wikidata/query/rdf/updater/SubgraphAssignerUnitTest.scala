package org.wikidata.query.rdf.updater

import org.openrdf.model.impl.ValueFactoryImpl
import org.scalatest.{FlatSpec, Matchers}
import org.wikidata.query.rdf.tool.subgraph.{SubgraphDefinitions, SubgraphDefinitionsParser}
import org.wikidata.query.rdf.updater.SubgraphAssignerUnitTest.{assigner, definitions, factory}

object SubgraphAssignerUnitTest {
  val factory = new ValueFactoryImpl()
  val definitions: SubgraphDefinitions = SubgraphDefinitionsParser.parseYaml(
    getClass.getResource("/subgraph-definitions.yaml").openStream()
  )
  val assigner = new SubgraphAssigner(definitions)
}

class SubgraphAssignerUnitTest extends FlatSpec with Matchers {

  "SubgraphRuleMatcher" should "do its job" in {
    var result = assigner.assign(List(factory.createStatement(
      factory.createURI(definitions.getPrefixes.get("nsb"), "Q2013"),
      factory.createURI(definitions.getPrefixes.get("nsa"), "PROP_X"),
      factory.createURI(definitions.getPrefixes.get("nsb"), "Q115471117")
    )))

    result.map(definition => definition.getName) should contain("subgraph0")
    result.map(definition => definition.getName) should contain("subgraph_")

    result = assigner.assign(List(factory.createStatement(
      factory.createURI(definitions.getPrefixes.get("nsb"), "Q18507561"),
      factory.createURI(definitions.getPrefixes.get("nsa"), "PROP_X"),
      factory.createURI(definitions.getPrefixes.get("nsb"), "PROP_VAL"))))

    result.map(definition => definition.getName) should contain("subgraph1")
    result.map(definition => definition.getName) should contain("subgraph_")

    result = assigner.assign(List(factory.createStatement(
      factory.createURI(definitions.getPrefixes.get("nsb"), "QANY"),
      factory.createURI(definitions.getPrefixes.get("nsc"), "TYPE_X"),
      factory.createURI(definitions.getPrefixes.get("nsd"), "Property"))))

    result.map(definition => definition.getName) should contain("subgraph1")
    result.map(definition => definition.getName) should contain("subgraph_")
  }

}


