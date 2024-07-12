package org.wikidata.query.rdf.updater

import org.openrdf.model._
import org.wikidata.query.rdf.tool.subgraph.SubgraphRule.TriplePattern
import org.wikidata.query.rdf.tool.subgraph.{SubgraphDefinition, SubgraphDefinitions, SubgraphRule}
import org.wikidata.query.rdf.updater.SubgraphAssigner.isDistinct

import java.util
import java.util.Collections.singletonList
import scala.collection.JavaConverters._

class SubgraphAssigner(subgraphDefinitions: SubgraphDefinitions) extends Serializable {

  final val distinctSubgraphs: Iterable[SubgraphDefinition] = subgraphDefinitions.getSubgraphs.asScala
    .filter(isDistinct)
    .distinct

  def distinctUnassignedSubgraphsFor(assignedSubgraphs: Iterable[SubgraphDefinition]): Iterable[SubgraphDefinition] = {
    distinctSubgraphs
      .filterNot(distinctSubgraph =>
        assignedSubgraphs.exists(assignedSubgraph =>
          assignedSubgraph.getSubgraphUri == distinctSubgraph.getSubgraphUri))
  }

  def distinctStubSourcesFor(assignedSubgraphs: Iterable[SubgraphDefinition]): Iterable[SubgraphDefinition] = {
    assignedSubgraphs.filter(SubgraphAssigner.isDistinctStubSource)
  }

  def stream(uri: URI): Option[String] = subgraphDefinitions.getSubgraphs.asScala
    .find(_.getSubgraphUri == uri)
    .map(_.getStream)

  /**
   * Identify all the subgraph that match the given the statements owned by the entity identified by entityUri.
   *
   * @param entityStatements the statements representing the data of the entity being matched
   * @param entityUri        the entity URI owning the statements being matched
   * @return the list of subgraphs matching this entity
   */
  def assign(entityStatements: Iterable[Statement], entityUri: URI): List[SubgraphDefinition] = {
    subgraphDefinitions.getSubgraphs.asScala.filter { definition =>
      val rules: List[SubgraphRule] = if (definition.getRules == null) List.empty else definition.getRules.asScala.toList
      // find first rule that matches any statement
      val outcome: SubgraphRule.Outcome = rules
        .find(rule => entityStatements.exists(statement => matches(rule, statement, entityUri)))
        .map(rule => rule.getOutcome)
        .getOrElse(definition.getRuleDefault)

      outcome == SubgraphRule.Outcome.pass
    }.toList
  }

  private def matches(rule: SubgraphRule, statement: Statement, entityUri: URI): Boolean = {
    implicit val bindings = rule.getPattern.getBindings
    val subjectMatches = matches(rule.getPattern.getSubject, statement.getSubject, entityUri)
    val predicateMatches = matches(rule.getPattern.getPredicate, statement.getPredicate, entityUri)
    val objectMatches = matches(rule.getPattern.getObject, statement.getObject, entityUri)

    subjectMatches && predicateMatches && objectMatches
  }

  /**
   * Tests if <pre>ruleValue</pre> matches <pre>statementValue</pre>.
   *
   * @param ruleValue      [[TriplePattern]] value (subject, predicate, or object)
   * @param statementValue aligned [[Statement]] value (subject, predicate, or object)
   * @param bindings       defaults to [[SubgraphAssigner.bindings]] unless the calling scope declares an implicit override
   * @param entityUri      the entity URI used to bind TriplePattern.ENTITY_BINDING_NAME
   * @return
   */
  private def matches(ruleValue: Value, statementValue: Value, entityUri: URI)(implicit bindings: util.Map[String, util.Collection[Resource]]): Boolean = {
    ruleValue match {
      case n: BNode if n.getID == TriplePattern.ENTITY_BINDING_NAME => statementValue.equals(entityUri)
      case n: BNode if n.getID == TriplePattern.WILDCARD_BNODE_LABEL => true
      case n: BNode if bindings.containsKey(n.getID) => bindings.get(n.getID).contains(statementValue)
      case _ => ruleValue.equals(statementValue)
    }
  }
}

object SubgraphAssigner {
  def empty(stream: String = "default"): SubgraphAssigner = new SubgraphAssigner(
    new SubgraphDefinitions(singletonList(
      SubgraphDefinition.builder()
        .name(stream)
        .stream(stream)
        .ruleDefault(SubgraphRule.Outcome.pass)
        .stubsSource(false)
        .build()
    ))
  )

  def isDistinct(subgraphDefinition: SubgraphDefinition): Boolean = {
    subgraphDefinition.getSubgraphUri != null
  }

  def isDistinctStubSource(subgraphDefinition: SubgraphDefinition): Boolean = {
    isDistinct(subgraphDefinition) && subgraphDefinition.isStubsSource
  }
}
