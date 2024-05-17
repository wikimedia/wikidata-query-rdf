package org.wikidata.query.rdf.spark.transform.structureddata.dumps

import org.apache.spark.api.java.function.FilterFunction
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.openrdf.model.{BNode, Resource, Value}
import org.wikidata.query.rdf.common.uri.{Ontology, UrisScheme}
import org.wikidata.query.rdf.tool.subgraph.SubgraphRule.{Outcome, TriplePattern}
import org.wikidata.query.rdf.tool.subgraph.{SubgraphDefinition, SubgraphDefinitions, SubgraphRule}

import java.util
import scala.collection.JavaConverters._
import scala.language.postfixOps

@SuppressWarnings(Array("scala:S117"))
case class Entity(entity_uri: String)
@SuppressWarnings(Array("scala:S117"))
case class MappedRules(entity_uri: String, matched_rules: Array[String])

class SubgraphRuleMapper(urisScheme: UrisScheme, subgraphDefinition: SubgraphDefinitions, subgraphNames: List[String]) {
  private val ENTITY_URI_FIELD = "entity_uri"
  private val MATCHED_RULLES_FIELD = "matched_rules"

  val subgraphs: List[SubgraphDefinition] = subgraphNames.map(subgraphDefinition.getDefinitionByName)
  private val statementEncoder: StatementEncoder = new StatementEncoder()

  /** Build a list of (subgraphName, ruleIndex, ruleDefinition). The ruleIndex is relative to the graph. */
  private val rules: List[(String, Int, SubgraphRule)] = subgraphs map {
    s => (s.getName, s.getRules.asScala)
  } flatMap {
    case (name, rules) =>
      rules.zipWithIndex map {
        case (rule, idx) => (name, idx, rule)
      }
  }

  implicit private val entityEncoder: Encoder[Entity] = Encoders.product[Entity]
  implicit private val mappedRulesEncoder: Encoder[MappedRules] = Encoders.product[MappedRules]

  /**
   * Map subgraph to their corresponding entities by applying the set of rules declared in their respective definition.
   */
  def mapSubgraphs(baseTable: DataFrame): Map[SubgraphDefinition, Dataset[Entity]] = {
    val entityUrisFilter: Column = urisScheme.entityURIs().asScala map {
      uriPrefix => baseTable("context").startsWith("<" + uriPrefix)
    } reduce {
      _ or _
    }

    val entityUris: Dataset[Entity] = baseTable
      .filter(entityUrisFilter)
      .select(baseTable("context").as(ENTITY_URI_FIELD))
      .dropDuplicates()
      .as[Entity]
      .cache()

    val appliedRules: Dataset[MappedRules] = mapRules(baseTable).cache()
    val allEntities = entityUris
      .withColumn(MATCHED_RULLES_FIELD, lit(Array.empty[String]))
      .join(appliedRules, entityUris(ENTITY_URI_FIELD) === appliedRules(ENTITY_URI_FIELD), "left_anti")
      .select(ENTITY_URI_FIELD, MATCHED_RULLES_FIELD)
      .as[MappedRules]
      .union(appliedRules)

    subgraphs map { subgraph =>
      subgraph -> allEntities.filter(filterEntities(subgraph)).select(ENTITY_URI_FIELD).as[Entity]
    } toMap
  }

  private def filterEntities(subgraph: SubgraphDefinition): FilterFunction[MappedRules] = {
    val subgraphRules: List[(String, Outcome)] = rules filter {
      case (name, _, _) => subgraph.getName.equals(name)
    } map {
      case (name, idx, r) => (ruleName(name, idx), r.getOutcome)
    }
    (t: MappedRules) => {
      subgraphRules find {
        case (r, _) => t.matched_rules.contains(r)
      } map {
        case (_, outcome) => outcome == Outcome.pass
      } getOrElse (subgraph.getRuleDefault == Outcome.pass)
    }
  }

  def buildStubs(mappedSubgraphs: Map[SubgraphDefinition, Dataset[Entity]]): Map[SubgraphDefinition, Option[DataFrame]] = {
    mappedSubgraphs map { case (dest, entities) =>
      val stubsDf = mappedSubgraphs filter {
        case (source, _) => source.isStubsSource && !source.equals(dest)
      } map { case (source, sourceEntities) =>
        sourceEntities
          .withColumn("context", sourceEntities("entity_uri"))
          .withColumn("subject", sourceEntities("entity_uri"))
          .withColumn("predicate", lit(statementEncoder.encodeURI(Ontology.QueryService.SUBGRAPH)))
          .withColumn("object", lit(statementEncoder.encode(source.getSubgraphUri)))
          .drop("entity_uri")
      } reduceOption {
        _ union _
      } map (stubs => stubs.join(entities, entities("entity_uri") === stubs("context"), "left_anti"))
      dest -> stubsDf
    }
  }

  /**
   * Map rules to their corresponding entities.
   */
  private def mapRules(baseTable: DataFrame): Dataset[MappedRules] = {
    // build a chained "or" filters with all the rules
    val filteredTable = rules map {
      case (_, _, rule) => buildFilter(baseTable.apply, rule)
    } reduceOption {
      _ or _
    } map {
      baseTable.filter
    } getOrElse baseTable

    // reduce by grouping by entities and merge the array of matched rules
    addRuleColumnAndGroupByEntity(filteredTable, rules)
  }

  /**
   * Encode a ruleName as a string: "$subgraphName[$ruleIndex]".
   */
  private def ruleName(subgraphName: String, idx: Int): String = {
    s"$subgraphName[$idx]"
  }

  /**
   * Add an array column named matched_rules containing the list of rules (encoded using ruleName) that matched.
   * Then group by entity merging the matched_rules array.
   * The resulting dataset has a shape corresponding to MappedRules which is the list of entity URIs and the list of rules
   * that matched for this entity.
   */
  private def addRuleColumnAndGroupByEntity(dataFrame: DataFrame, rules: List[(String, Int, SubgraphRule)]): Dataset[MappedRules] = {
    var df = dataFrame
      .withColumn(MATCHED_RULLES_FIELD, lit(Array[String]()))

    rules foreach {
      case (name, idx, rule) =>
        val filter = buildFilter(dataFrame.apply, rule)
        val ruleId = ruleName(name, idx)
        // if filter matches
        //    return matched_rules + [ruleId]
        // else
        //    return matched_rules
        val appendRulesToMatchedRuleField = when(filter, array_union(df(MATCHED_RULLES_FIELD), lit(Array(ruleId)))).otherwise(df(MATCHED_RULLES_FIELD))
        df = df.withColumn(MATCHED_RULLES_FIELD, appendRulesToMatchedRuleField)
    }

    val flattenDistinct = array_distinct _ compose flatten
    df.withColumn(ENTITY_URI_FIELD, df("context"))
      .drop("context", "subject", "predicate", "object")
      .as[MappedRules]
      .groupBy(ENTITY_URI_FIELD)
      .agg(flattenDistinct(collect_list(MATCHED_RULLES_FIELD)).alias(MATCHED_RULLES_FIELD))
      .as[MappedRules]
  }

  /**
   * Build a filter from a subgraph rule.
   */
  private def buildFilter(colSupplier: String => Column, subgraphRule: SubgraphRule): Column = {
    filterFromTriplePattern(colSupplier, subgraphRule.getPattern)
  }

  /**
   * Build a filter from a triple pattern.
   */
  private def filterFromTriplePattern(colSupplier: String => Column, triplePattern: TriplePattern): Column = {
    filterFromValue(colSupplier("context"), colSupplier("subject"), triplePattern.getSubject, triplePattern.getBindings)
      .and(filterFromValue(colSupplier("context"), colSupplier("predicate"), triplePattern.getPredicate, triplePattern.getBindings))
      .and(filterFromValue(colSupplier("context"), colSupplier("object"), triplePattern.getObject, triplePattern.getBindings))
  }

  /**
   * Filter a rdf resource or literal based on the splitting rules convention.
   * - BNode("?entity"): matches the current entity
   * - BNone("wildcard"): matches any literal or resource
   * - anything else: matches the N3 representation using statementEncoder
   */
  private def filterFromValue(entityColumn: Column, col: Column, value: Value, bindings: util.Map[String, util.Collection[Resource]]): Column = {
    value match {
      case bnode: BNode =>
        if (bnode.getID.equals(SubgraphRule.TriplePattern.ENTITY_BINDING_NAME)) {
          col === entityColumn
        } else if (bnode.getID.equals(TriplePattern.WILDCARD_BNODE_LABEL)) {
          lit(true)
        } else {
          val bindingValues = bindings.get(bnode.getID)
          if (bindingValues == null) {
            throw new UnsupportedOperationException("Unknown binding: " + bnode.getID)
          }
          col.isInCollection(bindingValues.asScala.map(statementEncoder.encode(_)))
        }
      case _ =>
        col === lit(statementEncoder.encode(value))
    }
  }
}
