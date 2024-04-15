package org.wikidata.query.rdf.spark.transform.structureddata.dumps

import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.openrdf.model.{BNode, Value}
import org.wikidata.query.rdf.common.uri.UrisScheme
import org.wikidata.query.rdf.tool.subgraph.SubgraphRule.{Outcome, TriplePattern}
import org.wikidata.query.rdf.tool.subgraph.{SubgraphDefinition, SubgraphDefinitions, SubgraphRule}

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

    val appliedRules: Dataset[MappedRules] = mapRules(baseTable)

    subgraphs map { subgraph =>
      val maybePassFilter = matchedRulesFilter(subgraph, appliedRules.apply, Outcome.pass)
      val maybeBlockFilter = matchedRulesFilter(subgraph, appliedRules.apply, Outcome.block)
      val filteredEntities = (maybePassFilter, maybeBlockFilter, subgraph.getRuleDefault) match {
        case (Some(passFilter), Some(blockFilter), Outcome.block) => passThenBlock(appliedRules, passFilter, blockFilter)
        case (Some(passFilter), None, Outcome.block) => filterEntities(appliedRules, passFilter)
        case (_, Some(blockFilter), Outcome.pass) => block(appliedRules, entityUris, blockFilter)
        case (None, _, Outcome.block) => baseTable.sparkSession.createDataFrame(Seq.empty[Entity]).as[Entity]
        case (_, None, Outcome.pass) => entityUris
      }
      subgraph -> filteredEntities
    } toMap
  }

  /**
   * Combination of pass and block rules.
   * First extract all entities that either one of the pass rules and then remove the entities matching a block rule
   */
  private def passThenBlock(appliedRules: Dataset[MappedRules], passFilter: Column, blockFilter: Column): Dataset[Entity] = {
    val passedEntities = filterEntities(appliedRules, passFilter)
    val blockedEntities = filterEntities(appliedRules, blockFilter)
    passedEntities
      .join(blockedEntities, blockedEntities(ENTITY_URI_FIELD) === passedEntities(ENTITY_URI_FIELD), "left_anti")
      .select(ENTITY_URI_FIELD).as[Entity]
  }

  /**
   * Remove all entities from allEntities that match the blockFilter.
   */
  private def block(appliedRules: Dataset[MappedRules], allEntities: Dataset[Entity], blockFilter: Column): Dataset[Entity] = {
    val blockedEntities = filterEntities(appliedRules, blockFilter)
    allEntities.join(blockedEntities, allEntities(ENTITY_URI_FIELD) === blockedEntities(ENTITY_URI_FIELD), "left_anti")
      .select(ENTITY_URI_FIELD).as[Entity]
  }

  /**
   * Include all entities that match the filter.
   */
  private def filterEntities(appliedRules: Dataset[MappedRules], filter: Column): Dataset[Entity] = {
    appliedRules.filter(filter).select(ENTITY_URI_FIELD).as[Entity]
  }

  /**
   * Build a filter to apply against a Dataset[MappedRules] where all the rules matching the outcome provided are chained
   * together with an or.
   * The rules are identified using the ruleName string format.
   */
  private def matchedRulesFilter(subgraph: SubgraphDefinition, colSupplier: String => Column, outcome: SubgraphRule.Outcome): Option[Column] = {
    rules filter {
      case (name, _, r) => subgraph.getName.equals(name) && r.getOutcome == outcome
    } map {
      case (_, idx, _) => array_contains(colSupplier(MATCHED_RULLES_FIELD), lit(ruleName(subgraph.getName, idx)))
    } reduceOption {
      _ or _
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
    filterFromValue(colSupplier("context"), colSupplier("subject"), triplePattern.getSubject)
      .and(filterFromValue(colSupplier("context"), colSupplier("predicate"), triplePattern.getPredicate))
      .and(filterFromValue(colSupplier("context"), colSupplier("object"), triplePattern.getObject))
  }

  /**
   * Filter a rdf resource or literal based on the splitting rules convention.
   * - BNode("?entity"): matches the current entity
   * - BNone("wildcard"): matches any literal or resource
   * - anything else: matches the N3 representation using statementEncoder
   */
  private def filterFromValue(entityColumn: Column, col: Column, value: Value): Column = {
    value match {
      case bnode: BNode =>
        if (bnode.getID.equals(SubgraphRule.TriplePattern.ENTITY_BINDING_NAME)) {
          col === entityColumn
        } else if (bnode.getID.equals(TriplePattern.WILDCARD_BNODE_LABEL)) {
          lit(true)
        } else {
          throw new UnsupportedOperationException("Unsupported bnode id: " + bnode.getID)
        }
      case _ =>
        col === lit(statementEncoder.encode(value))
    }
  }
}
