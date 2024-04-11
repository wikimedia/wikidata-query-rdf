package org.wikidata.query.rdf.spark.transform.structureddata.dumps

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit}
import org.wikidata.query.rdf.spark.utils.SparkUtils

object ScholarlyArticleSplitter {
  /** Splits an input partition into two output partitions, keeping shared pieces where needed.
   * Adapted from https://people.wikimedia.org/~andrewtavis-wmde/T342111_spark_sa_subgraph_metrics.html
   * for job wireup and more reliable execution. This additionally pulls values for references (not just entities).
   *
   * @param params input table partition spec and output table partition parent spec
   */
  def splitIntoPartitions(params: ScholarlyArticleSplitParams)(implicit spark: SparkSession): Unit = {
    val outPart = params.outputPartitionParent
    val baseTable = readBaseTable(params.inputPartition)
    // The following are re-used throughout this routine.
    // Spark operations run more reliably this way.
    val ontologyContextRefTriples = ontologyContextReferenceTriples(baseTable).cache()
    val ontologyContextValTriples = ontologyContextValueTriples(baseTable).cache()

    val allEntities = applyRule(baseTable, lit(true))
    val instanceOfScholarlyEntities = applyRule(baseTable, col("predicate") === lit(P31) && col("object") === lit(Q13442814))

    val scholEntities = instanceOfScholarlyEntities
    val mainEntities = allEntities
      .join(instanceOfScholarlyEntities, allEntities("entity_uri") === instanceOfScholarlyEntities("entity_uri"), "left_anti")

    SparkUtils.insertIntoTablePartition(s"$outPart/scope=scholarly_articles",
      allEntityTriples(scholEntities, baseTable, ontologyContextRefTriples, ontologyContextValTriples))
    SparkUtils.insertIntoTablePartition(s"$outPart/scope=wikidata_main",
      allEntityTriples(mainEntities, baseTable, ontologyContextRefTriples, ontologyContextValTriples))
  }

  private def applyRule(from: DataFrame, subgraphRules: Column): DataFrame = {
    from
      .filter(col("context").startsWith(ENTITY_NS))
      .filter(subgraphRules)
      .select(col("context").as("entity_uri"))
      .distinct()
  }

  private def allEntityTriples(entities: DataFrame, allTriples: DataFrame, allReferenceTriples: DataFrame, allValueTriples: DataFrame): DataFrame = {
    joinReferenceAndValues(localEntityTriples(entities, allTriples), allReferenceTriples, allValueTriples)
  }

  private def localEntityTriples(entities: DataFrame, allTriples: DataFrame): DataFrame = {
    entities.join(
        allTriples,
        allTriples("context") === entities("entity_uri"))
      .select(QUAD_COL_NAMES: _*)
  }

  private def joinReferenceAndValues(entitiesTriple: DataFrame, allReferenceTriples: DataFrame, allValueTriples: DataFrame): DataFrame = {
    val entityDirectReferenceUris = distinctObjects(entitiesTriple, PREFIX_REF, "reference_uri")
    val entityDirectValueUris = distinctObjects(entitiesTriple, PREFIX_VAL, "value_uri")

    val entityDirectRefTriples = allReferenceTriples
      .join(entityDirectReferenceUris, allReferenceTriples("subject") === entityDirectReferenceUris("reference_uri"))
      .select(QUAD_COL_NAMES: _*)

    val entityIndirectValueUris = distinctObjects(entityDirectRefTriples, PREFIX_VAL, "value_uri")

    val entityValueUris = entityDirectValueUris
      .union(entityIndirectValueUris)
      .distinct()

    val entityValueTriples = allValueTriples
      .join(entityValueUris, allValueTriples("subject") === entityValueUris("value_uri"))
      .select(QUAD_COL_NAMES: _*)
      .distinct()

    entitiesTriple
      .union(entityDirectRefTriples)
      .union(entityValueTriples)
  }

  private def readBaseTable(fromPartition: String)(implicit spark: SparkSession) = {
    SparkUtils.readTablePartition(fromPartition)
      .select(QUAD_COL_NAMES: _*)
  }

  private def ontologyContextValueTriples(baseTable: DataFrame) = {
    baseTable
      .filter(baseTable("context") === lit("<http://wikiba.se/ontology#Value>"))
  }

  private def ontologyContextReferenceTriples(baseTable: DataFrame) = {
    baseTable
      .filter(baseTable("context") === lit("<http://wikiba.se/ontology#Reference>"))
  }

  private def distinctObjects(from: DataFrame, startingWith: String, aliasOfColumn: String): DataFrame = {
    val columnObject = "object"
    from
      .filter(from(columnObject) startsWith startingWith)
      .select(from(columnObject).alias(aliasOfColumn))
      .distinct()
  }

  private val QUAD_COL_NAMES = List("subject", "predicate", "object", "context").map(col)
  private val P31 = "<http://www.wikidata.org/prop/direct/P31>"
  private val Q13442814 = "<http://www.wikidata.org/entity/Q13442814>"
  private val PREFIX_REF = "<http://www.wikidata.org/reference/"
  private val PREFIX_VAL = "<http://www.wikidata.org/value/"
  private val ENTITY_NS = "<http://www.wikidata.org/entity/"
}
