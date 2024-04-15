package org.wikidata.query.rdf.spark.transform.structureddata.dumps

import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.wikidata.query.rdf.common.uri.{Ontology, UrisSchemeFactory}
import org.wikidata.query.rdf.spark.utils.SparkUtils
import org.wikidata.query.rdf.tool.subgraph.{SubgraphDefinitions, SubgraphDefinitionsParser}

import scala.collection.JavaConverters._

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
    val dumpMetadata = baseTable.filter(baseTable("context") === lit("<" + Ontology.DUMP + ">")).dropDuplicates().cache()
    val subgraphDefinitions = loadSubGraphDefinitions()
    val subgraphRuleMapper = new SubgraphRuleMapper(UrisSchemeFactory.WIKIDATA, subgraphDefinitions, List("wikidata_main", "scholarly_articles"))
    val mappedSubgraphs = subgraphRuleMapper.mapSubgraphs(baseTable)

    mappedSubgraphs foreach { case (definition, dataset) =>
      SparkUtils.insertIntoTablePartition(s"$outPart/scope=${definition.getName}",
        allEntityTriples(dataset, baseTable, ontologyContextRefTriples, ontologyContextValTriples, dumpMetadata))
    }
  }

  private def loadSubGraphDefinitions(): SubgraphDefinitions = {
    SubgraphDefinitionsParser.yamlParser(PREFIXES.asJava).parse(this.getClass.getResourceAsStream("/scholarly_subgraph_v1.yaml"))
  }

  private def allEntityTriples(entities: Dataset[Entity],
                               allTriples: DataFrame,
                               allReferenceTriples: DataFrame,
                               allValueTriples: DataFrame,
                               dumpMetadata: DataFrame): DataFrame = {
    joinReferenceAndValues(localEntityTriples(entities, allTriples), allReferenceTriples, allValueTriples)
      .union(dumpMetadata)
  }

  private def localEntityTriples(entities: Dataset[Entity], allTriples: DataFrame): DataFrame = {
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
      .filter(baseTable("context") === lit("<" + Ontology.VALUE + ">"))
  }

  private def ontologyContextReferenceTriples(baseTable: DataFrame) = {
    baseTable
      .filter(baseTable("context") === lit("<" + Ontology.REFERENCE + ">"))
  }

  private def distinctObjects(from: DataFrame, startingWith: String, aliasOfColumn: String): DataFrame = {
    val columnObject = "object"
    from
      .filter(from(columnObject) startsWith startingWith)
      .select(from(columnObject).alias(aliasOfColumn))
      .distinct()
  }

  /**
   * @todo load prefixes from some config or add prefixes to the subgraph definition
   */
  private val PREFIXES: Map[String, String] = Map(
    "wd" -> "http://www.wikidata.org/entity/",
    "wdt" -> "http://www.wikidata.org/prop/direct/"
  )
  private val QUAD_COL_NAMES = List("subject", "predicate", "object", "context").map(col)
  private val PREFIX_REF = "<http://www.wikidata.org/reference/"
  private val PREFIX_VAL = "<http://www.wikidata.org/value/"
}
