package org.wikidata.query.rdf.spark.transform.queries.subgraphsqueries

import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.wikidata.query.rdf.spark.utils.SparkUtils
import org.wikidata.query.rdf.spark.utils.SubgraphUtils.extractItem

class SubgraphQueryMapper() {

  /**
   * Gets the query mapping given the necessary intermediate calculated dataframes.
   *
   * @param queryWDnames       expected dataframe columns: id, item
   * @param queryUri           expected dataframe columns: id, item, uri
   * @param queryLiterals      expected dataframe columns: id, item, literal
   * @param selectedPredicates expected dataframe columns: subgraph, predicate_code
   * @param selectedUris       expected dataframe columns: subgraph, uri
   * @param selectedLiterals   expected dataframe columns: subgraph, literal
   * @param topSubgraphItems   expected dataframe columns: subgraph, item
   * @return four spark DataFrame containing the query-mapping and some intermediate results
   *         - subgraphQItemsMatchInQuery: Expected columns: id, subgraph, item
   *         - subgraphPredicatesMatchInQuery: Expected columns: id, subgraph, predicate_code
   *         - subgraphUrisMatchInQuery: Expected columns: id, subgraph, uri
   *         - queryMapping: mapping of query to subgraph. Expected columns: id, subgraph, qid, item, predicate, uri, literal
   */
  // method works on multiple inputs, hence the length
  // scalastyle:off method.length
  def getQueryMapping(queryWDnames: DataFrame,
                      queryUri: DataFrame,
                      queryLiterals: DataFrame,
                      selectedPredicates: DataFrame,
                      selectedUris: DataFrame,
                      selectedLiterals: DataFrame,
                      topSubgraphItems: DataFrame): (DataFrame, DataFrame, DataFrame, DataFrame) = {

    // Q-ID (cols: id, subgraph, item)
    val subgraphNames = topSubgraphItems
      .withColumn("item", extractItem(col("subgraph"), lit("/")))
      .distinct()
    val subgraphQidMatchInQuery = queryWDnames
      .join(subgraphNames, Seq("item"), "inner")
      .drop("item")
      .distinct()

    // Instance ITEMS (cols: id, subgraph, item)
    val subgraphQItemsMatchInQuery = topSubgraphItems
      .withColumn("item", extractItem(col("item"), lit("/")))
      .join(queryWDnames, Seq("item"), "inner")
      .distinct()

    // PREDICATES (cols: id, subgraph, predicate_code)
    val subgraphPredicatesMatchInQuery = queryWDnames
      .join(selectedPredicates, queryWDnames("item") === selectedPredicates("predicate_code"), "inner")
      .drop("item")
      .distinct()

    // SUB/OBJ (cols: id, subgraph, uri)
    val subgraphUrisMatchInQuery = queryUri
      .join(selectedUris, Seq("uri"), "inner")
      .distinct()

    // LITERALS (cols: id, subgraph, literal)
    val subgraphLiteralsMatchInQuery = queryLiterals
      .join(selectedLiterals, Seq("literal"), "inner")
      .distinct()

    // TOTAL QUERIES
    val queryMapping = subgraphQItemsMatchInQuery
      .select(col("id"), col("subgraph"),
        lit(true).alias("item"))
      .join(subgraphPredicatesMatchInQuery.select(col("id"), col("subgraph"),
        lit(true).alias("predicate")).distinct(), Seq("id", "subgraph"), "outer")
      .join(subgraphUrisMatchInQuery.select(col("id"), col("subgraph"),
        lit(true).alias("uri")).distinct(), Seq("id", "subgraph"), "outer")
      .join(subgraphQidMatchInQuery.select(col("id"), col("subgraph"),
        lit(true).alias("qid")).distinct(), Seq("id", "subgraph"), "outer")
      .join(subgraphLiteralsMatchInQuery.select(col("id"), col("subgraph"),
        lit(true).alias("literal")).distinct(), Seq("id", "subgraph"), "outer")
      .na.fill(false)
      .distinct()

    (subgraphQItemsMatchInQuery, subgraphPredicatesMatchInQuery, subgraphUrisMatchInQuery, queryMapping)
  }
  // scalastyle:on method.length
}

object SubgraphQueryMapper {

  /**
   * Reads input tables with spark, gets subgraph-query-mapping, and saves the output tables.
   * Calls getSubgraphQueryMapping(...)
   */
  // entry point method takes in all parameters for subsequent functions
  // scalastyle:off parameter.number
  def extractAndSaveSubgraphQueryMapping(wikidataTriplesPath: String,
                                         processedQueryPath: String,
                                         topSubgraphTriplesPath: String,
                                         topSubgraphItemsPath: String,
                                         filteringLimit: Double,
                                         subgraphQItemsMatchInQueryPath: String,
                                         subgraphPredicatesMatchInQueryPath: String,
                                         subgraphUrisMatchInQueryPath: String,
                                         queryMappingPath: String): Unit = {

    implicit val spark: SparkSession = SparkUtils.getSparkSession("SubgraphQueryMapper")

    val (
      subgraphQItemsMatchInQuery,
      subgraphPredicatesMatchInQuery,
      subgraphUrisMatchInQuery,
      queryMapping
      ) = getSubgraphQueryMapping(
      SparkUtils.readTablePartition(wikidataTriplesPath),
      SparkUtils.readTablePartition(processedQueryPath),
      SparkUtils.readTablePartition(topSubgraphTriplesPath),
      SparkUtils.readTablePartition(topSubgraphItemsPath),
      filteringLimit
    )

    SparkUtils.saveTables(
      List(
        subgraphQItemsMatchInQuery,
        subgraphPredicatesMatchInQuery,
        subgraphUrisMatchInQuery,
        queryMapping
      ) zip List(
        subgraphQItemsMatchInQueryPath,
        subgraphPredicatesMatchInQueryPath,
        subgraphUrisMatchInQueryPath,
        queryMappingPath
      )
    )
  }
  // scalastyle:on parameter.number

  /**
   * Get subgraph query mapping using wikidata triples and the parsed queries.
   * Calls the util functions in SubgraphQueryMapperUtils and finally gets the query mapping
   * using subgraphQueryMapper.getQueryMapping(...)
   *
   * @return spark DataFrame containing the query-mapping and three intermediate results table.
   */
  def getSubgraphQueryMapping(wikidataTriples: DataFrame,
                              processedQueries: DataFrame,
                              topSubgraphTriples: DataFrame,
                              topSubgraphItems: DataFrame,
                              filteringLimit: Double): (DataFrame, DataFrame, DataFrame, DataFrame) = {

    val subgraphQueryMapper = new SubgraphQueryMapper()
    val subgraphQueryMapperUtils = new SubgraphQueryMapperUtils(wikidataTriples, processedQueries, topSubgraphTriples, filteringLimit)

    val queryWDnames = subgraphQueryMapperUtils.getQueryWDnames()
    val queryLiterals = subgraphQueryMapperUtils.getQueryLiterals()
    val queryUri = subgraphQueryMapperUtils.getQueryURIs()
    val selectedPredicates = subgraphQueryMapperUtils.getSelectedPredicates()
    val wikidataNodeMetrics = subgraphQueryMapperUtils.getWikidataNodeMetrics()
    val subgraphNodeMetrics = subgraphQueryMapperUtils.getSubgraphNodeMetrics()
    val selectedUris = subgraphQueryMapperUtils.getSelectedURIs(wikidataNodeMetrics, subgraphNodeMetrics)
    val selectedLiterals = subgraphQueryMapperUtils.getSelectedLiterals(wikidataNodeMetrics, subgraphNodeMetrics)

    val (
      subgraphQItemsMatchInQuery,
      subgraphPredicatesMatchInQuery,
      subgraphUrisMatchInQuery,
      queryMapping
      ) = subgraphQueryMapper.getQueryMapping(
      queryWDnames,
      queryUri,
      queryLiterals,
      selectedPredicates,
      selectedUris,
      selectedLiterals,
      topSubgraphItems
    )

    (subgraphQItemsMatchInQuery, subgraphPredicatesMatchInQuery, subgraphUrisMatchInQuery, queryMapping)
  }
}
