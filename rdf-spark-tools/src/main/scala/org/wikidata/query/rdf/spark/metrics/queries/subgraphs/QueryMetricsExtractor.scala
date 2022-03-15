package org.wikidata.query.rdf.spark.metrics.queries.subgraphs

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.wikidata.query.rdf.spark.metrics.queries.subgraphs.GeneralQueryMetrics.getGeneralQueryMetrics
import org.wikidata.query.rdf.spark.metrics.queries.subgraphs.GeneralSubgraphQueryMetrics.getGeneralSubgraphQueryMetrics
import org.wikidata.query.rdf.spark.metrics.queries.subgraphs.PerSubgraphQueryMetrics.getPerSubgraphQueryMetrics
import org.wikidata.query.rdf.spark.utils.SparkUtils


object QueryMetricsExtractor {

  /**
   * Reads input tables, calls getQueryMetrics(...) to extract subgraph query metrics, and saves output tables
   */
  // entry point method takes in all parameters for subsequent functions
  // scalastyle:off parameter.number
  def extractAndSaveQueryMetrics(topN: Long,
                                 eventQueriesPath: String,
                                 processedQueriesPath: String,
                                 subgraphQueriesPath: String,
                                 subgraphQItemsMatchInQueryPath: String,
                                 subgraphPredicatesMatchInQueryPath: String,
                                 subgraphUrisMatchInQueryPath: String,
                                 generalQueryMetricsPath: String,
                                 generalSubgraphQueryMetricsPath: String,
                                 perSubgraphQueryMetricsPath: String): Unit = {
    // scalastyle:on parameter.number

    implicit val spark: SparkSession = SparkUtils.getSparkSession("QueryMetricsExtractor")

    val (generalQueryMetrics, generalSubgraphQueryMetrics, perSubgraphQueryMetrics) = getQueryMetrics(
      topN,
      SparkUtils.readTablePartition(eventQueriesPath),
      SparkUtils.readTablePartition(processedQueriesPath),
      SparkUtils.readTablePartition(subgraphQueriesPath),
      SparkUtils.readTablePartition(subgraphQItemsMatchInQueryPath),
      SparkUtils.readTablePartition(subgraphPredicatesMatchInQueryPath),
      SparkUtils.readTablePartition(subgraphUrisMatchInQueryPath)
    )
    SparkUtils.saveTables(List(generalQueryMetrics, generalSubgraphQueryMetrics, perSubgraphQueryMetrics)
      zip List(generalQueryMetricsPath, generalSubgraphQueryMetricsPath, perSubgraphQueryMetricsPath))
  }

  /**
   * Extracts general and subgraphs query metrics. Calls getGeneralQueryMetrics, getGeneralSubgraphQueryMetrics
   * and getPerSubgraphQueryMetrics methods
   */
  def getQueryMetrics(topN: Long,
                      eventQueries: DataFrame,
                      processedQueries: DataFrame,
                      subgraphQueries: DataFrame,
                      subgraphQItemsMatchInQuery: DataFrame,
                      subgraphPredicatesMatchInQuery: DataFrame,
                      subgraphUrisMatchInQuery: DataFrame)(implicit spark: SparkSession): (DataFrame, DataFrame, DataFrame) = {

    val (totalTime, processedQueryCount, uaCount, generalQueryMetrics) = getGeneralQueryMetrics(eventQueries, processedQueries)

    val (numOfSubgraphsPerQuery, subgraphQueriesInfo, generalSubgraphQueryMetrics) = getGeneralSubgraphQueryMetrics(processedQueries, subgraphQueries)

    val perSubgraphQueryMetrics = getPerSubgraphQueryMetrics(
      totalTime,
      processedQueryCount,
      uaCount,
      numOfSubgraphsPerQuery,
      subgraphQueriesInfo,
      subgraphQueries,
      subgraphQItemsMatchInQuery,
      subgraphPredicatesMatchInQuery,
      subgraphUrisMatchInQuery,
      topN)

    (generalQueryMetrics, generalSubgraphQueryMetrics, perSubgraphQueryMetrics)
  }
}
