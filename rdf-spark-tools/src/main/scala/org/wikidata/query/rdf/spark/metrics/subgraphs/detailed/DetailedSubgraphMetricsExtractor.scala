package org.wikidata.query.rdf.spark.metrics.subgraphs.detailed

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.wikidata.query.rdf.spark.metrics.subgraphs.detailed.PerSubgraphMetrics.getPerSubgraphMetrics
import org.wikidata.query.rdf.spark.metrics.subgraphs.detailed.PredicatesPerSubgraph.getPredicatesPerSubgraph
import org.wikidata.query.rdf.spark.metrics.subgraphs.detailed.SubgraphPairMetrics.getSubgraphPairMetrics
import org.wikidata.query.rdf.spark.metrics.subgraphs.detailed.SubgraphPairTriples.getSubgraphPairTriples
import org.wikidata.query.rdf.spark.utils.SparkUtils

object DetailedSubgraphMetricsExtractor {

  /**
   * Reads input tables, calls getDetailedSubgraphMetrics(...) to extract subgraph metrics, and saves output tables
   */
  def extractAndSaveDetailedSubgraphMetrics(topSubgraphItemsPath: String,
                                            topSubgraphTriplesPath: String,
                                            generalSubgraphMetricsPath: String,
                                            allSubgraphsPath: String,
                                            minItems: Long,
                                            perSubgraphMetricsPath: String,
                                            subgraphPairMetricsPath: String): Unit = {

    implicit val spark: SparkSession = SparkUtils.getSparkSession("DetailedSubgraphMetricsExtractor")

    val (perSubgraphMetrics, subgraphPairMetrics) = getDetailedSubgraphMetrics(
      SparkUtils.readTablePartition(generalSubgraphMetricsPath),
      SparkUtils.readTablePartition(topSubgraphItemsPath),
      SparkUtils.readTablePartition(topSubgraphTriplesPath),
      SparkUtils.readTablePartition(allSubgraphsPath),
      minItems
    )
    SparkUtils.saveTables(List(perSubgraphMetrics, subgraphPairMetrics) zip List(perSubgraphMetricsPath, subgraphPairMetricsPath))
  }

  /**
   * Extracts subgraphs metrics in details: per subgraph metrics and per subgraph-pair metrics.
   * Calls getSubgraphPairTriples, getPredicatesPerSubgraph, getPerSubgraphMetrics, and getSubgraphPairMetrics methods
   */
  def getDetailedSubgraphMetrics(generalSubgraphMetrics: DataFrame,
                                 topSubgraphItems: DataFrame,
                                 topSubgraphTriples: DataFrame,
                                 allSubgraphs: DataFrame,
                                 minItems: Long)(implicit spark: SparkSession): (DataFrame, DataFrame) = {

    val (subgraphPairTripleCount, fromSubgraphTripleCount, toSubgraphTripleCount) = getSubgraphPairTriples(topSubgraphItems, topSubgraphTriples)
    val predicatesPerSubgraph = getPredicatesPerSubgraph(topSubgraphTriples)

    val perSubgraphMetrics = getPerSubgraphMetrics(
      predicatesPerSubgraph,
      fromSubgraphTripleCount,
      toSubgraphTripleCount,
      topSubgraphTriples,
      generalSubgraphMetrics)

    val subgraphPairMetrics = getSubgraphPairMetrics(
      allSubgraphs,
      topSubgraphItems,
      subgraphPairTripleCount,
      predicatesPerSubgraph,
      minItems)

    (perSubgraphMetrics, subgraphPairMetrics)
  }
}
