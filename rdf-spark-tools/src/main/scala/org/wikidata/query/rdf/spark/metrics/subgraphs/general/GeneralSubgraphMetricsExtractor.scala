package org.wikidata.query.rdf.spark.metrics.subgraphs.general

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit}
import org.wikidata.query.rdf.spark.utils.SparkUtils
import org.wikidata.query.rdf.spark.utils.SubgraphUtils.{extractItem, getPercentileExpr}

object GeneralSubgraphMetricsExtractor {

  /**
   * Reads input tables, calls getSubgraphGeneralMetrics(...) to extract subgraph metrics, and saves output tables
   */
  def extractAndSaveGeneralSubgraphMetrics(minItems: Long,
                                           wikidataTriplesPath: String,
                                           allSubgraphsPath: String,
                                           topSubgraphItemsPath: String,
                                           topSubgraphTriplesPath: String,
                                           generalSubgraphMetricsPath: String): Unit = {

    implicit val spark: SparkSession = SparkUtils.getSparkSession("GeneralSubgraphMetricsExtractor")

    val generalSubgraphMetrics = getGeneralSubgraphMetrics(
      minItems,
      SparkUtils.readTablePartition(wikidataTriplesPath),
      SparkUtils.readTablePartition(allSubgraphsPath),
      SparkUtils.readTablePartition(topSubgraphItemsPath),
      SparkUtils.readTablePartition(topSubgraphTriplesPath)
    )
    SparkUtils.saveTables(List(generalSubgraphMetrics.coalesce(1)) zip List(generalSubgraphMetricsPath)) // file size <1Mb
  }

  /**
   * Extracts general subgraph metrics containing aggregate metrics about triples, items, subgraph size
   * distribution, percent items and triples etc.
   *
   * @param minItems           the minimum number of items a subgraph should have to be called a top_subgraph
   * @param wikidataTriples    All wikidata triples. Expected columns: context, subject, predicate, object
   * @param allSubgraphs       list of all subgraphs and item count. Expected columns: subgraph, count
   * @param topSubgraphItems   items of the top subgraphs. Expected columns: subgraph, item
   * @param topSubgraphTriples triples of the top subgraphs. Expected columns: subgraph, item, subject, predicate, object, predicate_code
   * @return spark dataframe with each metric in a column. Expected columns: total_items, total_triples, percent_subgraph_item,
   *         percent_subgraph_triples, num_subgraph, num_top_subgraph, subgraph_size_percentiles, subgraph_size_mean
   */
  def getGeneralSubgraphMetrics(minItems: Long,
                                wikidataTriples: DataFrame,
                                allSubgraphs: DataFrame,
                                topSubgraphItems: DataFrame,
                                topSubgraphTriples: DataFrame)(implicit spark: SparkSession): DataFrame = {

    val topSubgraphs: DataFrame = allSubgraphs.filter(col("count") >= minItems)
    val totalItems: Long = getTotalItems(wikidataTriples)
    val totalTriples: Long = wikidataTriples.count()

    val numSubgraphs: Long = allSubgraphs.count()
    val numTopSubgraphs: Long = topSubgraphs.count()
    val percentItemsCovered: Double = topSubgraphItems
      .select("item")
      .distinct()
      .count() * 100.0 / totalItems
    val percentTriplesCovered: Double = topSubgraphTriples
      .select("subject", "predicate", "object")
      .distinct()
      .count() * 100.0 / totalTriples
    val subgraphSizeDistribution: DataFrame = allSubgraphs
      .selectExpr(getPercentileExpr("count", "subgraph_size_percentiles"),
        "mean(count) as subgraph_size_mean")

    val data = Seq((totalItems, totalTriples, percentItemsCovered,
      percentTriplesCovered, numSubgraphs, numTopSubgraphs))
    val columns = Seq("total_items", "total_triples", "percent_subgraph_item",
      "percent_subgraph_triples", "num_subgraph", "num_top_subgraph")

    spark.createDataFrame(data)
      .toDF(columns: _*)
      .crossJoin(subgraphSizeDistribution)
  }

  def getTotalItems(wikidataTriples: DataFrame): Long = {

    val allContexts = wikidataTriples
      .select(extractItem(col("context"), lit("/")).as("context"))
      .distinct()
    val totalItems = allContexts
      .filter("context like 'Q%'")
      .count()

    totalItems
  }
}
