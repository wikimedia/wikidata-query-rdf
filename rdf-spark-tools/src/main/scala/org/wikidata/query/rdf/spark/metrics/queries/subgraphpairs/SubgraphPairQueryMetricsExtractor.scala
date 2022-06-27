package org.wikidata.query.rdf.spark.metrics.queries.subgraphpairs

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.wikidata.query.rdf.spark.utils.SparkUtils

object SubgraphPairQueryMetricsExtractor {

  /**
   * Reads input table, calls getSubgraphPairQueryMetrics(...) to extract subgraph pair query metrics, and saves output table
   */
  def extractAndSaveSubgraphPairQueryMetrics(subgraphQueriesPath: String,
                                             subgraphPairQueryMetricsPath: String): Unit = {

    implicit val spark: SparkSession = SparkUtils.getSparkSession("SubgraphPairQueryMetricsExtractor")

    val queriesPerSubgraphPair = getSubgraphPairQueryMetrics(SparkUtils.readTablePartition(subgraphQueriesPath))
    SparkUtils.saveTables(List(queriesPerSubgraphPair.coalesce(1)) zip List(subgraphPairQueryMetricsPath)) // file size ~8Mb
  }

  /**
   * Extracts subgraph pair query count.
   *
   * @param subgraphQueries mapping of query to subgraph. Expected columns: id, subgraph, qid, item, predicate, uri, literal
   * @return spark dataframe with columns: subgraph1, subgraph2, query_count
   */
  def getSubgraphPairQueryMetrics(subgraphQueries: DataFrame)(implicit spark: SparkSession): DataFrame = {

    // This will not contain queries that use only 1 subgraph
    // columns: subgraph1, subgraph2, query_count
    val queriesPerSubgraphPair = subgraphQueries
      .selectExpr("subgraph as subgraph1", "id")
      .join(
        subgraphQueries
          .selectExpr("subgraph as subgraph2", "id"),
        Seq("id"),
        "inner"
      )
      .filter("subgraph1 > subgraph2") // this makes sure we have unique pairs of subgraphs
      .groupBy("subgraph1", "subgraph2")
      .count()
      .withColumnRenamed("count", "query_count")

    queriesPerSubgraphPair
  }
}
