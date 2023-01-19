package org.wikidata.query.rdf.spark.metrics.subgraphs.detailed

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, countDistinct, desc, row_number}
import org.wikidata.query.rdf.spark.metrics.subgraphs.detailed.TriplesPerSubgraphItem.getTriplesPerSubgraphItem
import org.wikidata.query.rdf.spark.utils.SubgraphUtils.sparkDfColumnsToMap

object PerSubgraphMetrics {

  /**
   * Gets metrics per subgraph which includes number of triples, items, predicates etc.
   * Calls getTriplesPerSubgraphItem(...)
   *
   * @param predicatesPerSubgraph   expected columns: subgraph, predicate_code, count
   * @param fromSubgraphTripleCount expected columns: subgraph, subgraph_to_WD_triples
   * @param toSubgraphTripleCount   expected columns: subgraph, WD_to_subgraph_triples
   * @param topSubgraphTriples      expected columns: subgraph, item, subject, predicate, object, predicate_code
   * @param generalSubgraphMetrics  expected columns: total_items, total_triples, percent_subgraph_item, percent_subgraph_triples,
   *                                num_subgraph, num_top_subgraph, subgraph_size_percentiles, subgraph_size_mean
   * @return a spark dataframe with each column as a metric per subgraph, Expected columns: subgraph, item_count,
   *         triple_count, predicate_count, item_percent, triple_percent, density, item_rank, triple_rank,
   *         triples_per_item_percentiles, triples_per_item_mean, num_direct_triples, num_statements,
   *         num_statement_triples, predicate_counts, subgraph_to_WD_triples, WD_to_subgraph_triples
   */
  def getPerSubgraphMetrics(predicatesPerSubgraph: DataFrame,
                            fromSubgraphTripleCount: DataFrame,
                            toSubgraphTripleCount: DataFrame,
                            topSubgraphTriples: DataFrame,
                            generalSubgraphMetrics: DataFrame): DataFrame = {

    val totalItems = generalSubgraphMetrics.select("total_items").first.getLong(0)
    val totalTriples = generalSubgraphMetrics.select("total_triples").first.getLong(0)
    val topSubgraphMetrics = topSubgraphTriples
      .groupBy("subgraph")
      .agg(
        countDistinct("item").as("item_count"),
        countDistinct("subject", "predicate", "object").as("triple_count"),
        countDistinct("predicate").as("predicate_count")
      )
      .withColumn("item_percent", col("item_count") * 100.0 / totalItems)
      .withColumn("triple_percent", col("triple_count") * 100.0 / totalTriples)
      .withColumn("density", col("triple_count") / col("item_count"))
      .withColumn("item_rank", row_number().over(Window.orderBy(desc("item_count"))))
      .withColumn("triple_rank", row_number().over(Window.orderBy(desc("triple_count"))))

    val (triplesPerItem, numDirectTriples, numStatements, numFullStatements) = getTriplesPerSubgraphItem(topSubgraphTriples)

    val predicateListPerSubgraph = sparkDfColumnsToMap(
      predicatesPerSubgraph,
      "predicate_code",
      "count",
      "predicate_counts",
      List("subgraph")
    )

    val perSubgraphMetrics = topSubgraphMetrics
      .join(triplesPerItem, Seq("subgraph"), "outer")
      .join(numDirectTriples, Seq("subgraph"), "outer")
      .join(numStatements, Seq("subgraph"), "outer")
      .join(numFullStatements, Seq("subgraph"), "outer")
      .join(predicateListPerSubgraph, Seq("subgraph"), "outer")
      .join(fromSubgraphTripleCount, Seq("subgraph"), "outer")
      .join(toSubgraphTripleCount, Seq("subgraph"), "outer")
      .na.fill(0)

    perSubgraphMetrics
  }
}
