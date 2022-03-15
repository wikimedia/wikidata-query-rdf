package org.wikidata.query.rdf.spark.metrics.subgraphs.detailed

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

object SubgraphPairMetrics {

  /**
   * Gets metrics per subgraph pair, that is, number of triples, items, predicates etc common in pairs of subgraphs.
   *
   * @param allSubgraphs            expected columns: subgraph, count
   * @param topSubgraphItems        expected columns: subgraph, item
   * @param subgraphPairTripleCount expected columns: subgraph1, subgraph2, triples_from_1_to_2, triples_from_2_to_1
   * @param predicatesPerSubgraph   expected columns: subgraph, predicate_code, count
   * @param minItems                the minimum number of items a subgraph should have to be called a top_subgraph
   * @return a spark dataframe with each column as a metric per subgraph-pair.
   *         Expected columns: subgraph1, subgraph2, triples_from_1_to_2, triples_from_2_to_1, common_predicate_count,
   *         common_item_count, common_item_percent_of_subgraph1_items, common_item_percent_of_subgraph2_items
   */
  def getSubgraphPairMetrics(allSubgraphs: DataFrame,
                             topSubgraphItems: DataFrame,
                             subgraphPairTripleCount: DataFrame,
                             predicatesPerSubgraph: DataFrame,
                             minItems: Long): DataFrame = {

    val topSubgraphs: DataFrame = allSubgraphs.filter(col("count") >= minItems)

    // (Q123,Q456) and (Q456,Q123) pairs are the same. Latter will remain.
    val commonPredicates = predicatesPerSubgraph
      .selectExpr("subgraph as subgraph1", "predicate_code")
      .join(predicatesPerSubgraph.selectExpr("subgraph as subgraph2", "predicate_code"), Seq("predicate_code"), "inner")
      .filter("subgraph1 > subgraph2") // this makes sure we have unique pairs of subgraphs
      .groupBy("subgraph1", "subgraph2")
      .count()
      .withColumnRenamed("count", "common_predicate_count")

    val commonItems = topSubgraphItems.selectExpr("subgraph as subgraph1", "item")
      .join(topSubgraphItems.selectExpr("subgraph as subgraph2", "item"), Seq("item"), "inner")
      .filter("subgraph1 > subgraph2") // this makes sure we have unique pairs of subgraphs
      .groupBy("subgraph1", "subgraph2")
      .count()
      .join(topSubgraphs.selectExpr("subgraph as subgraph1", "count as subgraph1_item_count"), Seq("subgraph1"), "left")
      .join(topSubgraphs.selectExpr("subgraph as subgraph2", "count as subgraph2_item_count"), Seq("subgraph2"), "left")
      .withColumn("common_item_percent_of_subgraph1_items", col("count") * 100.0 / col("subgraph1_item_count"))
      .withColumn("common_item_percent_of_subgraph2_items", col("count") * 100.0 / col("subgraph2_item_count"))
      .drop("subgraph1_item_count", "subgraph2_item_count")
      .withColumnRenamed("count", "common_item_count")

    subgraphPairTripleCount
      .join(commonPredicates, Seq("subgraph1", "subgraph2"), "outer")
      .join(commonItems, Seq("subgraph1", "subgraph2"), "outer")
      .na.fill(0)
  }
}
