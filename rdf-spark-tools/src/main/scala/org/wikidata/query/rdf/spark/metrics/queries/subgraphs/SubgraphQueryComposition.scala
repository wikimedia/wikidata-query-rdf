package org.wikidata.query.rdf.spark.metrics.queries.subgraphs

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.countDistinct

object SubgraphQueryComposition {

  /** Gets per subgraph query composition, that is, counts of the reasons why queries matched with subgraphs.
   *
   * @param subgraphQueries mapping of query to subgraph. Expected columns: id, subgraph, qid, item, predicate, uri, literal
   * @return spark dataframe with columns: subgraph, qid_count, item_count, pred_count, uri_count, literal_count
   */
  def getSubgraphQueryComposition(subgraphQueries: DataFrame): DataFrame = {
    val perSubgraphQueryByQID = subgraphQueries
      .filter("qid == 1")
      .groupBy("subgraph")
      .agg(countDistinct("id").as("qid_count"))

    val perSubgraphQueryByItem = subgraphQueries
      .filter("item == 1")
      .groupBy("subgraph")
      .agg(countDistinct("id").as("item_count"))

    val perSubgraphQueryByPredicate = subgraphQueries
      .filter("predicate == 1")
      .groupBy("subgraph")
      .agg(countDistinct("id").as("pred_count"))

    val perSubgraphQueryByUri = subgraphQueries
      .filter("uri == 1")
      .groupBy("subgraph")
      .agg(countDistinct("id").as("uri_count"))

    val perSubgraphQueryByLiteral = subgraphQueries
      .filter("literal == 1")
      .groupBy("subgraph")
      .agg(countDistinct("id").as("literal_count"))

    perSubgraphQueryByQID
      .join(perSubgraphQueryByItem, Seq("subgraph"), "outer")
      .join(perSubgraphQueryByPredicate, Seq("subgraph"), "outer")
      .join(perSubgraphQueryByUri, Seq("subgraph"), "outer")
      .join(perSubgraphQueryByLiteral, Seq("subgraph"), "outer")
      .na.fill(0)
  }
}
