package org.wikidata.query.rdf.spark.metrics.subgraphs.detailed

import org.apache.spark.sql.DataFrame

object PredicatesPerSubgraph {

  /**
   * Gets the connection between subgraph pairs through triples, and the number of triples to and from each subgraph.
   *
   * @param topSubgraphTriples triples of the top subgraphs.
   *                           Expected columns: subgraph, item, subject, predicate, object, predicate_code
   * @return spark dataframe with number of predicates per subgraph. Expected columns: subgraph, predicate_code, count
   */
  def getPredicatesPerSubgraph(topSubgraphTriples: DataFrame): DataFrame = {

    // All predicates per subgraph = ~40k rows total
    val predicatesPerSubgraph = topSubgraphTriples
      .groupBy("subgraph", "predicate_code")
      .count()

    predicatesPerSubgraph
  }
}
