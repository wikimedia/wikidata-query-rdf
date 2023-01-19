package org.wikidata.query.rdf.spark.metrics.subgraphs.detailed

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

object SubgraphPairTriples {

  /**
   * Gets the connection between subgraph pairs through triples, and the number of triples to and from each subgraph.
   *
   * @param topSubgraphItems   items of the top subgraphs. Expected columns: subgraph, item
   * @param topSubgraphTriples triples of the top subgraphs. Expected columns: subgraph, item, subject, predicate, object, predicate_code
   * @return tuple of 3 spark dataframes:
   *         - subgraphPairTripleCount: number of triples per subgraph pair. Expected columns: subgraph1, subgraph2, triples_from_1_to_2, triples_from_2_to_1
   *         - fromSubgraphTripleCount: number of triples from each subgraph. Expected columns: subgraph, subgraph_to_WD_triples
   *         - toSubgraphTripleCount: number of triples to each subgraph. Expected columns: subgraph, WD_to_subgraph_triples
   */
  def getSubgraphPairTriples(topSubgraphItems: DataFrame,
                             topSubgraphTriples: DataFrame): (DataFrame, DataFrame, DataFrame) = {

    // The same items when used as object can link to multiple subgraphs
    // e.g Sub Pred Obj, Obj P31 A, Obj P31 B: then there will be connection to both A and B subgraph
    // from "Sub"s subgraph, and both will counted, although it is the same triple/item used
    val subgraphPairTripleConnection = topSubgraphTriples
      .selectExpr("subgraph as from_subgraph", "object")
      .distinct()
      .join(
        topSubgraphItems.selectExpr("subgraph as to_subgraph", "item"),
        col("object") === col("item"),
        "inner"
      )
      .groupBy("from_subgraph", "to_subgraph")
      .count()

    // If the same triple is connected to multiple subgraphs, it is counted multiple times
    val fromSubgraphTripleCount = subgraphPairTripleConnection
      .filter("from_subgraph <> to_subgraph")
      .groupBy("from_subgraph")
      .sum()
      .withColumnRenamed("sum(count)", "subgraph_to_WD_triples")
      .withColumnRenamed("from_subgraph", "subgraph")

    val toSubgraphTripleCount = subgraphPairTripleConnection
      .filter("from_subgraph <> to_subgraph")
      .groupBy("to_subgraph")
      .sum()
      .withColumnRenamed("sum(count)", "WD_to_subgraph_triples")
      .withColumnRenamed("to_subgraph", "subgraph")

    // Triples connection between subgraph pairs
    // Turn directed pairs (A->B != B->A) into pairs without direction (A-B or B-A)
    val set1 = subgraphPairTripleConnection
      .filter("from_subgraph >= to_subgraph") // this makes sure we have unique pairs of subgraphs
      .withColumnRenamed("from_subgraph", "subgraph1")
      .withColumnRenamed("to_subgraph", "subgraph2")
      .withColumnRenamed("count", "triples_from_1_to_2")
    val set2 = subgraphPairTripleConnection
      .filter("from_subgraph < to_subgraph") // this makes sure we have unique pairs of subgraphs
      .withColumnRenamed("from_subgraph", "subgraph2")
      .withColumnRenamed("to_subgraph", "subgraph1")
      .withColumnRenamed("count", "triples_from_2_to_1")

    val subgraphPairTripleCount = set1.join(set2, Seq("subgraph1", "subgraph2"), "outer")

    (subgraphPairTripleCount, fromSubgraphTripleCount, toSubgraphTripleCount)
  }
}
